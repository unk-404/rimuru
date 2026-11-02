> 本文保留原始设计细节；如与 [版本和现状](versioning.md) 冲突，以后者和当前源码为准。历史测量归档保存在本机忽略目录，不构成当前验收。

## 10. 完全线程安全的 Rust/C API 与原生 async（ABI 1.0 冻结）

### 10.1 对象、句柄与并发合同（后续C导出规范）

Runtime隐式，不属于导出对象。普通C使用路径为bind→send/poll→release，peer握手、默认channel、默认CQ/Waiter由Endpoint管理；Session/Channel/独立CQ等只作为高级入口，不是使用L1的前置概念。

**Endpoint、Session、Channel、SendTicket、CompletionQueue、Waiter、TxBuilder、Message 的所有公开操作均支持从任意线程并发调用，不绑定创建线程，不要求调用方给Endpoint加Mutex。** 并发send、批量poll、查询、clone、cancel、close、release均由库处理；原来的“同一channel仅单consumer”限制删除。C输入/输出内存仍必须在调用期间有效，不能让两个线程无同步地写同一调用方数组；这是C内存本身的规则，不是Endpoint线程亲和要求。

Rust共享句柄实现Send+Sync，内部Arc持有对象，短暂同步只保护状态转换；协议owner串行处理其可变状态，跨线程调用以有界命令队列进入。并发send的成功准入线性化点决定同channel顺序，ticket/事件携带对应sequence。并发接收按CQ出队线性化顺序唯一分配消息；多个线程返回/开始业务处理的墙钟顺序可能不同，调用方需要顺序处理时按事件delivery_seq合并，库不承诺任意外部线程调度有序。

C不再导出可释放opaque对象指针，统一使用`uint64_t RmHandle`：高32位generation、低32位slot索引，0非法。全进程唯一的两级稀疏slot目录按需增长（不是每个Runtime复用同一组数字），每slot以`ArcSwapOption<HandleRecord>`发布不可变generation/kind/Arc对象；查找验证kind/generation并pin后才进入操作，批量API一次pin整个批次。句柄表是直接索引slab，不是Mutex<HashMap>，不做数字hash。该实现使用成熟arc-swap保护读/回收，不自写裸指针epoch算法 [S24]。

`rm_handle_clone`创建新的拥有型token引用同一对象；跨线程可共享同一个live token，也可给每个长期持有者clone独立token。`release`原子撤销该token：已pin调用可完成，尚未pin的调用返回INVALID_HANDLE；并发重复release只可能一次成功，其他返回INVALID_HANDLE，不发生UAF。generation将回绕时永久退休slot，不能重新用旧token。close不撤销token，仅改变对象状态；clone/release本身不阻塞网络排空。

最后一个公开Endpoint token释放时触发非阻塞close；公开token计数与内部任务Arc分开，不能因内部引用环永不关闭。Runtime监督任务持有排空引用；任何仍存活的Message/View独立保留池。内部执行器由活跃Endpoint及已接受操作保活，无公开Runtime token；空闲时库自动清理，创建/关闭竞争由库处理。Runtime只有其端点/任务/内核引用退出才销毁，不在Tokio worker上析构并等待自身线程。释放ticket只放弃观察引用，不暗中取消消息。所有对象计入预算，clone耗尽时可失败。

### 10.2 完成队列、提交与所有权

FFI主路径冻结为**批量提交 + MPMC CompletionQueue + 原生Waiter**。默认每Endpoint一个CQ，可显式创建更多CQ；channel创建时固定消息交付CQ，send提交指定ticket完成CQ；一个事件只路由到一个CQ，不广播payload。`rm_endpoint_poll_batch`只是默认CQ的别名，不另建第二份接收队列；不再导出会与CQ抢同一消息的channel_receive。

CQ采用有界`crossbeam_queue::ArrayQueue`承载固定大小完成记录，禁止force_push覆盖。准入时为每个可靠消息的接收完成、发送ticket终态预留事件额度；可靠结果不得因CQ满被覆盖/丢弃。数据事件+终态状态分别保留。close/fatal等结果先持久记录在已收费的对象终态槽/ready bitmap，64个控制ring槽只承载可合并唤醒提示；提示满时poll仍按bitmap唯一合成终态事件，不能丢第65个并发关闭结果。L1额度不足在准入前丢弃并计数。事件被poll取走后归调用方，含Message句柄须release；未取事件随CQ关闭排空并释放，票据终态本身仍可查询 [S25]。

单个send包装批量入口；批量返回accepted_count，只接受前缀，未接受后缀无所有权转移。send_copy在返回前完成复制，成功后调用者原内存可复用。不会为每个datagram创建一个FFI事件，默认仅每消息完成/失败、资源可写、会话变化等用户可操作事件。

TxBuilder提供write/writev/seal/submit：内部写入和seal同步，跨线程重叠write按线性化顺序后写胜出；未写区间有覆盖位图，seal遇缺口返回INVALID_STATE，禁止交出未初始化字节。seal后永久只读，submit至多成功一次。rm_channel_open对同Session/channel_id/层/CQ重复打开返回新token引用同一channel；变更层/CQ拒绝，同incarnation终止的channel_id不复用。C不暴露builder可写裸指针；以此保证所有FFI对象操作均线程安全。Rust独占TrainLayout仍可直接填槽位。C若要求无需复制的已有内存，采用freeze后的`rm_send_borrowed`：调用者在ticket的LOCAL_BUFFER_RELEASED之前保证输入只读且有效，库不释放C内存；这属于明确的外部内存借用合同，普通send_copy不需要它。

Message内容不可变。`rm_message_view`生成独立View句柄并返回只读指针/len；即使其他线程关闭Endpoint、释放原Message token，View仍pin数据直到其token释放。并发释放同一View后继续解引用指针是调用者违反借用合同；其他正常并发API不使View失效。取消不意味着对端未交付；本地调用超时不自动取消。

### 10.3 批量轮询与丢唤醒协议

完成队列拥有`ready_epoch:u64`、pending_count、终态与注册waiter集合；pending_count在发布前预增、出队后递减，包含尚在发布中的槽位，不能采用发布后才加计数而被消费者先减导致下溢。失败发布回滚计数并推进epoch/通知，不能留下假就绪。poll只搬运完成记录，不执行网络收发、不推进协议、不创建future。调用者传cap/stride/结构尺寸与输出数组，一次取尽当前可用事件至cap，返回实际数与observed_epoch；空且未终止返回WOULD_BLOCK。cap=0仅查状态，不消费；timeout=0不做系统等待。

异步使用固定流程：**poll_batch → 空则arm(observed_epoch) → 若READY立即重试，否则等系统通知 → poll_batch**。注册与事件发布的竞态采用以下规范，不能简化为检查空后直接睡眠：

1. 生产者先取得消息预留的槽位/计数，再Release发布事件并递增ready_epoch；随后仅在存在armed waiter时进入通知路径。
2. arm在waiter的短暂arm_gate中清除旧OS信号、置ARMED，再Acquire复核epoch/pending_count/终态。若已变化或非空，则置SIGNALED并返回READY；否则返回ARMED。
3. 生产者在同一arm_gate中将ARMED→SIGNALED并进行一次非阻塞系统signal。清信号与signal按gate排序，禁止出现signal还未写出就被误清后无人再唤醒。
4. 生产者发生在arm之前，由epoch/recheck发现；发生在arm之后，由ARMED通知发现。伪唤醒允许；不能以伪唤醒为由忽略CQ重新检查。
5. 每个waiter只允许一个未消费signal，数据不会一条一次写eventfd/投IOCP。多消费者按就绪批数唤醒有限数量waiter，默认上限min(4,armed_count)，防惊群；close/fatal唤醒全部，零已注册waiter时不做通知syscall。

waiter注册集合按CQ分片，活动bitmap为空时生产者无遍历；batch=64为启动值，可在1..256内根据时延/负载调优。CQ未排空时poll者继续批量收割或触发其他armed waiter，不把剩余数据留在无人唤醒的边沿状态。通知是“可能有进展”的提示，不保证一次signal对应一个完成，也不广播消息所有权。

同一Waiter允许并发arm/wait/cancel：注册状态线性化，重复arm返回当前状态；多个wait线程等同一信号时可发生一个取走数据、其他得到空的正常竞争。每个宿主异步任务长期持有独立Waiter可减少竞争，数量受配额；不为每个in-flight message创建OS等待对象。cancel只取消waiter的等待，不取消消息，唤醒所有当前阻塞wait；重新arm需显式reset，避免自动复活已取消等待。

### 10.4 各系统通知与async桥接

| 系统 | 宿主事件循环主路径 | 阻塞wait与回退 |
|---|---|---|
| Linux/Android | 每个已导出的Waiter一个非阻塞CLOEXEC eventfd，可加入epoll；通知合并，无额外UDP socket | Linux futex类等待+64位谓词复核；不可用时条件变量；eventfd不可用退非阻塞pipe [S20] |
| Windows | 可绑定宿主IOCP，ARMED→SIGNALED时PostQueuedCompletionStatus；通用导出为manual-reset event HANDLE | WaitOnAddress/WakeByAddress与谓词复核；或等待导出event；不得声称event HANDLE天然等同IOCP [S21][S22] |
| macOS/iOS | 显式绑定宿主kqueue的EVFILT_USER/NOTE_TRIGGER，使用唯一ident/token；其他宿主循环导出非阻塞pipe读fd | pthread条件变量；全部使用公开SDK能力，无后台常驻保证 [S23] |
| Rust async | 适配相同epoch/arm协议，每个future独立注册；Tokio/其他executor复用通知对象 | 不把单个AtomicWaker当多消费者注册表，不因最后一个waker覆盖而饿死其他等待者 |

eventfd/pipe/event HANDLE属于本地通知资源，不绑定UDP端口、不增加UDP socket。导出或绑定按需创建，poll-only用法不申请内核wait对象。OS wait source通过独立owner handle保活；宿主先解除其loop注册再release source，不能直接close/read/reset库拥有的fd/HANDLE；清信号由arm处理。宿主IOCP/kqueue绑定使用库持有的重复OS引用，解绑关闭库引用，不关闭宿主原对象。

IOCP包使用调用方保留的completion_key及整数generation cookie，lpOverlapped=NULL；不传可释放Rust地址。kqueue udata同样只放整数token。unbind后内核/宿主队列中已存在的旧通知允许到达，API通过generation判为失效；不能承诺从宿主事件队列撤回已经投递的包。signal失败设置可查询的NOTIFY_FAILED并唤醒库内waiters，后续arm/wait返回IO；宿主仍可手动poll或重建source，不静默丢失完成事件。

FFI不调用任意外部回调，不使用每次完成跨语言闭包唤醒；由宿主在自己的loop收通知再批量取完成。Tokio runtime负责独立驱动协议，不依赖用户poll频率；用户暂不poll只产生有界背压，不冻结ACK/补洞/close。外部宿主无需Tokio，也不能把本库socket再次注册给宿主读包。

### 10.5 ABI与函数清单

ABI major=1、minor=0；Rust固定`extern "C"`、`#[unsafe(no_mangle)]`，仅导出整数、repr(C) POD和调用方缓冲指针。所有配置结构以`abi_version:u32,struct_size:u32`开头；输出结构都显式传size/stride，先检查再写。句柄不是C指针。状态码固定为：0 OK，1 WOULD_BLOCK，2 TIMED_OUT，3 READY，4 ARMED；负值-1 INVALID_ARGUMENT，-2 INVALID_HANDLE，-3 VERSION_MISMATCH，-4 MESSAGE_TOO_BIG，-5 NO_BUFFER，-6 ABORTED，-7 CLOSED，-8 OUTCOME_UNKNOWN，-9 INTERNAL，-10 UNSUPPORTED，-11 INVALID_STATE，-12 WOULD_DEADLOCK，-13 ALREADY_COMPLETED，-14 IO。

SEND_TERMINAL仅在协议终态与LOCAL_BUFFER_RELEASED都满足后入CQ；ticket_query可以更早看到协议终态但flag未置位。借用源调用者可在该终态事件到达后安全复用。事件kind：1 MESSAGE、2 SEND_TERMINAL、3 WRITABLE、4 SESSION_READY、5 SESSION_CLOSED、6 ENDPOINT_CLOSED、7 RESERVED、8 NOTIFY_FAILED、9 CHANNEL_CLOSED。票据state：0 QUEUED、1 AWAIT_GRANT、2 IN_FLIGHT、3 RECEIVED、4 ABORTED、5 OUTCOME_UNKNOWN；LOCAL_BUFFER_RELEASED为独立flag(bit0)，不能因协议终态提前释放内核引用的借用源。WRITABLE和指标通知合并，可靠终态不丢。事件ticket_id/session_id为本地稳定标识而非拥有型handle，不能传给rm_handle_release；delivery_seq为同一接收channel内的u64交付序号，不宣称跨channel全局有序。

下列为**冻结的C声明规范**，仍不是生成完成的实际头文件。POD字段顺序/宽度固定；ABI同一target内保证布局，padding不进入wire，输入reserved必须0。

```c
#include <stdint.h>
#include <stddef.h>
typedef uint64_t RmHandle;
typedef int32_t RmStatus;
typedef struct RmEndpointConfig {
    uint32_t abi_version, struct_size;
    uint64_t memory_budget_bytes;
    uint32_t max_message_bytes, max_peers, max_channels, cq_capacity;
    uint32_t backend, flags;
} RmEndpointConfig;
typedef struct RmAddress {
    uint32_t family; uint16_t port, reserved;
    uint32_t scope_id; uint8_t address[16];
} RmAddress;
typedef struct RmEvent {
    uint32_t kind; RmStatus status;
    uint64_t user_data, ticket_id, session_id, delivery_seq;
    RmHandle message;
    uint32_t channel_seq; uint16_t channel_id; uint8_t layer, flags;
} RmEvent;
typedef struct RmSendItem {
    RmAddress peer;
    RmHandle channel, completion_queue; /* 0 = Endpoint自动默认路径 */
    const uint8_t *data; uint32_t len, flags; uint64_t user_data;
} RmSendItem;
typedef struct RmIoSlice {
    const uint8_t *data; uint32_t len, reserved;
} RmIoSlice;
typedef struct RmWaitSource {
    uint32_t kind, flags; uint64_t native_value; RmHandle owner;
} RmWaitSource;
uint32_t rm_abi_version(void);
RmStatus rm_endpoint_config_init(uint32_t version, size_t size, RmEndpointConfig *out);
RmStatus rm_handle_clone(RmHandle handle, RmHandle *out);
RmStatus rm_handle_release(RmHandle handle);
RmStatus rm_handle_release_batch(const RmHandle *handles, uint32_t count,
                                 RmStatus *item_status);
RmStatus rm_endpoint_bind(const RmEndpointConfig *, const RmAddress *, RmHandle *out);
RmStatus rm_endpoint_local_address(RmHandle endpoint, size_t size, RmAddress *out);
RmStatus rm_endpoint_default_cq(RmHandle endpoint, RmHandle *out);
RmStatus rm_cq_create(RmHandle endpoint, uint32_t capacity, RmHandle *out);
RmStatus rm_session_open(RmHandle endpoint, const RmAddress *, RmHandle *out);
RmStatus rm_session_close(RmHandle session);
RmStatus rm_channel_open(RmHandle session, uint32_t layer, uint16_t id,
                         RmHandle cq, RmHandle *out);
RmStatus rm_channel_close(RmHandle channel);
RmStatus rm_send_batch(RmHandle endpoint, const RmSendItem *items,
                       size_t item_size, uint32_t count,
                       RmHandle *tickets, uint32_t *accepted_count);
RmStatus rm_send_copy(RmHandle endpoint, const RmAddress *peer,
                      const uint8_t *, uint32_t len, uint64_t user_data, RmHandle *ticket);
RmStatus rm_send_borrowed(RmHandle endpoint, const RmAddress *peer,
                          const uint8_t *, uint32_t len, uint64_t user_data, RmHandle *ticket);
RmStatus rm_tx_builder_create(RmHandle channel, uint32_t len, RmHandle *out);
RmStatus rm_tx_builder_write(RmHandle builder, uint32_t offset,
                             const uint8_t *, uint32_t len);
RmStatus rm_tx_builder_writev(RmHandle builder, uint32_t offset,
                              const RmIoSlice *, size_t item_size, uint32_t count);
RmStatus rm_tx_builder_seal(RmHandle builder);
RmStatus rm_tx_builder_submit(RmHandle builder, uint64_t user_data, RmHandle *ticket);
RmStatus rm_ticket_query(RmHandle ticket, uint32_t *state, uint32_t *flags);
RmStatus rm_ticket_cancel(RmHandle ticket);
RmStatus rm_message_view(RmHandle message, const uint8_t **data,
                         uint32_t *len, RmHandle *view_owner);
RmStatus rm_cq_poll_batch(RmHandle cq, RmEvent *out, size_t event_size,
                          uint32_t cap, uint32_t *count, uint64_t *epoch);
RmStatus rm_endpoint_poll_batch(RmHandle endpoint, RmEvent *out, size_t event_size,
                                uint32_t cap, uint32_t *count, uint64_t *epoch);
RmStatus rm_endpoint_wait_source(RmHandle endpoint, size_t size, RmWaitSource *out);
RmStatus rm_endpoint_arm(RmHandle endpoint, uint64_t observed_epoch);
RmStatus rm_waiter_create(RmHandle cq, RmHandle *out);
RmStatus rm_waiter_arm(RmHandle waiter, uint64_t observed_epoch);
RmStatus rm_waiter_wait(RmHandle waiter, uint64_t timeout_ns);
RmStatus rm_waiter_cancel(RmHandle waiter);
RmStatus rm_waiter_reset(RmHandle waiter);
RmStatus rm_waiter_export(RmHandle waiter, size_t size, RmWaitSource *out);
RmStatus rm_waiter_bind_iocp(RmHandle waiter, uintptr_t port, uintptr_t key);
RmStatus rm_waiter_bind_kqueue(RmHandle waiter, int32_t kq, uintptr_t ident);
RmStatus rm_waiter_unbind(RmHandle waiter);
RmStatus rm_endpoint_close(RmHandle endpoint);
RmStatus rm_endpoint_metrics_copy(RmHandle endpoint, uint8_t *dst,
                                  size_t capacity, size_t *required);
```

C没有Runtime配置。rm_endpoint_bind的配置可为NULL表示安全默认；rm_send_copy按peer自动建链并提交到默认L1 channel，默认poll/source/arm不要求创建CQ或Waiter。独立CQ/Waiter保留为高级并发集成入口。backend：0 Auto、1 Portable、2 Native。flags默认0，配置unknown bits拒绝；ABI版本编码`(major<<16)|minor`。地址family=4/6，port/scope_id为host order，address为网络字节序，IPv4使用前4B其余清零。WaitSource kind：1 POSIX_FD、2 WIN_EVENT；IOCP/kqueue为显式bind模式，不能同时bind两个通知目标，重复同参bind幂等，不同目标需先unbind。

数据len=0时允许data=NULL；实现不据空指针构造Rust slice。跨FFI指针必须正确对齐、有效且满足读写长度；同一out缓冲不能被调用者并发乱写。所有batch/writev/release_batch每次最多256项，尺寸乘加checked后才访问。失败的out句柄置0、count置0；批量部分接受以accepted_count为准。release_batch逐项返回状态，某项失效不阻止其他有效token释放，不采用send的前缀接受语义。单消息临时超预算返回NO_BUFFER，可配置单消息上限超出返回MESSAGE_TOO_BIG。等待timeout_ns=0立即检查，UINT64_MAX为无超时；cancel/close仍可终止无限等待。

取消等待不消耗CQ事件；ticket取消的结果只为明确终态或OUTCOME_UNKNOWN。cancel、close幂等；已pin的send若在关闭前成功准入，进入正常完成/终止，未准入返回CLOSED。CLOSED事件在该对象不再产生普通新事件、相关内核I/O已结算后发布，最后引用仍可能由Message/View保留。不要求调用者在close之前停掉其他正常API调用。

### 10.6 cbindgen、运行时退出与验收

固定cbindgen 0.29.4，显式CI/打包生成C头：语言C、guard `RIMURU_H`、include stdint/stddef、`rm_`/`Rm` allowlist。Rust库同时交付rlib、staticlib及目标支持的cdylib；单crate名固定rimuru，交付C FFI时直接提供导出，头文件不作为手写第二事实来源。本轮仅冻结文档，未生成头或链接库。

```text
cbindgen --config cbindgen.toml --crate rimuru --lang c --output include/rimuru.h
```

所有可失败入口阻止panic跨C边界，任务失败转错误/关闭；abort型panic/OOM不能虚称可捕获。Runtime最后引用释放不得同步阻塞当前worker析构自身，由监督线程完成退出；应用只等待对应Endpoint的CLOSED，不暴露内部执行器关闭事件。借用发送源直到LOCAL_BUFFER_RELEASED，关闭/超时也不能提前复用。

ABI门禁包含C11/C++编译链接、尺寸/对齐/offset、跨线程创建使用、并发send/poll/clone/release/close、token generation复用、Message/View跨关闭存活、MPMC出队唯一性、L3出队顺序、多waiter丢唤醒/通知合并、取消与解绑后迟到OS通知、host已有runtime、IOCP/kqueue/eventfd接入、backpressure与退出。单次Cargo test或“Tokio对象Send+Sync”都不能替代这些证明。

