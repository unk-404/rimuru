> 本文保留原始设计细节；如与 [版本和现状](versioning.md) 冲突，以后者和当前源码为准。历史测量归档保存在本机忽略目录，不构成当前验收。

## 3. 架构、状态归属与并发

### 3.1 数据路径

```text
应用 Rust API / C FFI
  ├─ L1 Whole 帧
  ├─ L2 可靠无序消息
  └─ L3 有序 channel → L2
                ↓ 共用 L1 编解码与单 socket 调度
Endpoint：一个 UDP socket → 收包池 → 校验/解析 → Session/Frame owner
                ↓
       重组 / ARQ / 完成事件 → L3 重排（若启用）→ 应用
共享：能力图、预算 governor、BufferPool、Rayon CPU pool、时钟、指标
```

| 对象 | 唯一状态归属 | 约束 |
|---|---|---|
| Endpoint | socket、session 注册、总预算、I/O 调度、CPU 池引用 | peer/channel 不是创建线程或 socket 的理由 |
| Session | peer 地址、会话身份、统一帧 ID 分配器、可靠发送与接收账本、CC/credit | 两个方向的命名空间独立；同一方向跨所有层/channel 统一取号 |
| Channel | 固定层语义；L3 才有 seq/expected/reorder；当前等优先级 | 绑定后不自动从 ordered 变成 unordered |
| L1 | 分片/解析、重组引擎、Whole 交付、Raw 内部事件 | Raw 交给 L2 时复用同一重组组件，不再重组一次 |
| L2 | BEGIN 授权、SACK/重传、接收去重、恢复、流控/拥塞 | 不读取 L3 重排表；分片 SACK 不依赖按序发布，L3 的 COMPLETE 等待按序入队 |
| L3 | 每 channel 顺序准入、重排和终止事件 | 不发第二套 DATA/ACK，不自建拥塞窗口 |
| BufferPool | 物理缓冲、分配来源、容量、回收账本 | 数据在层间移交不改变总占用 |
| Governor/Tuner | 总资源仲裁、能力与优化决策 | 指标是观测，不绕过硬上限 |

按 peer 地址定位 Session；首版不透明支持 NAT 地址迁移或多路径。地址变化须重新建立会话。不能把“单端口”推导为真实网络一定不会乱序或路径迁移。

### 3.2 I/O 与 CPU 并发

冻结默认 I/O 模式为 Dispatcher：一个逻辑收包入口分发到有界 owner 队列；Windows 后端可预投递多个 overlapped 接收，但统一归一化完成。Direct 仅作为能力门控的后续可选策略，不同时给同一个 socket 安装多个 reactor。硬件 RSS、内核收包路径及锁行为依平台而异，只承诺一个 socket，不泛称所有平台必定只有一个硬件队列。

尽量由 owner 串行修改一个 Session/Frame 的协议状态，owner 本地散列表用 hashbrown；跨 owner 注册/查找使用 DashMap，短暂持 guard 后取拥有型引用并立即释放。不得持 DashMap guard 跨阻塞、回调、Rayon `install/join`、其他表操作或任务等待。

**Rayon 为 CPU 并行的指定组件**，使用并行迭代、分块任务或 `join/scope`；提供端点组/运行时共享的有界自建 pool，避免库初始化占用宿主进程全局 Rayon pool [S6]。

| 工作 | 并发策略 | 验证条件 |
|---|---|---|
| 大消息 CRC、独立块校验 | 足够大时 Rayon 分块；保持校验算法的正确组合规则 | 顺序/并行结果一致；调度成本可摊销 |
| FEC 编码/解码 | 按独立块或已证明无冲突的符号任务分派 | 输出区间独占、受 scratch 与 CPU 配额限制 |
| 批量解析/准备、较大重组复制批 | 已获授权的互不重叠任务可并行 | 不把每个小 datagram 变成一次任务；并发内存协议成立 |
| 指标归约、离线基准分析 | 分片本地计数后批量归约 | 不在每段上竞争一个全局原子 |
| socket 阻塞收发、ACK 定时器、等待 credit | 专用 I/O/协议调度任务 | 不占满 Rayon worker 等待 I/O 或等待相互依赖任务 |

总线程预算包括 RX、TX、owner、Rayon、定时/控制、遥测线程；不采用“每 peer/channel 一线程”和“每端点各开满核线程池”。Rayon 池的线程数量不假设可原地随意调整；运行时主要调任务准入与分块，确需改 pool 时在安全边界排空后重建或切换。

L3 并发 `send` 的顺序定义为成功准入的线性化顺序，同一临界决策内取得 frame_id 与 channel_seq；并行编码完成顺序不改变该顺序。跨 channel 共享链路容量与内存，提供配额/调度公平；只承诺顺序阻塞隔离，不承诺资源完全互不影响。

### 3.3 隐式执行器与宿主async集成

**Runtime完全属于库内部，不公开Rust Runtime构造器，也不导出RmRuntime句柄/配置/关闭函数。** Endpoint直接bind；首个Endpoint惰性初始化共享Tokio multi-thread执行器、Rayon池和退出协调；后续Endpoint共享，最后拥有引用释放后由库清理。应用不管理多runtime，不需要先初始化或最后shutdown，也不需要宿主使用Tokio。

Tokio负责网络、异步任务和定时器；Rayon负责达到粒度阈值的CRC/FEC CPU任务。线程与内存受内部governor约束；内部可以因NUMA/平台测量采用隔离策略，但不构成外部MultiRuntime API，不把调优责任交给使用者。当前L1/L2/L3使用portable Tokio就绪后排空到WouldBlock，单个逻辑RX dispatcher拥有重组状态；native后端仍是后续任务 [S18][S19]。

未来原生后端仍遵守一个Endpoint一个socket，一个socket只由一个reactor驱动。Windows可采用IOCP，Linux/Android用epoll/批量IO，Apple用公开kqueue；原生驱动到协议owner以有界批量队列合并通知，禁止每个datagram spawn任务。系统能力自动探测，不通过Cargo features切换。

同步方法在外部线程可用；处于async runtime时使用async变体，避免嵌套block_on。应用已有executor可直接await Endpoint异步接口，底层库执行器独立驱动网络。FFI通过第10节原生等待对象接入宿主loop，不导出Rust Future/Tokio Handle/Waker。当前C ABI仅是后续规范，尚无实际导出库。


## 5. 三层交付、恢复与完整性

### 5.1 L1

公共首版提供 Whole：完整接收、长度与校验通过才交付。丢失/过期/资源拒绝时整帧丢弃，可统计或产生失败事件；不承诺可靠送达，也不承诺跨帧去重。内部 Raw 仅作为 L2 接口，不将未验证字节交给普通 Whole 用户。

Scatter/Chunked明确排除在v1公共API之外，后续版本若增加：必须暴露“片段有效”和“整帧最终有效”的区别，定义丢失/校验失败时已消费数据如何处理。最终整帧校验之前已经交出的前缀，不能宣称拥有 Whole 的完整性保证。

### 5.2 L2

冻结状态为：发送 `Queued→AwaitGrant→InFlight→Acked/Aborted/OutcomeUnknown`；接收 `AwaitBegin→Reserved→Receiving→Complete→Delivered→Released`，另有终止分支。可靠性表示：在会话持续可用、预算允许且恢复最终成功时，完整消息在接收方至多交付一次；否则明确失败。会话断开后的持久化“恰好一次”、进程崩溃恢复不在 v1 范围。

接收 ACK 仅确认**校验成功且已经发布/保留的段**；接收方不得 ACK 后静默回收该段还期待发送方继续重传。消息交付后保留可响应重复请求的压缩墓碑。ACK 丢失可能导致同一数据再发送；目标是“不主动调度已知 ACK 的段”，不能保证网络中绝无重复字节。

SACK 按 `(frame_id, frag_idx range)` 报告，批量范围有长度上限；ACK 中未知 frame、越界/重叠范围必须验证。发送时间/顺序属于本地恢复元数据，不靠 frame_id 推断包发送先后。重传复用 ID 时使用 Karn 类规则避免歧义 RTT 样本；无明确测量依据的 ack_delay 不任意从 RTT 扣除。

恢复骨架冻结为 SACK + RACK 类检测 + NACK + 尾部探测 + RTO 兜底，按里程碑实现并保留禁用优化的对照路径。NACK 与 SACK 触发需对同一待恢复段合并；重试有速率、次数/总时长上限，过限终止。等待 API 超时只表示调用等待结束，默认不取消消息。取消可能与交付竞争，返回“已完成/已确认终止/结果未知”，不得把本地取消当作远端未交付的保证。

### 5.3 L3

v1所有L3 channel统一使用BEGIN的显式channel_id/channel_seq，包括单channel；不在运行中切换隐式/显式顺序键。BEGIN采用统一固定布局，零额外成本是相对同布局L2而言，不能把占位字段说成线上不存在；0B额外普通DATA段保持成立。并发发送的序号必须在准入线性化点连续分配，尚未准入的失败不能留下永远没有 BEGIN 的洞。

接收 `seq==expected` 时交付，并连续弹出后继；更大的 seq 在预算内缓冲；旧 seq 根据去重账本忽略。排序仅处理完整校验成功的消息，不进入分片 SACK 判定；L3 COMPLETE 则等待有序发布，避免前驱失败后把未发布后继误报为Delivered。缺失前驱永久失败时，默认终止该有序 channel 并产生错误；不静默跳过，不自动降级无序。若以后需要 skip，须显式定义可靠终止记录与新契约。

后继重排缓存不能挤占前驱已预留的恢复空间；ACK、额度 probe、已准入消息补洞保留独立的控制/执行资源，避免 credit=0 时死锁。同 channel 有序阻塞是必然代价；其他 channel 通过配额和公平调度获得进展。

### 5.4 校验与非加密边界

默认保留 UDP 校验和，并采用**BEGIN 携带4B全帧 CRC32C**的完整性模式，覆盖第4节规定的标准化元数据与payload。可对受信路径显式选择仅 UDP 校验和的性能 profile，但必须单列验收，不能拿该结果代表 CRC profile。

所有校验方案都可能漏检碰撞，不保证对任意损坏“绝对零漏检”，也不防主动伪造。正确性合同是：不交付检测到损坏、缺片、截断或串接错误的消息；给定故障注入集要求坏消息交付数为 0，并记录未检错边界。不以 `integrity=none` 默认绕过目标。

整帧 CRC 最终失败时，不能一面保留“全部片段 ACK 完成”的成功结论，一面又要求发送方凭空找回已释放数据。冻结为发送方保留到 COMPLETE 确认，接收方 CRC 失败以 ABORT(INTEGRITY) 终止；v1 不做自动整帧恢复。Cookie 只用于来源验证/限制放大，随机身份用于错会话隔离；无加密不等于跳过解析边界和内存检查。


## 6. 内存、Allocator 与缓冲所有权

### 6.1 总预算与准入

必须提供 Endpoint/共享运行时总预算，按第13节已冻结的上限及bootstrap公式建立账本，不从报文声明直接生成池大小。允许用更多资源换吞吐，但每次扩大都要通过 governor，并能解释收益。

```text
预算占用 = 活跃物理缓冲 + 已授予但尚未兑现的剩余预留
         + 缓存空闲池块 + 协议元数据/队列/墓碑
         + Rayon/FEC scratch + allocator 余量
预算占用 ≤ M_endpoint
```

预留被兑现为物理缓冲时必须从剩余预留扣除，避免双算；同一物理 buffer 被 L2/L3/应用共享也只记一次。按状态转移追踪：未完成接收→已完成待交付→L3 重排→事件队列→应用持有→回池→归还 OS，不能在 `recv()` 返回或消息从 map 删除时提前释放额度。

内核 socket 缓冲、页锁定、大页、线程栈、虚拟保留与进程 RSS 单列统计。用户态预算不能假装精确控制 OS 的所有内存；内核缓冲设置须回读实际值，超配时降低其他预算。总进程还包含宿主应用；FFI/Rust 用户需能查询 Rimuru 自身与外部开销边界。

至少设置：全局/每 peer 活跃消息数与字节、单消息上限、channel 数、孤儿缓存、TX 保留、重排、应用持有、队列深度、FEC scratch、墓碑与控制包配额。元数据同样收费，禁止“字节数为 0 所以允许无限空消息”。

L2 冻结采用整消息预算预留：发送方提交 BEGIN，接收方在分配前检查 `payload + bitmap + context + integrity/FEC reserve`，成功返回幂等 grant；重发 BEGIN 不重复预留。额度有版本/累计进度，旧 CREDIT 不恢复旧额度。收到 DATA 不得绕过授权新建大消息。grant 撤销必须完成同步终止后生效，不能接收方本地降水位就丢已承诺的数据。

重传不再次消耗消息预留，但仍消耗链路带宽、cwnd、处理时间与固定临时缓冲。ACK、CREDIT probe、终止和前驱补洞拥有有限保留资源；这不是无限制的“豁免”。预算不足返回 `WouldBlock/NoBuffer/MessageTooBig` 或远端拒绝，绝不无界入队。小消息通过有界 BEGIN/BEGIN_ACCEPT 批处理摊销 syscall/通知成本；v1 不采用无逐消息确认的可撤销通用 credit grant。多消息并行请求预留，禁止逐条等待一 RTT 才开始下一条。

### 6.2 BDP 与超大消息

`BDP ≈ 可用发送速率（B/s）×RTT（s）` 是窗口估计，不是强制分配量。以 10 Gbit/s、500ms 为例，单窗口已约 625 MB（约 596 MiB）；重传、整消息预留和有序缓冲还会增加压力。禁止无条件设定 `2×BDP` 或 `BDP+FEC` 并立即 malloc。

整消息预留意味着消息大于可授予预算时拒绝，即使 wire u32 能表达。mmap 的虚拟保留不等于低物理开销，也不能靠超量承诺隐藏资源。首版不承诺在小内存设备上接受 u32 最大消息；如后续增加可重读发送源、分段存储/文件后端，需要独立的生命周期、重读一致性与资源测量。

### 6.3 Allocator 分工

| 区域 | 冻结策略 | 约束与证据 |
|---|---|---|
| 通用分配 | 当前system；mimalloc作为实测候选，不用手动feature | 测并发分配/远端释放、p99、RSS、碎片、二进制体积；不预设跨平台更快 [S5] |
| DATA 缓冲 | 有界大小分级池、owner/NUMA 局部缓存 | 水位限额、惰性增长；稳定容量内热路径复用，空闲批量回收 |
| 帧/消息上下文 | slab与有界arena，热点证据充分时特化 | generation 防 ABA；同时计元数据；不按 u32 最大值预分配索引表 |
| FEC/校验临时区 | 每 worker 或任务借用的 scratch arena | 任务完成后回收；池与 scratch 总和受预算；不为每个符号分配 |
| 超大 buffer | 专用页级分配（能力门控） | 显式记录容量、对齐、来源与释放路径；大页失败回普通页 |

库不能替宿主应用擅自安装全局 allocator；可提供 opt-in 集成或显式内部池。多个 allocator 共存必须使每块内存可追踪到正确释放者，跨线程释放回来源池或有界远端回收队列。C 调用方不能 `free()` Rust/Rimuru 的缓冲；Rust 也不释放未明确转移的 C 内存。

自写 allocator 之前需要三项证据：profile 显示实际瓶颈；成熟 allocator/池无法在约束内解决；微基准和整链路测量都改善且碎片/峰值未越界。允许特化池，不另造通用分配器作为前置工程。

### 6.4 拷贝与发布

TX普通切片入口同步完成内部复制后调用者即可复用原内存；Rust独占layout可直接填写槽位，C TxBuilder通过同步write/writev/seal/submit操作，C借用源按第10节保持只读直至LOCAL_BUFFER_RELEASED。内核 ZC 需等待完成通知，L2 还要等待完成 ACK；二者都满足且引用归零才回收。ACK 不代表对端应用已消费。

RX portable 默认：内核→datagram buffer→检查/解析→一次复制到 contiguous 重组区→完成后借用交付。可选 scatter 交付复用片段，但必须明确 API 不再是连续切片。禁止为了宣称零拷贝隐去 gather 成本。

跨 worker 重组的冻结发布状态为 `Empty→Writing→Ready`：先校验长度并独占段，再写 payload，完成后 Release 发布 Ready；消费者 Acquire 后读取。完成判定必须确认所有需要的 Ready 与最终校验，而不读取 Writing 状态。失败/终止、回收和最后完成线程的竞态由并发模型验证。

普通动态位图优先 bitvec、固定 flags 优先 bitfield；真正跨线程共享的 claim/ready 必须有原子存储与明确的内存序，不能把 bitvec 容器名称当同步证明。可选择 owner 修改位图 + Rayon 独占分块来避免共享位写入；确需原子状态时用窄范围封装配合 bitfield/bitvec 视图，不未经同步地共享 `BitVec` 可变引用。


## 7. 容器、Hasher、结构布局与依赖策略

### 7.1 数字直接 Hasher 的精确边界

项目为已知整数键定义受限的自定义 `BuildHasher/Hasher`：单次整数 `Hash` 写入直接保存位表示，`finish()` 返回该值；不经过 SipHash、Fx、乘法混合或其他通用算法。Rust `Hasher` 的输出为 u64，且 `write_*` 不能假定等价于 `write(bytes)`，因此须按接受的数字类型落实方法契约 [S9]。

| 键类型 | 规则 |
|---|---|
| u8/u16/u32/u64；32/64 位 usize | 按类型无符号值扩展到 u64，直接输出；容器比较仍用完整 Eq |
| 有符号整数 | 保持该类型固定宽度的二进制位表示后无符号扩展，约定一致；同表键类型固定 |
| u128/i128、多个数字组成的复合键 | 不能无损直入 u64；首选按 Session 分层，再按单一 frame_id/frag_idx 建表，必要时高/低 64 位嵌套数字表，均用直接 Hasher；禁止直接截高位或把多次 write 覆盖成最后一项 |
| 浮点数 | 不直接当键；先定义 NaN/±0 与 Eq 语义，转换为明确整数新类型后再评审 |
| SocketAddr、字节串等非数字键 | 使用 `foldhash::fast::RandomState`，种子在运行时创建后保持稳定 |

直接 Hasher 通过内部新类型/容器别名限定调用入口，不把只能接受单个数字的实现暴露为任意 bytes/tuple 的通用 hasher。类型/调用不符合时在内部检查中发现，不能对来自网络的非法包产生 panic。无哈希混合不意味着没有探测、比较、Eq 或冲突处理。

**直接输出数字未必是整表最快方案。** hashbrown 的 SIMD tag 取 hash 的高位；小整数高位重复会降低筛选效果。DashMap 6.2.1 的分片选择也使用较高位；64 位目标上的递增 u32 ID 直接输出很可能集中到单个 shard [S1a][S2a]。这属于源码基础上的推断，仍需本项目基准确认，不是已测结论。

本稿仍遵守用户直接 Hasher 要求，不暗中混合数字：优先将稠密整数索引放入有界 slab/数组/环，将递增 frame_id 状态归 owner 本地 hashbrown，减少数字键共享表。确实需要共享数字表时仍使用 DashMap+直接 Hasher并披露分片退化，不伪称并发已线性扩展。如果该约束导致目标无法达到，提交证据并单独提请修改约束，不能偷偷换哈希函数。

### 7.2 容器与锁规则

项目散列表统一封装类型别名/构造入口，明确数字/非数字 Hasher 与容量。动态增长使用可失败的预留或显式可控分配路径，申请前走预算；对 crate 仍可能隐式分配的操作逐项审计，不承诺所有 OOM 都能转成普通错误。

共享表用 DashMap；owner 独占用 hashbrown；固定稠密 ID 优先 slab/数组；有序重排确需范围操作可用 `BTreeMap`/有序区间结构，不强改 HashMap。DashMap 的 shard 数受 CPU 与内存预算约束，不能每个小表都默认最大分片。遵守官方各方法的锁行为说明，尤其不持 guard 再重入同表 [S2]。

### 7.3 依赖与无功能开关策略

以下为本版选型，不再作为待用户选择的清单。版本是工程基线；开工后在不改变公开合同的前提下由实现者修复构建兼容问题并更新锁文件，记录原因，不把依赖解析失败留给用户决策。本轮不改Cargo文件。

| 组件 | 冻结选型 | 使用边界 |
|---|---|---|
| hashbrown / DashMap | 沿用0.17.1 / 6.2.1 | 本地/共享散列表；数字直接Hasher，非数字foldhash；旧有rayon features保留到消融 |
| bitfield / bitvec | 沿用0.20.0；增加bitvec 1.1.1 | 固定位域/动态位图，不把容器当同步证明 |
| zerocopy | 沿用0.8.63 | wire/POD布局；仅对合法类型derive |
| Rayon | 沿用1.12.0 | Runtime共享的有界CPU池，不占全局默认池 |
| Tokio | 1.54.1，features rt-multi-thread/net/time/sync | 默认共享异步运行时；不使用full或unstable；不把多runtime等同线程安全 [S18] |
| socket2 / mio | socket2 0.6.5；mio由Tokio依赖链使用，不再另起独立默认reactor | 原生能力封装与portable socket；Native驱动显式所有权 |
| arc-swap / crossbeam-queue | 1.9.3 / 0.3.14 | 句柄安全pin/回收；有界ArrayQueue；不使用force_push [S24][S25] |
| slab | 0.4.12 | owner本地稠密状态；FFI句柄另有generation保护，不能直接暴露可复用slab索引 [S29] |
| foldhash | 0.2.0 fast::RandomState | 非数字键默认；rapidhash仅作实验对照，不是第二条未定生产路径 |
| crc32c | 0.6.8 | 标准CRC与分块combine，SIMD能力运行时确认 [S27] |
| crc-fast | 1.10.0，无条件启用std、关闭依赖自身FFI默认导出 | payload的CRC32/ISCSI加速，与原CRC32C逐位兼容；运行时硬件探测与软件回退 [S32] |
| reed-solomon-simd | 3.1.0，后续加入时直接提供 | 首版唯一FEC codec，wire code=1；默认关闭，CPU/内存/收益门控 [S26] |
| blake3 | 1.8.7，标准实现 | 只在握手cookie使用keyed hash，不用于数字Hasher、不引入数据加密 [S28] |
| mimalloc | 0.1.52，allocator候选，当前未引入 | 通用allocator默认system；独立FFI分发/宿主opt-in可启用，经收益验收后改变默认；不擅自替换宿主全局allocator |
| cbindgen | 0.29.4，开发/打包工具 | 显式生成C头，不进入运行时路径 |
| proptest / loom / Miri / cargo-fuzz | 确定采用相应性质、并发模型、unsafe与解析测试 | 工具版本在开工锁文件/CI中记录，不是产品/ABI未定项；Miri用匹配nightly，只跑支持子集 |

当前Windows benchmark的CPU计时使用windows-sys；后续系统绑定使用libc/windows-sys等维护者组件，按target条件编译，不手写整套OS定义。Rimuru不定义Cargo功能features。当前实现无条件提供L1/L2/L3、CRC、池与异步能力；FFI/FEC尚未实现，后续交付即直接可用。原生IO用target cfg及运行时能力探测，allocator选型由实现与实测决定，不要求开发者手动开关。当前Cargo与测试结果见第16/17/18节；未引入的后续组件不是已实现能力。

### 7.4 zerocopy 与布局规则

固定 wire 结构使用明确布局、无隐式未初始化 padding 的字段和端序数字类型，按有效位模式分别选择 `FromBytes`/`TryFromBytes`、`IntoBytes`、`KnownLayout`、`Immutable` 等 derive；并非所有结构都能合法实现全部 traits [S3]。

flags 在 wire 上以原始整数表示，通过 bitfield 访问与校验；不假设宏生成结构可直接 derive 任意 zerocopy trait。长度受限扩展字段仍需有界解析，不用裸 transmute 把不定长字节看成结构体。禁止借 `repr(packed)` 创建未对齐引用；外部传入的 enum/bool 先按原始整数校验。

FFI POD 使用 `repr(C)` 与显式版本/尺寸；wire 小端布局和平台 C ABI 是两份合同，不能把 C 结构体内存直接发送到网络。带 Arc、DashMap、锁、原子、指针或生命周期的运行时结构不做任意字节构造；它们在边界之外保持 Rust 类型安全。

