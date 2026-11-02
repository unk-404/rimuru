> 本文保留原始设计细节；如与 [版本和现状](versioning.md) 冲突，以后者和当前源码为准。历史测量归档保存在本机忽略目录，不构成当前验收。

## 11. 性能目标、物理边界与验证计划

### 11.1 统一计量

`D` 为分段后的 UDP payload 字节数；`T` 为总 RX/处理线程拓扑参数，Dispatcher 入口与 worker 数分别记录；`M` 为帧/消息大小；`P` 为平台与后端/完整性/profile/预算配置。

```text
B_raw(D,T,P) = 同机 loopback、每端一 socket、相同地址族/线程拓扑/
               socket 配置/IO 模式/批处理/能力条件下，raw UDP 接收的应用字节/秒
G_L1         = 完整、有效 Whole 帧的唯一应用字节/秒
G_L2         = 完整可靠消息的唯一接收应用字节/秒
G_L3_ordered = 已按序交付的唯一应用字节/秒
G_L3_ready   = L2 已完成、尚未要求按序交付的唯一应用字节/秒
```

raw 每 datagram 可用应用字节为 D，L1 满段为 D−h；头开销自然进入比例。raw 不做重组/协议解析，但需统计接收与丢弃，不能用发送调用返回字节数冒充收到的字节数。GSO syscall 数与 datagram 数分开记，所有控制、FEC、重传、BEGIN 授权与校验字节纳入总线上账本。

baseline 与协议顺序交替或配对运行，不能一边争同一瓶颈一边称“同一次公平对照”。L1 可用相同 DATA datagram 工作量作额外对照，控制帧不能从运行时间或总字节成本中删掉；L2/L3 用相同应用消息工作量及相同时间窗比较，不强迫重传存在时 datagram 数相同。

### 11.2 保留的无损验收目标

| 指标 | 目标 | 条件与状态 |
|---|---:|---|
| L1 Whole / raw | `G_L1/B_raw ≥0.90` | 无损稳态、同 D/T/P；每个适用验收档位单独测；待证明 |
| L2 / L1 | `G_L2/G_L1 ≥0.99` | 相同消息分布、完整性与数据布局、无损稳态；待证明 |
| L3 ordered / L2 | `G_L3_ordered/G_L2 ≥0.995` | 消息 ≥1 KiB、稳定连续交付；待证明 |
| L3 全链路 / raw | `≥0.885` | 原稿保留的独立门槛；三层比值乘积为 0.886545，不能用四舍五入掩盖实测结果 |
| 普通 DATA 额外开销 | L2/L3 相对 L1 额外 0B/段 | 固定同一身份/profile；多 channel BEGIN 元数据、控制与 FEC 另计 |
| L2 控制开销 | 无损大消息稳态目标 ≤0.5% | 需报总字节分母和消息分布；不适用于所有 0B/小消息/高丢包情况 |
| 热路径分配 | 预热后、声明容量内通用 allocator 调用目标为 0/普通 DATA | context 准入、冷启动、池扩容另计；不能把池申请藏到未统计线程 |
| 资源 | 不越过硬预算 | 同时报告稳态/峰值、RSS/虚拟/页锁定/内核估计，吞吐合格不能覆盖资源失败 |

### 11.3 有损场景的修正

设 C 是已明确计量层次的可用发送速率，p 是等长 datagram 在固定 offered load 下的独立擦除概率，反馈不成为瓶颈，则理想成功字节率参考为 `C(1−p)`。真实网络有头、反馈、突发、拥塞、CPU 与资源成本，不能无条件把该式当所有路径的严格容量公式。

L1 不可靠 Whole 含 n 个必要 DATA 段时，独立丢包下完整概率约 `(1−p)^n`，BEGIN 等控制丢失会进一步降低。例：1 MiB 帧、D=1200、按旧 7B 头算约 879 段，p=1% 时全部 DATA 到齐概率仅约 **0.0146%**。因此“L1 Whole 在同样丢包下始终达到 raw 的 90%”不可满足。新增头使分片更多也不能改变这一结论。

修订后的有损 L1 同时报告：segment 接收效率（同条件 raw 比值目标 ≥0.90）和完整 Whole goodput/完成率/帧大小。后者不强行套 90% 门槛，也不拿 segment 数冒充成功帧。此处是本版对旧稿不可实现指标的冻结修正，不代表已经达标。

可靠层保留以下验收目标：

| 测试档 | L2 目标 | L3 目标 |
|---|---|---|
| 普通有损：`0≤p≤10%` 且 `0≤RTT≤200ms` | `G_L2 ≥0.90×B_raw(p,RTT)` | `G_L3_ready/G_L2 ≥0.995`，`G_L3_ordered/G_L2 ≥0.90` |
| 极端：范围仍限定 `p≤20%`、`RTT≤500ms`，且 `p>10%` 或 `RTT>200ms` | `G_L2 ≥0.85×B_raw(p,RTT)` | 同上，另报 HOL 与峰值重排 |
| 固定 p、同限速与预算，RTT 10→200ms | goodput 下降 ≤10% 的目标 | 报完成/ordered 两条曲线及 HOL |

`B_raw(p,RTT)` 必须在**同限速、同注入点、同 D/T/IO/预算类别**下实测成功接收，不把 p 应用两遍。分别记录正向/反向丢包与延迟、排队/突发模型、实际丢包与 offered rate；不能让 raw 无限抢占而可靠层遵守限速，再称等条件。ACK 方向也要限速/注入，防止假设反馈免费。

各层 goodput 必须说明稳态窗口和从首个提交到最终交付/终止的完整运行窗口；丢弃首个有序缺口、只数中段成功数据不能算 ordered 达标。因内存不足、设备热限制或窗口达不到 BDP 而未达目标时，记为**目标未达/资源受限**并报告上界，不能靠改分母转成通过。不承诺有限预算下任意速率×RTT 都成立。

### 11.4 恢复、FEC 与拥塞策略

随机/拥塞分类依据 queue delay、ECN、速率相关性与样本置信度；无 ECN/时钟偏差/路径变化时不能保证识别，默认保守。受控 loopback 可关闭 cwnd/pacing，但保留预算与突发限额；其他路径有 pacing 与及时拥塞响应。L1 原语不自带完整 CC，但批量公网使用须由调用者或可选路径 governor 提供相应控制 [S12]。

FEC 默认关闭，加入前先建立 ARQ 性能基线。启用后需协商 code/profile，明确 block_id、source/repair symbol_id、有效长度、完成条件；这些额外字段、冗余字节、scratch、CPU、块等待都单列。不能说“只加一个 FEC flag、不需要符号标识”。

若发送 k 个源符号与 r 个修复符号，理想成功块的源字节效率参考 `k/(k+r)`，再用实际成功交付、额外补发与编码成本评估；不得再次随意乘除损失项。交织/块大小受时延和内存预算限制。FEC 失败回到有界 ARQ，恢复段经相同校验/发布/去重入口；任何模式不能 ACK 尚未实际持有的数据。

“一个 RTT 内恢复”“块内零延迟”“FEC 必然提高吞吐”均不是保证。净收益为负或缺算力时关闭 FEC并报告；拥塞时不能靠增加冗余硬顶链路。普通 ECN 与 L4S 的语义不能混用，首版不把 L4S 当默认方案。

### 11.5 验证矩阵与证据

| 维度 | 计划覆盖 |
|---|---|
| D | 1200、4096、16384、65507；不适用档位记录 PMTU/地址族/OS 证据，不标通过 |
| T | 1、2、4、8；记录实际接收入口、owner、Rayon、总线程数与 CPU 预算；设备不适用仍保留未覆盖项 |
| 消息 | 0、边界分片长度、64B、1 KiB、16 KiB、64 KiB、1 MiB、64 MiB；u32 边界用解析/准入测试，禁止为覆盖数字盲目申请 4 GiB |
| 网络 | p=0/0.1%/1%/5%/10%/20%；RTT=本地实测/10/50/200/500ms；突发2–8及10+、乱序、重复、黑洞 |
| 拓扑 | 单/双向、同/跨进程、单/多 peer/channel；每端一个真实 socket |
| 能力 | portable 全优化关；批量/GSO/GRO/ZC/页策略逐项消融；失败/中途失效 |
| 内存与消费 | 正常消费、慢消费者、应用长期持有、预算紧张、grant/元数据不足、空消息压力 |
| 并发与容器 | 串行/Rayon、粒度扫描、稠密/稀疏/递增数字键、非数字键分布、DashMap shard 偏斜、guard 生命周期 |
| Allocator | system/mimalloc/池组合，跨线程释放、长跑碎片与 p99；不预设默认赢家 |
| C ABI | C11/C++ include、链接、配置版本、所有权、并发send/poll/close/clone/release、View存活与错误 |
| FFI async | 空poll/批量收割、0/1/多waiter、publish-arm所有交错、伪/丢唤醒、IOCP/kqueue迟到通知、宿主已有runtime、队列满/关闭/取消/析构 |

正确性先用确定性包级模拟器（固定 seed、虚拟时间、丢特定包类型、乱序/重复/延迟/MTU/有限队列）；真实 UDP 再核查单 socket 生命周期与后端。模拟器不能证明内核/驱动性能。

核心性质：无越界/截断交付；L2 会话内去重/原子性；L3 顺序和终止；授权不重复记账；应用持有不会超发 credit；credit=0 仍可补洞与控制推进；ID/重连旧包隔离；完成/abort/close/回收不竞态；任何优化关闭仍可运行。活性证明必须声明“调度最终执行、应用最终释放、反馈最终能到达”等前提，永久黑洞时要求有界终止，不能承诺必送达。

未来使用 proptest/fuzz 覆盖解析与状态组合，loom 验证窄并发发布/回收协议，Miri 检查可运行子集的 unsafe，C 端单独做 sanitizer/生命周期测试。它们各自证据范围不同，不互相替代。

FFI新增验收目标：相同应用消息、完整性、CQ批大小和资源预算下，C批量路径goodput≥原生Rust公共API的99%；单独报告空poll ns/op、每事件CPU、跨核竞争、signal syscall/批、publication→host dequeue的p50/p99/p99.9。空poll无通用heap申请、无通知syscall；有数据且无armed waiter不发系统signal，同一waiter未重新arm前不重复signal。目标是通知合并，不虚构已有绝对ns成绩。C批量参数处理与token pin要做消融，不能用payload复制差异掩盖FFI成本。

性能先预热，配对至少 5 轮，报告中位数、离散/噪声、持续时间与配置/commit/seed；小收益若在噪声内则不认定改善。CPU 时间、cycles/byte、syscalls、各类线字节、尾时延、分配、内存峰值、热/功耗与所有失败计数必须同行。

性能门禁放稳定专机，普通 CI 做正确性/构建/ABI；回归报警阈值冻结为 max(2%, 基线3倍相对噪声)，先建立噪声基线。开发期短 soak 后再做 24h 长跑，移动端前后台/热限制单列；本轮没有执行这些测试。


## 12. 开工筹备、任务顺序与完成定义

### 12.1 已冻结模块边界

冻结为单crate `rimuru`内部模块起步，ffi为后续普通模块，sim/bench为开发目标；拆分不是当前前置任务。

| 模块 | 职责 | 依赖约束 |
|---|---|---|
| wire | 固定布局、控制报文、控制包、校验与测试向量 | 可用 zerocopy/bitfield；不再声称“无依赖”；不依赖线程/socket |
| core/session | 状态机、统一 ID、授权、预算、错误/终止 | 可在虚拟时钟/模拟 IO 中运行 |
| backend | portable 与按目标启用的 OS 加速 | 一个 datagram IO 抽象；不按 peer/channel 创建 socket |
| memory/collections | 池、分配来源、容器/Hasher 封装 | 所有增长受预算；不反向依赖 L3 |
| compute | Rayon pool/任务准入、校验/FEC CPU 工作 | 与 I/O 分离，统一线程与 scratch 预算 |
| l1/l2/l3 | 各层语义与发送/接收调度 | 单向分层，公共组件不倒置依赖 |
| caps/tuning/metrics | 探测、控制器、冻结与证据 | 观测不能绕过硬上限 |
| ffi | C ABI 和生命周期适配 | 依赖公共 core/API，不泄露 Rust 内部类型 |
| sim/bench | 确定性语义验证、raw/分层基准 | 与生产探测隔离，不把模拟吞吐当真实 UDP |

### 12.2 分阶段任务包（Rust M1/M2/M3首版已实现；性能验收按第16/17/18节，FFI/native仍待实现）

| 阶段 | 前置 | 任务与预期交付 | 退出门禁 |
|---|---|---|---|
| D0 已完成 | 原稿可读 | 单文档整合、纠错、目标与筹备 | 原稿/源码/配置未变 |
| D1 本轮冻结 | 用户已授权设计者直接决定 | wire0.0、ABI1.0、完全线程安全、原生CQ async、Tokio/Rayon、资源与工程基线均在本文件确定 | 无待用户选择的设计阻塞；实现与实测未执行 |
| M0 开工第一包 | 另获开工指令，沿用本版冻结规范 | 最小工程、预算与容器入口、虚拟时钟/模拟器、portable 单 socket 后端、raw UDP 基准；Rayon pool 与串行对照；FFI 最小链接通路 | 可复现 baseline；socket 数断言；受限分配/队列；C header/符号链路真实可用 |
| M1 L1 | M0 | DATA_R/BEGIN、分片、Whole 重组、校验、生命周期、边界注入 | 正确性先过；所有适用 D/T 档位 L1≥0.90 raw，单一大包档通过仅算中间成果 |
| M2 L2 | M1 | grant/credit、SACK、恢复、保留/去重/终止、路径限速；后加 NACK/FEC | 原子/去重/预算/进展性质；无损与有损目标，未达如实记录 |
| M3 L3 | M2 | channel 线性化准入、显式 seq、重排/终止、慢消费背压 | 0B额外 DATA、顺序/隔离、ordered 目标与 HOL 证据 |
| M4 性能与平台深化 | 正确性合同稳定 | Rayon 粒度、Hasher/容器、allocator 消融；OS 优化、自调和能力失效 | 全平台真实记录；优化默认值有收益与资源证据；公开 API 与回退成立 |
| M5 发布准备 | 目标平台与 FFI 达标 | 工具链/MSRV/feature 矩阵、C/C++ 示例/绑定打包、24h soak、使用与限制说明 | 可重建分发物、ABI 检查、资源长跑、无伪造平台通过 |

### 12.3 下一步起手顺序

以下为D1阶段冻结的最初筹备顺序；Rust L1/L2/L3已经按后续授权实现，未完成项依各里程碑继续：

1. 再核对真实工作目录、用户已有变更和仓库指令；建立可回滚基线，不能把现有未跟踪文件当作可覆盖模板。
2. 按本版协议/ABI数值、资源公式、工具链与目标架构建立测试向量/构建配置；盘点真机与专机，缺设备标未验证，不作为设计阻塞。
3. 按第7.3节选型审核现有依赖/features及许可证，按里程碑引入；未实现FEC/native/allocator不预装为手动features。
4. 建可测预算/容器/时钟/IO边界与确定性模拟器；再建立真实单 socket raw UDP 成本基线。
5. 建共享Tokio multi-thread Runtime、有界Rayon池、句柄安全pin、MPMC CQ和Waiter；以丢唤醒/close竞态和CPU预算验证底座，数字Hasher测整表而不只finish。
6. 让 cbindgen→C 编译→库链接→最小生命周期形成闭环；此后扩展 L1，不能末期才发现 FFI 释放模型不成立。

用户已明确授权实现L1、L2、L3及benchmark；当前Rust实现状态以第16/17/18节为准。以上起手顺序保留为最初筹备记录，不表示仍需用户授权或全部退出门禁已经通过。C FFI、FEC及原生加速仍按后续里程碑推进，不把未实现能力写成现状。


## 13. 已冻结决策、资源初值与验证风险

本表关闭上一版全部“待冻结”事项，均由设计者依据用户授权作出决定；后续无需再请求用户选择技术细节。**没有设计阻塞，仍有实现/测试工作。** 实测若证伪目标，记录证据并在既定合同内优化；涉及外部语义/格式变更才升版本。

| ID | 已冻结结论 | 实现验证点 |
|---|---|---|
| W01 | wire 0.0：13B公共头、DATA16/32为15/17B、8B方向身份；type/控制body按第4节 | golden vectors、跨端序解析、非法长度/身份、控制CRC |
| W02 | UDP校验和+BEGIN全帧CRC32C默认；仅COMPLETE确认成功，CRC失败终止 | ACK不提前释放，COMPLETE丢失、确认重试与内核ZC保留 |
| W03 | 整消息幂等grant；CREDIT为单调提示，非授权；并行BEGIN批处理，无无确认通用grant | 预留+完成槽位+句柄额度一起原子准入，credit=0补洞 |
| W04 | frame_id/seq同incarnation不复用；压缩墓碑不按TTL恢复旧ID；空间/墓碑耗尽排空重连 | ID耗尽、区间碎片、旧握手/旧包与同时连接 |
| W05 | 同channel成功准入线性化；前驱失败终止channel；MPMC出队顺序有序，外部处理调度不承诺 | close/cancel/COMPLETE竞争与多consumer唯一交付 |
| RSC01 | 以下默认限额/公式；总账本约束所有层、事件、句柄与线程 | 受限设备、应用持有、额度分配失败不破坏已承诺消息 |
| PERF01 | 无损Whole≥90%raw；有损L1分别测segment与完整帧；可靠层目标保留 | 公平raw对照、全时窗ordered、资源受限不能改分母算通过 |
| COLL01 | 数字DirectHasher，不混合；非数字foldhash fast；句柄与稠密数据直接索引 | 数字分布/shard偏斜；不持guard等待；不靠默认hasher蒙混 |
| CPU01 | 库内隐式共享Tokio multi-thread执行器+有界Rayon池；不向使用者提供MultiRuntime | 总线程、NUMA、任务粒度、Native桥接与无嵌套block_on |
| ABI01 | ABI1.0、u64 generation handle、全对象线程安全、批量CQ+OS Waiter | clone/release/close竞态、MPMC、丢唤醒、C/OS loop真实接入 |
| ENG01 | 单crate rimuru，edition2024，开发/发布工具链Rust1.99.0，声明MSRV1.99.0 | Windows已在1.99.0构建/测试；其余目标CI尚未运行，见第16节 |
| LAB01 | 五平台同目标，以下目标系统/架构固定；设备缺口属于未验证 | 真机、稳定性能专机、热/后台与驱动能力证据 |

### 13.1 资源默认值与自调边界

默认值是版本化策略/安全bootstrap，允许调用者显式降低/提高上限并受Runtime总预算裁决，不是把可调性能参数永远写死。不得通过自调越过配置硬上限。

| 项目 | 冻结默认与边界 |
|---|---|
| Runtime用户态预算 | A取OS可用内存与进程/cgroup可用额度中的较小者，M_rt=min(256MiB,floor(A/16))；无法获取A时64MiB；低于8MiB返回NO_BUFFER而不无脑提高；只惰性分配 |
| Endpoint预算 | min(64MiB,M_rt/2)，最低4MiB；可显式配置；多个Endpoint共享Runtime总账本，局部上限之和可大于总上限，但绝不超承诺实际grant |
| 单消息上限 | min(16MiB,M_endpoint/4)；显式可提高至u32::MAX，但须预留整消息和元数据，分配失败拒绝；0值配置表示按公式推导，空消息len=0另行合法 |
| 内核socket缓冲 | 初始各min(1MiB,M_endpoint/16)，setsockopt后回读；OS扩大因子/实际内核成本另报，不能伪装用户态预算覆盖全部kernel RSS |
| per Endpoint peer/channel/活跃消息 | 默认分别min(1024,M/64KiB)、min(4096,M/16KiB)、min(8192,M/4KiB)，且都受实际内存收费；最小合法1，创建时未为全部预分配 |
| 默认CQ | capacity=min(4096,max(256,floor(M/2048)))，另保留64个控制槽；显式CQ容量1..65536，最多64个CQ/Endpoint，均按容量实际计费 |
| CQ/句柄准入 | 每接受的可靠消息预留一个接收Message token、一个完成记录及必要terminal额度；发送预留ticket和终态记录；poll不能临时申请通用heap才返回事件 |
| Waiter | 每CQ最多64，每Runtime最多1024；按需创建OS对象，poll-only为0个；热路径活跃bitmap为空时无系统通知 |
| FFI句柄 | 每Runtime配额min(262144,M_rt/256)，全进程目录最多4096页×每页1024slot，顶层约32KiB由首次Runtime计费、页按需申请；跨Runtime不重复发放同一有效token；clone也计费；generation回绕退休slot；Runtime/control句柄占全进程唯一的registry，每条记录归属相应Runtime预算 |
| 孤儿缓存 | 仅L1：每peer min(64KiB,M/1024)、全Endpoint min(1MiB,M/32)，并限每peer64段；L2无grant DATA不分配任意大缓存 |
| 空闲池 | 不超过M_endpoint/8；空闲5秒后分批归还，活跃块和已授予剩余预留优先，不能自动扩大到物理内存比例上限 |
| FEC/scratch | Endpoint上限M/8，且每job必须成功借用预算；无额度不启动任务/自动回ARQ；块k≤64/r_max=32按第4节 |
| 墓碑/握手 | 每Session最多4096区间；退役诊断4096条/120秒；未验证peer最多1KiB临时状态、全Endpoint最多min(256KiB,M/64) |

线程预算默认`T=min(available_parallelism+1,17)`且至少2：1个生命周期/退出协调线程、`I=2 if T>=4 else 1`个I/O/任务线程，其余`C=T-1-I`为Rayon线程；C=0时计算串行。用户配置max_total_threads不得低于2，I≥1，`1+I+C≤T`；调优可降低活跃任务，不越过上限。Native有专用驱动时占用I中的1个，其余至少1个Tokio worker；I不足2则Portable。遥测/控制是已有线程上的任务，不再另开固定线程。线程栈虚拟保留与实际提交单列，并在Runtime账本预留启动所需资源，资源不足降低默认T或拒绝，显式配置不静默违背。

当前CRC并行门槛经Windows成本实测调整为8MiB、按1MiB分块；TX布局还要求至少256片，常规1MiB校验留在当前任务。CRC合并缓存4KiB静态算子，避免每块重新生成GF(2)矩阵；实际任务由帧槽位和字节预算限流。FEC块/自调与最多`4*max(C,1)`个外层job的独立governor仍属后续，不在Rayon worker内等待另一个job的准入许可。RX/TX起始批深度32，FFI收割64，自调范围1..256；每次poll最多256条，参数更大返回INVALID_ARGUMENT，防单次垄断。spin默认0，显式允许后受CPU/热预算与驻留控制，不默认忙等。

### 13.2 时间、恢复与拥塞默认

握手总时限10秒，首个RTO500ms，指数退避上限2秒；有样本后采用`max(时钟精度,1ms,srtt+4*rttvar)`并上限60秒。可靠消息总寿命默认120秒、最多32次恢复轮，任一上限到达即明确终止/结果未知；空闲Session无自动失联判定，只有在途/显式keepalive时启用探测。L1未完成帧寿命初始1秒，有样本后`clamp(4*srtt,100ms,5s)`；控制丢包与应用超时仍是不同概念。

NACK延迟初始10ms，之后`clamp(实测乱序p99.9,1ms,min(srtt/4,100ms))`，边界倒置时用较小可行非零时钟tick；同缺口合并并限每frame每RTO一次。延迟ACK初值每32段或1ms先到者，最大间隔min(srtt/4,25ms)，握手阶段/尾部直接反馈；timer精度不足则回退可表示tick并记录。cwnd初始10*D，未知路径采用保守拥塞控制；随机丢包高置信度时可用速率模型保持总发送率，模型按字节计，重传/FEC计入同一速率。pacing关闭仅限受控loopback，否则开启；ECN只使用经过协商验证的普通ECN，不启用L4S。

Endpoint/Runtime close默认优雅排空5秒，之后取消未完成协议任务并关闭/取消内核I/O。关闭超时不能释放仍被内核引用的buffer：保留直到OS完成/取消证据到达，再发布最终CLOSED；不为快速退出制造UAF。所有等待可cancel，实际内核/驱动失效造成无法确认释放时如实报IO并保留安全引用。

### 13.3 工程与平台基线

单crate按wire/backend/core/memory/compute/l1/l2/l3/ffi/sim/bench模块组织，不先建多个workspace crate。Rust1.99.0、edition2024作为首版发布/CI/MSRV基线；本机读取到rustc1.99.0和cargo1.99.0，不代表尚未执行的crate/目标构建已通过。优化实现使用stable接口，实验工具可用独立nightly，不把nightly引入生产要求。

目标系统下限固定：Windows10 2004、Linux5.10、macOS13、Android API26、iOS16。主要triples为x86_64-pc-windows-msvc/aarch64-pc-windows-msvc、x86_64-unknown-linux-gnu/aarch64-unknown-linux-gnu、x86_64-apple-darwin/aarch64-apple-darwin、aarch64-linux-android/x86_64-linux-android、aarch64-apple-ios；aarch64-apple-ios-sim只作模拟辅助。基础功能用公开API，更新OS才有的能力按探针启用；旧于下限不承诺支持。五平台产品目标无优先级，执行先后受设备而不改变验收。

主要实测风险仍是单socket瓶颈、重组复制、数字Hasher分片偏斜、小消息控制、BDP内存、Rayon/Tokio过度订阅、allocator碎片、FEC开销、移动热限制以及FFI通知竞争。它们是必须验证的工程风险，不是要求用户再拍板的冻结项。


## 14. 一手资料与核验范围

本轮查阅以下官方/维护者资料，用于确认接口合同与纠正旧稿。资料支持设计依据，不提供 Rimuru 的吞吐、正确性或平台实测证据。链接可能随版本演进；实现应对锁定版本重新核对。

- **[S1]** [hashbrown 官方文档](https://docs.rs/hashbrown/0.17.1/hashbrown/)：项目指定的散列表组件。
- **[S1a]** [hashbrown v0.17.1 tag 源码](https://github.com/rust-lang/hashbrown/blob/v0.17.1/src/control/tag.rs)：高位 tag 对直接数字 hash 的影响依据。
- **[S2]** [DashMap 官方 API](https://docs.rs/dashmap/6.2.1/dashmap/struct.DashMap.html)：并发表、guard 与方法锁行为。
- **[S2a]** [DashMap 官方源码](https://docs.rs/dashmap/latest/src/dashmap/lib.rs.html)：本轮 6.2.1 的 determine_shard 位选择；升级要重核，不能当稳定公开 ABI。
- **[S3]** [zerocopy 官方文档](https://docs.rs/zerocopy/0.8.63/zerocopy/)：转换 traits、布局/有效值与端序工具。
- **[S4]** [bitfield 官方文档](https://docs.rs/bitfield/0.20.0/bitfield/)；**[S4a]** [bitvec 官方文档](https://docs.rs/bitvec/latest/bitvec/)：固定位域与动态位图的用途边界。
- **[S5]** [Microsoft mimalloc](https://github.com/microsoft/mimalloc)；**[S5a]** [Rust mimalloc 绑定文档](https://docs.rs/mimalloc/latest/mimalloc/)：allocator 候选及 Rust 集成方式，非本项目收益证明。
- **[S6]** [Rayon 官方文档](https://docs.rs/rayon/1.12.0/rayon/)及 [ThreadPoolBuilder](https://docs.rs/rayon/1.12.0/rayon/struct.ThreadPoolBuilder.html)：数据并行、自建/全局线程池的边界。
- **[S7]** [Linux 内核 MSG_ZEROCOPY 文档](https://docs.kernel.org/networking/msg_zerocopy.html)：pin/完成通知、实际复制回退，特别是 loopback 的 deferred copy。
- **[S8]** [Microsoft IPPROTO_UDP socket options](https://learn.microsoft.com/en-us/windows/win32/winsock/ipproto-udp-socket-options)：UDP 校验和、segmentation/coalescing 配置及边界信息。
- **[S9]** [Rust Hasher 合同](https://doc.rust-lang.org/std/hash/trait.Hasher.html)：u64输出、整数写入和字节写入契约。
- **[S10]** [cbindgen 维护者指南](https://github.com/mozilla/cbindgen/blob/main/docs.md)：从 Rust C API 生成头、repr布局与配置。
- **[S11]** [Rustonomicon FFI](https://doc.rust-lang.org/nomicon/ffi.html)：C ABI、所有权与 unwind 边界。
- **[S12]** [RFC 8085 UDP Usage Guidelines](https://www.rfc-editor.org/rfc/rfc8085.html)：UDP 应用的拥塞、反馈和突发控制依据；本项目不以严格 TCP 公平性为 KPI 不免除这些工程问题。
- **[S13]** [foldhash 维护者文档](https://docs.rs/foldhash/latest/foldhash/)；**[S14]** [rapidhash 维护者文档](https://docs.rs/rapidhash/latest/rapidhash/)：默认选foldhash，rapidhash仅作实验对照；默认改变须有本项目测量依据。
- **[S15]** [Rust 2024 Unsafe attributes](https://doc.rust-lang.org/edition-guide/rust-2024/unsafe-attributes.html)：no_mangle 等属性的显式 unsafe 要求。
- **[S16]** [socket2 维护者文档](https://docs.rs/socket2/latest/socket2/)；**[S17]** [loom 维护者文档](https://docs.rs/loom/latest/loom/)：后端socket封装与窄范围并发模型验证依据。

- **[S18]** [Tokio runtime](https://docs.rs/tokio/1.54.1/tokio/runtime/index.html)及[Builder](https://docs.rs/tokio/1.54.1/tokio/runtime/struct.Builder.html)：multi-thread与多个runtime的区别、配置和NUMA边界；本项目线程分工为设计决策。
- **[S19]** [Tokio UdpSocket](https://docs.rs/tokio/1.54.1/tokio/net/struct.UdpSocket.html)及[Notify](https://docs.rs/tokio/1.54.1/tokio/sync/struct.Notify.html)：Arc共享、低层poll/waker与permit语义，不能代替多消费者注册协议。
- **[S20]** [Linux eventfd手册](https://man7.org/linux/man-pages/man2/eventfd.2.html)及[epoll手册](https://man7.org/linux/man-pages/man7/epoll.7.html)：非UDP通知fd和宿主事件循环集成。
- **[S21]** [Microsoft PostQueuedCompletionStatus](https://learn.microsoft.com/en-us/windows/win32/api/ioapiset/nf-ioapiset-postqueuedcompletionstatus)：向宿主IOCP投递合并通知。
- **[S22]** [Microsoft WaitOnAddress](https://learn.microsoft.com/en-us/windows/win32/api/synchapi/nf-synchapi-waitonaddress)：地址等待、唤醒与谓词复核。
- **[S23]** [Apple kqueue/kevent公开手册](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/kevent.2.html)及[Apple XNU公开event.h](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/sys/event.h)：注册机制、EVFILT_USER/NOTE_TRIGGER定义；目标SDK与真机仍须验证。
- **[S24]** [arc-swap维护者文档](https://docs.rs/arc-swap/1.9.3/arc_swap/)：可替换Arc的安全读取与回收基础；generation token逻辑由本项目另行验证。
- **[S25]** [crossbeam ArrayQueue](https://docs.rs/crossbeam-queue/0.3.14/crossbeam_queue/struct.ArrayQueue.html)：有界MPMC，不使用覆盖式force_push。
- **[S26]** [reed-solomon-simd 3.1.0](https://docs.rs/reed-solomon-simd/3.1.0/reed_solomon_simd/)：选定FEC实现、SIMD回退及分片尺寸兼容边界，不引用其基准当本项目数据。
- **[S27]** [crc32c维护者文档](https://docs.rs/crc32c/0.6.8/crc32c/)：CRC32C及分块combine接口。
- **[S28]** [BLAKE3官方Rust实现](https://docs.rs/blake3/1.8.7/blake3/)：握手cookie的keyed hash；不改变无数据加密的范围。
- **[S29]** [slab维护者文档](https://docs.rs/slab/0.4.12/slab/)：本地索引存储，原始索引可能复用，不能代替generation保护。

- **[S30]** [Criterion官方文档](https://docs.rs/criterion/0.8.2/criterion/struct.Criterion.html)：标准warmup、采样、置信区间、吞吐分组、历史baseline比较；L1基准实际使用Criterion 0.8.2。
- **[S31]** [thiserror官方文档](https://docs.rs/thiserror/2.0.21/thiserror/)：结构化错误derive、source与From转换；实际使用thiserror 2.0.21。
- **[S32]** [crc-fast官方文档](https://docs.rs/crc-fast/1.10.0/crc_fast/)及[Digest](https://docs.rs/crc-fast/1.10.0/crc_fast/struct.Digest.html)：CRC32/ISCSI、流式状态与硬件分派；实际收益以本仓库Windows微基准和UDP对照为准。

