> 本文保留原始设计细节；如与 [版本和现状](versioning.md) 冲突，以后者和当前源码为准。历史测量归档保存在本机忽略目录，不构成当前验收。

## 8. 跨平台后端与能力门控

以下后端和回退已冻结，**尚无一个平台完成本项目验证**。最小回退为公开 UDP API、单 socket、单接收入口、有界缓冲、校验、Whole 与可靠性/排序状态机；性能目标仍需逐平台达标。

| 平台 | 最小路径 | 能力门控加速 | 必须验证的条件 |
|---|---|---|---|
| Windows | Winsock UDP | IOCP/overlapped、WSARecvMsg、USO/coalescing | 目标 OS/SDK/驱动、完成语义与截断、同 socket 并发；不能推定校验和默认关闭 [S8] |
| Linux | BSD socket UDP | epoll、sendmmsg/recvmmsg、GSO/GRO、io_uring、TX ZC | 内核/权限/队列、GRO 控制信息、ZC 实际 copy fallback 与 error queue [S7] |
| macOS | 公开 BSD socket/kqueue | 当前 SDK 公开的批处理/向量 I/O | 真机 MTU、公开 API 状态、实际 batch 与线程收益；不靠私有符号 |
| Android | 公开 NDK socket/事件接口 | 内核允许的批量/GSO/ZC | API level、厂商内核、seccomp/SELinux、热/后台；不写死“所有设备支持/禁止某接口” |
| iOS | 公开 SDK UDP 接口/事件机制 | 官方支持的批处理能力 | SDK 支持、应用沙箱、前后台与热状态；模拟器不能替代真机 |

能力图每项记录 `Supported{evidence}`、`Unsupported{reason}`、`Unknown{probe}`，以及作用域、依赖、探测时间/TTL、失败回退。编译目标支持不等于当前系统支持，setsockopt 成功不等于数据路径已加速。

IRQ/qdisc、系统 MTU、全局调度策略等只读取现状，本库不自动改主机配置。权限相关能力先尝试作用域明确、可回滚的本地探测，失败回退普通页/普通缓冲/阻塞；不把提权当功能前提。单 socket 自身可协商探测，不增设探针 UDP socket。地址族变更不得同时保留第二个数据 socket。

portable 正确性路径必须完整；优化可回退只意味着功能仍可用，不意味着回退后自动满足所有性能 KPI。缺设备/缺权限的结果明确写“未验证/受限”，不标通过。


## 9. 自调参数与可观测性

| 参数类别 | 例子 | 调整权限 |
|---|---|---|
| 协议不变量 | 包类型、端序、固定字段规则、最大字段宽度 | 协议版本控制，不自动改变 |
| 策略硬上限 | 内存、消息/peer/channel、线程、最大重试/寿命 | 配置/硬件推导；网络样本不能突破 |
| 协商参数 | DATA profile、stride、完整性、FEC 方案 | 可靠协商后在安全边界切换，含版本/确认/旧上下文保留 |
| 本地自调参数 | batch、并行粒度、ACK 间隔、pacing、重排结构 | 有界观察、试探、迟滞、回滚 |

FEC 码/符号格式不是纯本地参数：对端必须理解，码率变化也要能自描述或经协商。每帧 stride 固定；参数切换不能让在途包失去解释。会话语义不会为提速而自动降低。

控制状态为 Bootstrap→Probe→Ramp→Steady，错误走 Reassess/Degraded，稳定后 Recover。bootstrap按第13节固定，D初值1200B并允许按已知路径上限降低，仍需实际PMTU适配；不承诺一 RTT 或固定 10 秒内收敛。控制器记录样本量、噪声、时间与回滚原因；一次调整一个参数或明确耦合资源组。

Governor 优先保证正确性/资源安全，其次恢复与容量，再优化吞吐与尾时延。Rayon 并行度、RX 数、FEC 和 busy-spin 共享 CPU/热预算，不能分别把逻辑核用满。空闲、无样本、受限设备可长期保持 bootstrap，遥测也有频率与内存上限。

提供 freeze/unfreeze、能力/配置快照、有限长 tuning trace；freeze 只冻结探索性优化，保留 PMTU 错误、资源硬限制、失联退避等安全处理，并记录强制变化。基准将固定参数阶段与自调收敛阶段分开测量。

至少暴露：应用有效字节、DATA/控制/重传/FEC/头字节、包/系统调用计数、已完成/终止/重复/孤儿/校验失败、内核丢弃、cwnd/已授权 credit、重组/重排/应用持有/池空闲字节、allocator 调用与峰值、Rayon 活跃任务/等待/线程、批大小、p50/p99/p99.9、能力失败原因。不可用指标标 unknown，不伪造为 0。

