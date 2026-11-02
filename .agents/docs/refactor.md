# 模块职责与 FFI 工程合同

> 本文保留重构约束和依赖图；涉及版本、实现状态和历史测试数以 [版本与现状](versioning.md) 及当前代码为准。

## 12.2 解耦与拆分需求（RF）

| ID | 要求 | 判据 |
|---|---|---|
| RF-01 | 协议层不得依赖公开门面模块 `endpoint`/`session` 的**行为** | `wire/reassembly/reliable/ordered/handshake` 只依赖 `context`、`wire`、`buffer`、`checksum` 等下层模块；`grep` 可审查 |
| RF-02 | 单一 owner 上下文与 owner 邮箱分离 | `context::Context` 只承载共享资源与观测；`dispatch` 承载命令、peer 表、收包循环；`endpoint` 只做门面 |
| RF-03 | 按职责拆分过长文件 | `config` 拆 endpoint/runtime/reliable；`wire` 拆常量/头/BEGIN/控制/解析；`handshake` 拆 cookie/manager；`dispatch` 拆 receive/commands/packet；`reliable` 拆 peer/receive/tx 生命周期 |
| RF-04 | 依赖方向单向、无门面倒置 | 目标图见 12.6；owner-adjacent 公共句柄（Session/Channel/SendTicket）允许只依赖 `dispatch` 的命令通道 |
| RF-05 | 行为不变 | 现有 88 个测试全部通过；新增 FFI 测试独立计数 |
| RF-06 | 不新增手动 Cargo 功能开关 | `grep -R "cfg(feature" src` 为空；Cargo.toml 无 `[features]` |

## 12.3 项目结构需求（ST）

| ID | 要求 | 落地 |
|---|---|---|
| ST-01 | 根目录只保留产品库、文档、结果目录与工具配置 | `src/`、`tests/`、`include/`、`Cargo.toml`、`rustfmt.toml`、`cbindgen.toml` |
| ST-02 | benchmark 与产品库源码分离，但仍是同一个 package | `benches/` 独立 target；`src/` 无基准实现 |
| ST-03 | 基准共享夹具只有一份，不复制 relay/fixture | `benches/support/`；测试通过显式 `#[path]` 复用 relay |
| ST-04 | 普通 build/test 不编译 benchmark | `autobenches = false` + 显式 `[[bench]]` 清单 |
| ST-05 | 产品库单 package，可产出 rlib/staticlib/cdylib | 根 `Cargo.toml` 的 `[lib] crate-type`，不新建 workspace |
| ST-06 | 目录与模块命名表达职责 | 目录模块用于含子职责单元；叶子职责保持单文件 |

## 12.4 Benchmark 规范化需求（BM）

| ID | 要求 | 落地 |
|---|---|---|
| BM-01 | 矩阵、样本参数、消息尺寸只有一份事实来源 | `benches/support/matrix.rs` |
| BM-02 | Criterion 分组命名、样本数、warmup、测量时间统一 | `support::matrix::group` + `configure` |
| BM-03 | 饱和诊断与 Criterion 统计基准职责分离 | `l1/l2/l3/comparison/l1_costs`（Criterion）与 `udp_goodput`/`l2_goodput`/`l3_goodput`（诊断） |
| BM-04 | JSON schema 版本化、含环境与语义说明 | `support::report`、`support::goodput`；`schema` 字段冻结 |
| BM-05 | 入口命令、输出位置、复现步骤文档化 | `benches/README.md` |
| BM-06 | `--test` smoke 在无性能门槛的 CI 上可跑 | 每个 Criterion entry 支持 `--test` |
| BM-07 | 不把诊断吞吐冒充验收 | README 与报告 notes 明确边界 |

## 12.5 C FFI 需求（FF）

| ID | 要求 | 判据 |
|---|---|---|
| FF-01 | 导出 DOCS 10.5 冻结函数清单，全平台一致 | `src/ffi/abi.rs::EXPORTED_FUNCTIONS` 与 `include/rimuru.h` 的测试比对；另含 `rm_metrics_layout` |
| FF-02 | 句柄为指针宽度 token，clone/release 线程安全、无 UAF | 64 位 32+32 位、32 位 16+16 位；generation 每槽独立；并发测试覆盖 |
| FF-03 | 批量提交 + 有界 MPMC CQ + epoch | `rm_send_batch`、`rm_cq_poll_batch`；poll 无通用堆分配 |
| FF-04 | arm/wait 丢唤醒协议 | `rm_waiter_arm/wait/cancel/reset`；发布-注册全交错测试 |
| FF-05 | 平台等待对象导出 | Windows event + IOCP 绑定；Linux/Android eventfd；Apple pipe + kqueue 绑定；不支持的平台返回 `UNSUPPORTED` |
| FF-06 | 所有可失败入口不跨 C 边界 panic | 全部入口 `catch_unwind` 包络 |
| FF-07 | 错误用固定负状态码，枚举化导出，不改变导出面 | `RmStatus`/`RmEventKind`/`RmTicketState`/`RmWaitSourceKind`/`RmBackend` C enum（`_Static_assert` 固定 4 字节）；平台分支只返回值，不删符号 |
| FF-08 | 自有对接 API 与 C/C++ 可用性 | `include/rimuru.h`、`examples/ffi_async.c`、`ffi-usage.md` |
| FF-09 | 性能约束可审查 | 空 poll 无分配/无 syscall；无 armed waiter 不 signal；批量 pin 不持锁跨操作 |
| FF-10 | cbindgen 头文件是唯一生成事实来源 | `cbindgen.toml` + `include/rimuru.h` + CI diff |

## 12.6 耦合审计与目标依赖图

审计发现的倒置依赖（重构前）：

| 位置 | 依赖 | 问题 |
|---|---|---|
| `reassembly/mod.rs`、`reliable/rx.rs`、`ordered/channel.rs`、`handshake.rs` | `endpoint::Shared` | 协议层依赖公开门面内部状态 |
| `endpoint/receive.rs` | 同时持有收包循环、命令处理、包分派、peer 生命周期 | 职责过载，难单测 |
| `session/{mod,reliable,ordered}.rs` | `endpoint::{Command, Inner}` | 门面与 owner 邮箱混在一起 |
| `reliable/ticket.rs`、`ordered/channel.rs` | `endpoint::Command` | 公共句柄直接构造 owner 命令 |
| `benches/transport/mod.rs` | `#[path]` 引用 `tests/l2/support` | benchmark 与测试夹具互相穿透，路径脆弱 |
| `src/benchmarking.rs` | 公开（doc-hidden）基准夹具 | 无法完全移出库外；压缩为只暴露拓扑控制的最小面 |

目标依赖图（箭头为“依赖”）：

```text
error ← config ← runtime ← udp ← context
wire ───────────────────────────↗  │
buffer ← checksum ← reassembly ────┤
                                   ├← handshake
                                   ├← ordered ── buffer
                                   ├← reliable ─ reassembly, ordered
                                   │
dispatch（owner：命令、peer、收包、包分派）── 依赖以上全部
endpoint ─ dispatch, context（公开门面）
session / reliable::ticket / ordered::channel ─ dispatch 的命令通道（owner-adjacent）
ffi ─ endpoint/session/channel 公共 Rust API + context
```

说明：`context` 持有 `SessionState`（纯数据）与 `ChannelEvent` 队列类型，以消除协议层
对 `endpoint`/`session` 门面的依赖；`dispatch` 是唯一的 owner 实现，门面与公共句柄
把命令投递给它。Rust 允许模块环，但本图保证**行为依赖**单向。

