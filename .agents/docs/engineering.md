> 本文保留原始设计细节；如与 [版本和现状](versioning.md) 冲突，以后者和当前源码为准。历史测量归档保存在本机忽略目录，不构成当前验收。

# 13. 代码风格与工程约定

本文件把仓库既有风格显式化，供后续改动与审查使用。若与 rustfmt/clippy 冲突，
以工具输出为准；若不冲突，按本文件。

## 13.1 语言与工具

- Rust 2024，MSRV 1.99.0；不用 nightly 语法进生产路径。
- 不定义 Cargo `features`；平台差异用 `cfg(target_os)` 与运行时能力探测。
- `cargo fmt` 默认配置；`max_width = 100`。提交前 `cargo fmt --all`。
- `cargo clippy --all-targets -- -D warnings` 必须为 0。
- 注释与文档字符串用英文；设计/流程文档用中文（与 `docs/` 一致）。

## 13.2 模块与职责

- 含多个子职责的单元用目录模块：`mod.rs` 声明与重导出，子文件承担行为。
- 叶子职责保持单文件，不为对称而造空模块。
- 文件头 `//!` 只写**职责与不变量**，不复述代码，不写与实现无关的愿景。
- 模块声明顺序：`mod` / `pub mod` 在前，`use` 随后（rustfmt 负责组内排序）。
- 私有测试统一放在同目录 `test.rs`，文件末尾 `#[cfg(test)] mod test;`，不得插在
  其它 item 中间。
- 公开 `pub use` 集中在 `lib.rs`；子模块内部用 `pub(crate)`。

## 13.3 命名

| 类别 | 规则 | 例 |
|---|---|---|
| 类型/枚举 | `UpperCamelCase`，名词 | `Context`、`WaiterState` |
| 函数/方法 | `snake_case`，动词短语 | `pin_handle`、`publish_event` |
| 常量 | `SCREAMING_SNAKE_CASE`，带单位/域 | `MAX_BATCH_ITEMS`、`CONTROL_QUEUE_SLOTS` |
| 协议字段 | 与 wire/DOCS 同名（短名），Rust 侧保持 | `idx_bytes`、`meta_flags` |
| 共享上下文 | `Context`（共享状态）；owner 邮箱 `Owner`；门面 `Endpoint` | 不用 `Shared`/`Manager` 这类泛名 |
| 时间 | `now`、`at`、`timeout`、`deadline` 明确语义 | 不写模糊的 `t` |

## 13.4 导入

- 标准库一个 `std::{...}`；外部 crate 按字母序；`crate::{...}` 单独一组。
- 禁止在函数体内写长路径 `crate::module::Type`；在文件顶部导入。仅以下例外：
  - 宏展开需要绝对路径；
  - `cfg` 分支内一次性使用且导入会引入未使用警告；
  - 文档/测试中刻意的完整路径。
- 不使用 `use crate::*`；不重命名导入，除非解决真实冲突。
- 同一模块内引用自身类型用 `Self`。

## 13.5 数字与常量

- 协议/资源界限不得散落魔数。提取为具名常量，注释给出单位与依据（DOCS 节号）。
- 位宽转换显式：`u32::try_from(x)` / `as` 仅在已证明不截断处使用。
- 容量、批大小、超时、重试上限必须能被配置或由冻结常量解释。
- 乘法、数组长度、偏移一律 checked（或先证明上界）。

## 13.6 错误与 panic

- 库错误统一 `thiserror` 的 `Error`；只在 FFI 边界转成 `RmStatus`。
- 不用 `unwrap`/`expect` 处理外部输入；仅可用于：
  - 已由前置检查保证的内部不变量，且附一行 `// INVARIANT:` 注释；
  - 测试与 benchmark。
- 不 `panic!` 处理网络输入；解析失败返回 `Error::Protocol`。
- `unsafe` 块必须有 `// SAFETY:` 注释说明指针/生命周期/别名依据。
- FFI 入口不得 unwind；统一包络 `catch_unwind`。

## 13.7 并发与所有权

- 跨线程共享句柄 `Clone` + `Send + Sync`，内部 `Arc`。
- 共享可变状态用原子或短临界区；不持锁跨 await、回调、Rayon join 或其它表操作。
- 注释写清内存序意图（`Acquire`/`Release` 保护哪个断言），不写“线程安全”空话。
- owner 串行修改协议状态；跨 owner 只传拥有型数据或有界命令。

## 13.8 测试

- 测试名描述行为与边界，不写 `test1`。
- 每个测试自带拓扑/配置，不依赖执行顺序；需要共享夹具时用 `testkit`。
- 网络测试统一用 loopback 数值地址与显式超时。
- 外部故障注入（drop/reorder/delay）走 `rimuru-testkit` 的 relay，不在测试内复制。
- FFI 测试覆盖：布局、导出面、句柄生命周期、并发、CQ 唤醒、关闭/视图存活。

## 13.9 基准与观测

- benchmark 不是测试：正确性断言只保留“夹具不丢包/不超时”的最低限度。
- 命名 `层级_拓扑/线程/分片/消息`，例如 `frame_loopback/t1/d1200`。
- 吞吐、CPU、syscall、内存峰值、尾时延同表报告；不单挑有利指标。
- 结果文件写入 `benchmark-results/`（git 忽略），schema 带版本。

## 13.10 FFI 专属约定

- 导出函数名 `rm_`，类型名 `Rm`，全部 `#[unsafe(no_mangle)] pub extern "C"`。
- 所有 `#[cfg]` 只出现在实现内部；导出符号与签名在全部目标上一致。
- 结构体 `repr(C)`，首字段 `abi_version`/`struct_size`；输出参数先校验再写。
- 句柄是 `u64` token，不是指针；`0` 非法；所有查询/操作先 pin 再执行。
- 失败时把输出句柄置 0、计数置 0；批量部分成功以 `accepted_count` 为准。
