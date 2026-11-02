> 本文保留原始设计细节；如与 [版本和现状](versioning.md) 冲突，以后者和当前源码为准。历史测量归档保存在本机忽略目录，不构成当前验收。

# 14. C ABI 1.0 使用与实现说明（v0.17）

本文说明如何在 C/C++ 中使用 Rimuru，以及导出的合同。冻结语义仍以
[FFI 设计](ffi.md) 第 10 节为准；本文记录本轮实现落地后的差异与操作步骤。

## 14.1 构建与头文件

```text
cargo build --lib                                  # rlib + staticlib + cdylib
cbindgen --config cbindgen.toml --crate rimuru --lang c --output include/rimuru.h
```

- 头文件 `include/rimuru.h` 是唯一 C 声明来源，不允许手写第二份。
- 导出符号清单固定为 `src/ffi/abi.rs::EXPORTED_FUNCTIONS`，测试会与头文件逐项比对。
- `#[cfg]` 只出现在实现内部；所有 `rm_*` 在全部目标上编译、导出，签名相同。
- 平台不支持的绑定返回 `RM_UNSUPPORTED`，不删除符号。

## 14.2 句柄

```c
typedef size_t RmHandle;   /* 0 非法 */
```

句柄是指针宽度 token，不是指针，也不是固定 `uint64_t`：

| 目标 | generation | slot | 说明 |
|---|---:|---:|---|
| 64 位 | 32 位 | 32 位 | 低位 slot、高位 generation |
| 32 位 | 16 位 | 16 位 | 最多 65,535 槽；generation 每槽独立递增 |

generation 是**每槽**计数，release 后槽可复用；generation 用尽时该槽永久退休，旧 token
永远返回 `RM_INVALID_HANDLE`。`rm_handle_clone` 创建同一对象的新 token；`release` 只撤销
一个 token。已 pin 的调用可以完成，尚未 pin 的调用失败，不发生 UAF。

## 14.3 状态与枚举

状态码是 C enum `RmStatus`，数值与 DOCS 10.5 完全一致（`RM_OK=0`、`RM_WOULD_BLOCK=1`、
`RM_INVALID_HANDLE=-2` 等）。事件 kind 是 `RmEventKind`，票据状态是 `RmTicketState`，
等待源是 `RmWaitSourceKind`，backend 是 `RmBackend`。生成头中这些是 enum + 固定宽度
typedef；不再为每个状态生成 `#define`。

`RmEvent` 是 POD，`kind`/`status` 字段使用上述 enum 类型，宽度仍为 4 字节。

## 14.4 生命周期

```text
rm_endpoint_bind -> send/poll -> rm_endpoint_close -> rm_handle_release
```

- `rm_endpoint_config_init` 填默认配置；`rm_endpoint_bind(config=NULL|默认, addr, &ep)`。
- `rm_session_open(endpoint, peer, &session)` 建链；`rm_channel_open(session, layer, id, cq, &ch)`
  打开 L3 channel（`cq=0` 用默认 CQ）。
- `rm_endpoint_close` 阻塞到接收 owner 停止；发布一个 `RM_EVENT_ENDPOINT_CLOSED`。
- close 只改变对象状态，不撤销 token；token 仍需 release。重复 close/cancel 幂等。

## 14.5 发送路径

| API | 语义 |
|---|---|
| `rm_send_copy` | 返回前完成池内复制；L1 ticket 立即进入 `RM_TICKET_RECEIVED` |
| `rm_send_borrowed` | 外部借用合同；本轮实现为提交时复制，`LOCAL_BUFFER_RELEASED` 在终态置位；不宣称内核零拷贝 |
| `rm_send_batch` | 最多 256 项，只接受前缀；`accepted_count` 为准，后缀所有权不转移 |
| `rm_tx_builder_*` | 构造器直接映射池内 `FrameLayout` 槽位；write/writev 后 seal 校验覆盖位图；submit 不再二次复制 |

票据终态事件 `RM_EVENT_SEND_TERMINAL` 在协议终态与本地缓冲释放都满足后发布；发送准入前
先预留事件额度，所以 CQ 满不会丢终态。`rm_ticket_query` 可提前查看协议状态，flags bit0
表示本地借用源可复用。

## 14.6 完成队列与 async 协议

默认每 Endpoint 一个 CQ；`rm_cq_create` 创建独立 CQ。`rm_cq_poll_batch` /
`rm_endpoint_poll_batch` 只搬运事件，不推进协议、不做网络 I/O、不分配。

```text
for (;;) {
  poll_batch(cq, events, cap, &count, &epoch);
  if (count) { process(events, count); continue; }
  status = arm(waiter, epoch);
  if (status == RM_READY) continue;          /* 发布发生在 arm 之前 */
  if (status == RM_ARMED) wait(waiter, timeout);  /* 发布发生在 arm 之后 */
  else error(status);
  reset(waiter);
}
```

- 发布者先递增 epoch，再只通知当前 `ARMED` 的 waiter；最多唤醒 `min(4, armed)`，close/fatal
  唤醒全部。
- arm 清 OS 信号、置 ARMED，再用 SeqCst 复核 epoch/pending/closed；两种交错都不会丢唤醒。
- 伪唤醒允许，但必须重新 poll。
- 同一 waiter 只有一个未消费信号；再次等待前需要 `rm_waiter_reset`（取消后也必须 reset）。
- `rm_waiter_wait(timeout_ns=0)` 立即返回；`UINT64_MAX` 表示无超时；close/cancel 可终止等待。

## 14.7 平台等待对象

`rm_waiter_export` 返回 `RmWaitSource { kind, flags, native_value, owner }`：

| 平台 | kind | native_value | 宿主集成 |
|---|---|---|---|
| Linux/Android | `RM_WAIT_POSIX_FD` | eventfd | 加入 epoll |
| Windows | `RM_WAIT_WIN_EVENT` | manual-reset event HANDLE | 等待或 `rm_waiter_bind_iocp(port,key)` |
| macOS/iOS | `RM_WAIT_POSIX_FD` | pipe 读 fd | 等待或 `rm_waiter_bind_kqueue(kq,ident)` |

`owner` 是独立 token，宿主先解除事件循环注册，再释放该 token；不要直接 close/read/reset
库拥有的 fd/HANDLE。解绑后的迟到通知由 generation 判为失效。

## 14.8 消息与视图

消息事件携带 `message` 句柄。`rm_message_view(message, &data, &len, &view)` 返回只读指针和
独立 view token；释放原 message 后 view 仍保持数据有效，直到 view 释放。空消息允许
`len=0`；不要解引用空指针。

## 14.9 指标

`rm_endpoint_metrics_copy(endpoint, dst, capacity, &required)`：

- `dst=NULL` 只查询所需字节数；
- `capacity < required` 返回 `RM_INVALID_ARGUMENT`；
- 成功写入 `RmMetrics` POD（schema 1，首字段 `schema_version`、`struct_size`）。
- `rm_metrics_layout()` 返回同 schema 的零值描述结构，并保证该 POD 出现在头文件中。

## 14.10 错误与 panic

所有可失败入口用 `catch_unwind` 包络；panic 转 `RM_INTERNAL`，不得跨 C 边界 unwind。
参数错误在写输出前检查，失败时句柄/计数置 0。批量释放逐项返回状态，不阻止其他 token。

## 14.11 性能要点

- poll 无通用堆分配；空 poll 无通知 syscall。
- 无 armed waiter 时发布不调用系统 signal。
- 批量 send 只 pin 一次 endpoint；每消息一个终态事件，不为每 datagram 建事件。
- CQ 事件额度在准入时预留；可靠终态不会因队列满被覆盖。
- 借用发送当前为提交时复制；`TxBuilder` 才是避免二次复制的路径。
