<div align="center">

<img src="assets/rimuru-hero.png" alt="An original blue slime floating through a network of stars" width="100%">

# R I M U R U

**Message transport over UDP, written in Rust.**

<img src="https://img.shields.io/badge/dynamic/toml?url=https%3A%2F%2Fraw.githubusercontent.com%2Funk-404%2Frimuru%2Frefs%2Fheads%2Fmaster%2FCargo.toml&query=%24.package.version&style=flat-square&label=Version">

[Getting started](#getting-started) · [Transport layers](#transport-layers) · [C API](#c-api) · [Versioning](#versioning) · [Development](#development)

</div>

---

Rimuru is a message-oriented transport library with unreliable frames, reliable messages, and ordered channels. Create an endpoint and the library manages its I/O runtime; applications do not need to construct one.

> The name is a nod to Rimuru Tempest from *That Time I Got Reincarnated as a Slime*. This is an independent project, unaffiliated with the original work or its rights holders. The banner is an original, generated illustration, not an official character or asset.

## 🌋 Transport layers

| Layer | Delivery | Use when |
|:--|:--|:--|
| **L1** | Whole frames, without retransmission or delivery guarantees | A missed update can be replaced by the next one |
| **L2** | Reliable, whole messages without ordering | Every message matters, but messages are independent |
| **L3** | Reliable, whole messages ordered within a channel | The order of events matters |

Delivery guarantees depend on a live session, available resources, and successful recovery. Persistence across process crashes is outside the current scope. CRC32C detects accidental corruption; **Rimuru does not provide encryption or authentication**.

## 🍰 Getting started

The first release is planned as `0.0.1-beta`. See [Cargo.toml](Cargo.toml) for the required Rust version.

``` rust
use rimuru::{Endpoint, EndpointConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sender = Endpoint::bind("127.0.0.1:0".parse()?, EndpointConfig::default())?;
    let receiver = Endpoint::bind("127.0.0.1:0".parse()?, EndpointConfig::default())?;
    let session = sender.connect_async(receiver.local_addr()?).await?;

    let ticket = session.submit_reliable_async(b"Hello, Rimuru!").await?;
    let message = receiver.recv_async().await?;
    assert_eq!(message.as_slice(), b"Hello, Rimuru!");
    ticket.wait_async().await?;

    sender.close_async().await;
    receiver.close_async().await;
    Ok(())
}
```

The example uses Tokio as the application executor. Add it to your application's dependencies alongside Rimuru:

``` toml
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

Rimuru owns its own I/O runtime, and its async methods can also be polled by other executors. Receive and ticket waits are event-driven; they do not poll on a timer. `recv_batch_async` waits for one frame and then drains currently available frames, up to the requested limit.

For L3, create a channel with `session.channel(id)` and use `channel.submit_async(...)` or `channel.send_async(...)`. Synchronous methods remain available for non-async callers. Endpoint and session handles can be shared across threads. The default datagram size targets common networks; do not assume that a large loopback datagram is suitable for the public Internet.

Run the complete example with `cargo run --example quickstart`.

## 🚀 C API

The crate builds an `rlib`, static library, and dynamic library. [`include/rimuru.h`](include/rimuru.h) is generated with cbindgen; [`examples/ffi_async.c`](examples/ffi_async.c) demonstrates the C API. Handles are thread-safe and completion events can be polled in batches.

```sh
cargo build --locked --lib
cbindgen --config cbindgen.toml --crate rimuru --lang c --output include/rimuru.h
```

Native wait sources use Windows events, Linux/Android eventfd, and Apple pipes. Host integration supports IOCP or kqueue on the respective platforms. Availability and platform validation are described in the design documents.

## 🧾 Versioning

Releases follow **`X.Y.Z`**:

- **X** changes when application protocol versions are incompatible. The intended handshake discovery envelope remains readable across X versions so an application can select a matching implementation.
- **Y** adds compatible protocol content: an older Y ignores optional additions, and a newer Y accepts older data. Extensions must be skippable and preserve existing semantics.
- **Z** leaves protocol content unchanged and covers internal logic or API changes.

## 🤓 Development

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --lib --tests
cargo bench --locked --bench l3 -- --test
```

See [the benchmark guide](benches/README.md) for Criterion and diagnostics. CI runs tests before generating Benchcat charts and uploads the charts, not raw benchmark data. Local Windows and WSL results do not substitute for a native Linux host or macOS CI run.

The design and engineering documents live in [`.agents/docs/`](.agents/docs/).
