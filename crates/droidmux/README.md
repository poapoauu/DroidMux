# DroidMux

[English](https://github.com/poapoauu/DroidMux#readme) |
[简体中文](https://github.com/poapoauu/DroidMux/blob/main/README.zh-CN.md)

**A pure Rust, asynchronous Android Debug Bridge (ADB) client library and embeddable SDK.**

DroidMux connects directly to Android's `adbd` over USB, TCP, or wireless debugging. It
provides authentication, mDNS discovery, multi-device sessions, logical stream multiplexing,
Shell v2, Sync v2, package management, forwarding, Logcat, and screenshots without starting
an adb server or spawning the ADB command-line client.

> The current version is `0.1.0`. Public APIs may change before the first stable release.

## Designed for Embedding

DroidMux is a client SDK rather than an ADB CLI or adb-server replacement. Create one transport
and one `AdbClient` per physical device. Multiple clients can run concurrently without sharing
transport state, reconnect policy, stream flow-control credit, or operation limits.

The facade is feature-gated, and the lower-level `droidmux-*` crates can also be used directly.
The protocol stack has no GUI, database, or desktop application dependency.

## Capabilities

- ADB 1.0.0/1.0.1 framing and RSA-2048 host authentication with multiple saved keys.
- Native asynchronous TCP and direct libusb USB Host transports.
- Android 11+ wireless pairing, TLS 1.3, STLS, and mDNS discovery.
- Concurrent logical streams, automatic device reconnect, and opt-in `delayed_ack` burst mode.
- Shell v2, Sync v1/v2, package and split APK installation, Logcat, and screenshots.
- Direct `abb`, root/unroot, reconnect, local forwarding, and reverse-forwarding services.
- Optional scrcpy-server screen mirroring and input control.

## Installation

DroidMux has not yet been published to crates.io. Use the Git dependency for now:

```toml
[dependencies]
droidmux = { git = "https://github.com/poapoauu/DroidMux", features = ["full"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

TCP, pairing, Shell, and Sync are enabled by default. Optional capabilities are controlled by
Cargo features:

| Feature | Default | Capability |
| --- | :---: | --- |
| `tcp` | Yes | Tokio TCP transport |
| `usb` | No | Direct libusb USB Host transport |
| `mdns` | No | Wireless ADB endpoint discovery |
| `pairing` | Yes | Android 11+ pairing and TLS; enables `tcp` |
| `shell` | Yes | Shell v2 and legacy shell |
| `sync` | Yes | Sync v1/v2 file services |
| `package` | No | Package/APK management with force-stop and process-kill; enables `shell` and `sync` |
| `logcat` | No | Bounded streaming Logcat |
| `screenshot` | No | PNG screenshots |
| `forward` | No | Bounded local TCP forwarding |
| `control` | No | Screen mirroring and input; enables `shell` and `sync` |
| `full` | No | All optional transports and services |

## Quick Start

```rust,no_run
use std::{net::SocketAddr, sync::Arc};

use droidmux::{
    auth::RsaAdbCredential,
    client::AdbClient,
    shell,
    tcp::{TcpTransport, TcpTransportConfig},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint: SocketAddr = "192.168.1.20:5555".parse()?;
    let credential = Arc::new(RsaAdbCredential::generate("droidmux@example-host")?);
    let transport = TcpTransport::connect(endpoint, TcpTransportConfig::default()).await?;
    let client = AdbClient::connect(Box::new(transport), credential).await?;

    let output = shell::execute(&client, "getprop ro.product.model").await?;
    println!("{}", String::from_utf8_lossy(&output.stdout).trim());

    client.close().await?;
    Ok(())
}
```

Use `AdbClient::connect_authorized_with_authenticators` for passive discovery. It never submits
a new public key, so a background connection cannot trigger an Android authorization dialog.

## Scope and Compatibility

DroidMux does not implement the adb-server protocol on port 5037, an ADB command-line client,
or fastboot. Offline tests cover packet fragmentation, authentication, multi-device and
multi-stream use, burst mode, reconnect, Shell, Sync, pairing, USB framing, and forwarding.
Live-device coverage currently includes Android 14 and direct USB on Windows; more Android
versions, USB controllers, and Linux/macOS combinations remain to be validated.

See the repository's full
[English documentation](https://github.com/poapoauu/DroidMux#readme),
[Chinese documentation](https://github.com/poapoauu/DroidMux/blob/main/README.zh-CN.md), and
[compatibility matrix](https://github.com/poapoauu/DroidMux/blob/main/docs/compatibility.md).

## License

DroidMux is dual-licensed under
[MIT](https://github.com/poapoauu/DroidMux/blob/main/LICENSE-MIT) or
[Apache-2.0](https://github.com/poapoauu/DroidMux/blob/main/LICENSE-APACHE).

The optional `control` feature includes the Apache-2.0-licensed scrcpy-server and an OpenH264
source build. The lower-level `droidmux-control` crate also offers an opt-in `decoder-ffmpeg`
backend for consumers that provide compatible FFmpeg development and runtime libraries. DroidMux
does not launch `ffmpeg.exe`. Review the repository's
[licensing notes](https://github.com/poapoauu/DroidMux/blob/main/docs/licensing.md) before
distributing binaries with screen control enabled.
