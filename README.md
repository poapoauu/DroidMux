# DroidMux

[English](README.md) | [简体中文](README.zh-CN.md)

[![CI](https://github.com/poapoauu/DroidMux/actions/workflows/ci.yml/badge.svg)](https://github.com/poapoauu/DroidMux/actions/workflows/ci.yml)

**A pure Rust, asynchronous Android Debug Bridge (ADB) client library and embeddable SDK.**

DroidMux connects directly to the `adbd` daemon on Android devices over USB, TCP, or
Android wireless debugging. It provides protocol framing, host authentication, device
discovery, concurrent multi-device sessions, logical stream multiplexing, and common ADB
services without starting an adb server or spawning an ADB command-line process.

> The current version is `0.1.0`. Public APIs may change before the first stable release.

## Why DroidMux

ADB multiplexes many logical service streams over one device connection by assigning each
stream a local and remote ID. DroidMux is named for that core job: Android device
communication (Droid) and stream multiplexing (Mux).

The library is designed for Rust applications that need ADB as an embedded capability,
including device-management tools, test infrastructure, automation services, and desktop
applications. Each `AdbClient` owns one device session; an application can hold multiple
clients concurrently without sharing transport state, reconnect policy, flow-control
credit, or resource limits between devices.

## Capabilities

- ADB 1.0.0/1.0.1 packet encoding, decoding, checksums, size limits, and incremental reads.
- Native asynchronous TCP and direct libusb USB Host transports.
- Android wireless-debugging mDNS discovery for modern TLS and legacy ADB services.
- RSA-2048 host authentication with ordered attempts across multiple authorized keys.
- Android 11+ six-digit pairing with SPAKE2, TLS 1.3, and STLS connection upgrades.
- Concurrent multi-device sessions and independent logical stream lifecycle management.
- Device-level automatic reconnect with repeated CNXN/AUTH negotiation and cancellable
  exponential backoff.
- Opt-in `delayed_ack` burst mode with per-stream byte windows and concurrent `WRTE` packets.
- Shell v2 and legacy shell with stdin, stdout, stderr, exit codes, PTY, and window resizing.
- Sync v1/v2 metadata and transfers, compression negotiation, progress, and cancellation.
- Package query, launch, stop, kill, uninstall, single APK, and split APK session install.
- Streaming Logcat, PNG screenshots, reverse forwarding, and bounded local TCP forwarding.
- Direct `abb`, `abb_exec`, root, unroot, and reconnect service APIs.
- Optional screen mirroring, display selection, and input control.

## Scope

DroidMux is a client SDK that talks directly to `adbd`. It is intentionally not:

- an ADB command-line client or a command-for-command clone of `adb`;
- an implementation of the adb-server protocol on port 5037;
- a device-list UI, credential UI, database, job scheduler, or desktop application;
- a fastboot client, because fastboot is a separate protocol.

## Requirements

- Rust 1.85 or later.
- A Tokio runtime.
- A direct USB device, a traditional TCP ADB endpoint, or an Android 11+ wireless-debugging
  endpoint.
- A host key authorized by the device. First-time authorization can require confirmation on
  the Android device.
- Exclusive access to the ADB USB interface when using direct USB. Stop the system adb server
  before opening the same device through libusb.

## Installation

DroidMux has not yet been published to crates.io. Use the Git dependency for now:

```toml
[dependencies]
droidmux = { git = "https://github.com/poapoauu/DroidMux", features = ["full"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

TCP, wireless pairing, Shell, and Sync are enabled by default. Disable default features when
an application only needs a smaller protocol surface:

```toml
[dependencies]
droidmux = { git = "https://github.com/poapoauu/DroidMux", default-features = false, features = ["tcp", "shell"] }
```

## Cargo Features

| Feature | Default | Capability |
| --- | :---: | --- |
| `tcp` | Yes | Tokio TCP transport |
| `usb` | No | Direct libusb USB Host transport; does not start adb-server |
| `mdns` | No | Discovery for `_adb-tls-connect._tcp.local.` and `_adb._tcp.local.` endpoints |
| `pairing` | Yes | Android 11+ wireless pairing and TLS; enables `tcp` |
| `shell` | Yes | Shell v2 and legacy shell |
| `sync` | Yes | Sync v1/v2 file services |
| `package` | No | Application/APK management plus distinct force-stop and process-kill operations; enables `shell` and `sync` |
| `logcat` | No | Bounded streaming Logcat |
| `screenshot` | No | PNG screenshots |
| `forward` | No | Bounded local TCP forwarding to a device `tcp:<port>` service |
| `control` | No | Screen mirroring and input control; enables `shell` and `sync` |
| `full` | No | All optional transports and high-level services |

## Quick Start

The following example connects directly to a traditional TCP ADB endpoint and executes a
Shell v2 command:

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
    if output.exit_code != Some(0) {
        return Err(format!("device command failed: {:?}", output.stderr).into());
    }

    println!("{}", String::from_utf8_lossy(&output.stdout).trim());
    client.close().await?;
    Ok(())
}
```

Traditional TCP ADB may ask the user to confirm the host public key on the first connection.
Passive discovery should use `AdbClient::connect_authorized_with_authenticators`, which never
submits a new public key and therefore cannot trigger an Android authorization dialog.

## Multi-Device Embedding

Create one transport and one `AdbClient` per physical device. Device sessions can run in
parallel, while every session keeps its own:

- transport reader and bounded packet queues;
- negotiated features and authentication state;
- logical stream IDs, close state, and flow-control windows;
- reconnect factory, backoff policy, and connection state;
- Shell, Sync, screenshot, log, and forwarding resource limits.

Automatic reconnect creates a new transport and repeats CNXN/AUTH. Streams from the failed
transport are closed rather than reused with stale remote IDs, so callers must reopen their
services after reconnection.

## Architecture

DroidMux is a feature-gated facade over focused crates. The protocol stack has no GUI,
database, or application-framework dependency, and lower-level crates can be used directly.

| Layer | Crates | Responsibility |
| --- | --- | --- |
| Wire | `droidmux-protocol` | ADB headers, packets, checksums, and boundary validation |
| Transport | `droidmux-transport`, `droidmux-transport-tcp`, `droidmux-transport-usb` | Transport trait, Tokio TCP, and libusb USB |
| Discovery | `droidmux-discovery` | Android wireless-debugging mDNS endpoints |
| Identity | `droidmux-auth` | RSA host identities, signatures, and Android public-key encoding |
| Pairing | `droidmux-pairing` | Android 11+ pairing, TLS, and stored credentials |
| Multiplexing | `droidmux-client` | CNXN/AUTH state machine and concurrent logical streams |
| Services | `droidmux-shell`, `droidmux-sync`, `droidmux-package` | Shell, files, and package management |
| Observability | `droidmux-logcat`, `droidmux-screenshot` | Logs and screenshots |
| Networking | `droidmux-forward` | Local and reverse forwarding services |
| Screen | `droidmux-control` | scrcpy-server sessions, H.264/H.265/AV1 decoding, and input injection |

## Security Boundary

- DroidMux does not persist private keys. Applications should use an operating-system-protected
  credential store rather than plaintext files where possible.
- Pairing codes, private keys, TLS exporters, and session keys must not be written to logs.
- Traditional ADB RSA/SHA-1 signatures are a protocol-compatibility requirement, not a design
  choice for password hashing or new data-integrity schemes.
- Packets, Shell output, Sync chunks and decompression, screenshots, logs, and forwarding
  connections have defensive bounds.
- Local forwarding should bind to `127.0.0.1` or `::1` unless another host must explicitly
  access the port.
- Background discovery should use an authorized-only connection API so it cannot request a new
  trust decision on the device.

## Compatibility and Limitations

Offline tests cover fragmented packets, authentication, multi-device and multi-stream use,
burst mode, automatic reconnect, Shell v2, Sync v1/v2, wireless pairing, split APK sessions,
USB framing, forwarding, and abnormal closure. The live-device baseline includes Android 14 /
API 34 and direct USB CNXN/AUTH plus concurrent Shell v2 on Windows. A 20-run USB stability
loop completed successfully on the current physical-device fixture.

Remaining validation work includes more Android releases, USB controllers, device generations,
and Linux/macOS USB permission and driver combinations. See the full
[compatibility review](docs/compatibility.md) for the capability matrix and current test scope.

Unimplemented features are not advertised in the host CNXN feature list.

## Optional Screen Control

The `control` feature includes and uploads the Apache-2.0-licensed Android `scrcpy-server`
component, but it does not execute the scrcpy desktop client. H.264 video is decoded by default
with an `openh264` source build.

`droidmux-control` enables its `decoder-openh264` feature by default. Applications can disable
that feature and pass another implementation of the public `MirrorDecoder` trait to
`MirrorSession::start_with_decoder`. The session uses a bounded, long-lived decode pipeline and
latest-frame delivery so a slow renderer does not accumulate latency. Pixel buffers are reused,
and `MirrorPipelineStats` exposes decode timing and frame counters.

`MirrorSession::next_pixel_frame` remains the RGBA8888 compatibility API. Native renderers can
call `next_pixel_frame_with_format` and request `DecodedPixelFormat::Bgra8888`, allowing Windows
D3D11 textures to consume the returned buffer without a bridge-side channel-copy allocation.

FFmpeg is not a default dependency and DroidMux never launches `ffmpeg.exe`. To select the
included software backend, depend on the control crate directly and provide an FFmpeg development
SDK that `ffmpeg-sys-next` can discover:

```toml
[dependencies]
droidmux-control = { git = "https://github.com/poapoauu/DroidMux", default-features = false, features = ["decoder-ffmpeg"] }
```

Set `MirrorOptions::video_codec` to `VideoCodec::H265` or `VideoCodec::Av1`, create the matching
decoder with `FfmpegDecoder::for_codec`, and pass it to
`MirrorSession::start_with_decoder`. `FfmpegDecoder::new` remains the H.264-compatible constructor.
The session validates that the requested encoder, the codec announced by scrcpy-server, and the
decoder instance agree. The application is responsible for shipping compatible FFmpeg shared
libraries and notices. Enabling both decoder features is supported when runtime backend selection
is required; `OpenH264Decoder` accepts H.264 only.

OpenH264 is BSD-2-Clause licensed, but that copyright license does not grant H.264 patent
rights. H.265/HEVC may carry separate patent-pool obligations. Review codec patent obligations
in the target jurisdictions before publicly or commercially distributing a binary with the
`control` feature. Disable `control` when only ADB protocol capabilities are needed.

`PackageManager::force_stop` and `PackageManager::kill` intentionally have different Android
semantics. `force_stop` prevents normal restart until the package is explicitly launched again.
`kill` first uses the JDWP/DDMS `EXIT` request used by Android Studio for debuggable processes,
then tries ActivityManager and a direct signal where device permissions allow it. Android may
restart services after `kill`, and protected system processes may reject every non-root fallback.

## Development and Testing

Run the checks used by CI:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --features full -- -D warnings
cargo test --workspace --features full
cargo build --workspace --features full
```

Tests that require a device are ignored by default and enabled explicitly:

```powershell
$env:DROIDMUX_LIVE_ADB_ENDPOINT = "192.168.1.20:5555"
cargo test --workspace --features full -- --ignored --nocapture
```

Direct USB testing requires the system adb server to be stopped and an authorized PKCS#8 ADB
private key:

```powershell
$env:DROIDMUX_LIVE_ADB_KEY = Join-Path $env:USERPROFILE ".android\adbkey"
$env:DROIDMUX_LIVE_USB_DEVICE = "2717:ff48"
cargo test -p droidmux --features full --test live_usb -- --ignored --nocapture
```

## Repository Layout

```text
crates/droidmux/                 Feature-gated public facade
crates/droidmux-protocol/        ADB wire protocol
crates/droidmux-client/          Authentication and stream multiplexing
crates/droidmux-transport-*/     TCP and direct USB transports
crates/droidmux-pairing/         Wireless pairing and TLS
crates/droidmux-{shell,sync,...} High-level device services
docs/                            Compatibility, licensing, and publishing notes
```

## Contributing

Open a GitHub issue with a reproducible problem or a concrete proposal before a large change.
Protocol behavior should be checked against AOSP ADB, and new capabilities should include
bounded failure handling plus offline tests. Live-device results should record the Android
version, host OS, transport, and whether the system adb server was running.

## License

DroidMux is dual-licensed under either of:

- [MIT](LICENSE-MIT)
- [Apache-2.0](LICENSE-APACHE)

The `droidmux-control` package additionally carries scrcpy-server's `LICENSE-SCRCPY`. See the
[licensing notes](docs/licensing.md) for license selection and third-party distribution details.
