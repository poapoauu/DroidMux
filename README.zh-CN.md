# DroidMux

[English](README.md) | [简体中文](README.zh-CN.md)

[![CI](https://github.com/poapoauu/DroidMux/actions/workflows/ci.yml/badge.svg)](https://github.com/poapoauu/DroidMux/actions/workflows/ci.yml)

**纯 Rust 异步 Android Debug Bridge（ADB）客户端库和嵌入式 SDK。**

DroidMux 通过 USB、TCP 或 Android 无线调试直接连接设备上的 `adbd` 守护进程，提供
协议编解码、主机认证、设备发现、多设备并发会话、逻辑流复用和常用 ADB 服务。运行时
不需要启动 ADB Server，也不会调用 ADB 命令行程序。

> 当前版本为 `0.1.0`，首次稳定版本发布前，公开 API 仍可能调整。

## 为什么叫 DroidMux

ADB 使用本地 ID 和远端 ID，在一条设备连接上复用多个逻辑服务流。DroidMux 描述的正是
这个核心职责：Android 设备通信（Droid）与流复用（Mux）。

这个库面向需要将 ADB 能力嵌入 Rust 应用的场景，例如设备管理工具、测试基础设施、自动化
服务和桌面应用。每个 `AdbClient` 独立拥有一个设备会话；应用可以同时持有多个客户端，
不同设备之间不会共享 transport 状态、重连策略、流量控制额度或资源上限。

## 功能

- ADB 1.0.0/1.0.1 packet 编解码、校验和、长度限制和增量读取。
- 原生异步 TCP transport 和直接 libusb USB Host transport。
- Android 无线调试 mDNS 发现，支持现代 TLS 和传统 ADB 服务类型。
- RSA-2048 ADB 主机认证，支持按顺序尝试多把已授权密钥。
- Android 11+ 六位码配对、SPAKE2、TLS 1.3 和 STLS 连接升级。
- 多设备并发会话和相互隔离的逻辑流生命周期管理。
- 设备级自动重连，重新执行 CNXN/AUTH 协商并支持可取消的指数退避。
- 可选 `delayed_ack` burst mode，按 stream 使用字节窗口并支持并发 `WRTE`。
- Shell v2 和 legacy shell，包括 stdin、stdout、stderr、退出码、PTY 和窗口尺寸。
- Sync v1/v2 元数据与传输、压缩协商、进度和取消。
- 应用查询、启动、停止、kill、卸载、单 APK 和 Split APK session 安装。
- 流式 Logcat、PNG 截图、reverse forwarding 和有界本地 TCP forwarding。
- 直接 `abb`、`abb_exec`、root、unroot 和 reconnect 服务接口。
- 可选屏幕镜像、显示屏选择和输入控制。

## 项目边界

DroidMux 是直接连接 `adbd` 的客户端 SDK。它有意不实现以下内容：

- ADB 命令行客户端，或对 `adb` 的逐命令复刻；
- ADB Server 的 5037 端口兼容协议；
- 设备列表 UI、凭据 UI、数据库、任务调度器或桌面应用；
- fastboot 客户端，因为 fastboot 是独立协议。

## 环境要求

- Rust 1.85 或更高版本。
- Tokio runtime。
- 直接 USB 设备、传统 TCP ADB 端点，或 Android 11+ 无线调试端点。
- 已被设备授权的主机密钥。首次授权可能需要在 Android 设备上确认。
- 使用直接 USB 时，需要独占 ADB USB 接口。通过 libusb 打开同一设备前，应先停止系统
  ADB Server。

## 安装

DroidMux 尚未发布到 crates.io，当前请使用 Git 依赖：

```toml
[dependencies]
droidmux = { git = "https://github.com/poapoauu/DroidMux", features = ["full"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

默认启用 TCP、无线配对、Shell 和 Sync。只需要较小的协议能力时，可以关闭默认 Feature：

```toml
[dependencies]
droidmux = { git = "https://github.com/poapoauu/DroidMux", default-features = false, features = ["tcp", "shell"] }
```

## Cargo Feature

| Feature | 默认 | 能力 |
| --- | :---: | --- |
| `tcp` | 是 | Tokio TCP transport |
| `usb` | 否 | 直接 libusb USB Host transport；不会启动 adb-server |
| `mdns` | 否 | 发现 `_adb-tls-connect._tcp.local.` 和 `_adb._tcp.local.` 端点 |
| `pairing` | 是 | Android 11+ 无线配对和 TLS；自动启用 `tcp` |
| `shell` | 是 | Shell v2 和 legacy shell |
| `sync` | 是 | Sync v1/v2 文件服务 |
| `package` | 否 | 应用与 APK 管理，以及语义独立的强制停止和进程终止；自动启用 `shell` 和 `sync` |
| `logcat` | 否 | 有界流式 Logcat |
| `screenshot` | 否 | PNG 截图 |
| `forward` | 否 | 本地 TCP 到设备 `tcp:<port>` 服务的有界转发 |
| `control` | 否 | 屏幕镜像和输入控制；自动启用 `shell` 和 `sync` |
| `full` | 否 | 启用全部可选 transport 和高级服务 |

## 快速开始

以下示例直接连接传统 TCP ADB 端点并执行 Shell v2 命令：

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

首次连接传统 TCP ADB 时，设备可能要求确认主机公钥。后台探测设备应使用
`AdbClient::connect_authorized_with_authenticators`；该接口不会提交新公钥，因此不会
触发 Android 授权对话框。

## 多设备嵌入

每个实体设备应创建一个 transport 和一个 `AdbClient`。多个设备会话可以并行运行，且每个
会话都独立保存以下状态：

- transport reader 和有界 packet 队列；
- 已协商 Feature 和认证状态；
- 逻辑流 ID、关闭状态和流量控制窗口；
- transport 重建工厂、退避策略和连接状态；
- Shell、Sync、截图、日志和 forwarding 的资源上限。

自动重连会创建新的 transport 并重新执行 CNXN/AUTH。旧 transport 上的逻辑流会被关闭，
不会使用过期的远端 ID 继续运行，因此调用方应在重连后重新打开所需服务。

## 架构

DroidMux 是由多个专用 crate 组成的 feature-gated facade。协议栈不依赖 GUI、数据库或应用
框架，各底层 crate 也可以单独使用。

| 层 | crate | 职责 |
| --- | --- | --- |
| Wire | `droidmux-protocol` | ADB header、packet、checksum 和边界验证 |
| Transport | `droidmux-transport`, `droidmux-transport-tcp`, `droidmux-transport-usb` | transport trait、Tokio TCP 和 libusb USB |
| Discovery | `droidmux-discovery` | Android 无线调试 mDNS 端点 |
| Identity | `droidmux-auth` | RSA 主机身份、签名和 Android 公钥编码 |
| Pairing | `droidmux-pairing` | Android 11+ 配对、TLS 和凭据接口 |
| Multiplexing | `droidmux-client` | CNXN/AUTH 状态机和并发逻辑流 |
| Services | `droidmux-shell`, `droidmux-sync`, `droidmux-package` | Shell、文件和应用管理 |
| Observability | `droidmux-logcat`, `droidmux-screenshot` | 日志和截图 |
| Networking | `droidmux-forward` | 本地与反向转发服务 |
| Screen | `droidmux-control` | scrcpy-server 会话、H.264 解码和输入注入 |

## 安全边界

- DroidMux 不负责持久化私钥。应用应尽量使用操作系统保护的凭据存储，而不是明文文件。
- 配对码、私钥、TLS exporter 和 session key 不应写入日志。
- 传统 ADB 的 RSA/SHA-1 签名是协议兼容要求，不应用于密码散列或新的数据完整性设计。
- packet、Shell 输出、Sync chunk 与解压数据、截图、日志和转发连接均有防御性上限。
- 除非确实需要被其他主机访问，本地 forwarding 应绑定 `127.0.0.1` 或 `::1`。
- 后台发现应使用仅连接已授权设备的接口，避免在设备上意外请求新的信任授权。

## 兼容性与限制

离线测试覆盖 packet 分片、认证、多设备与多流、burst mode、自动重连、Shell v2、
Sync v1/v2、无线配对、Split APK session、USB framing、转发和异常关闭。实机基线包含
Android 14 / API 34，以及 Windows 上的直接 USB CNXN/AUTH 和并发 Shell v2。当前实体机
测试样本上的 USB 稳定性循环已通过 20 次。

仍需扩展更多 Android 版本、USB 控制器、设备代际，以及 Linux/macOS USB 权限和驱动组合。
完整能力矩阵和当前测试范围见[兼容性审查](docs/compatibility.md)。

尚未实现的 Feature 不会在主机 CNXN Feature 列表中声明。

## 可选屏幕控制

`control` Feature 会携带并上传采用 Apache-2.0 许可证的 Android `scrcpy-server` 组件，
但不会执行 scrcpy 桌面客户端。视频由源码构建的 `openh264` 解码。

`droidmux-control` 默认启用 `decoder-openh264` Feature。应用可以关闭该 Feature，并通过
`MirrorSession::start_with_decoder` 传入公共 `MirrorDecoder` trait 的其他实现。会话使用有界
的长驻解码管线和 latest-frame 交付，渲染端变慢时不会累积延迟。像素缓冲会被复用，
`MirrorPipelineStats` 提供解码耗时和帧计数。

`MirrorSession::next_pixel_frame` 作为 RGBA8888 兼容 API 保持不变。原生渲染器可以调用
`next_pixel_frame_with_format` 请求 `DecodedPixelFormat::Bgra8888`，Windows D3D11 纹理可
直接消费返回的缓冲区，不需要在桥接层再次分配整帧内存完成通道转换。

FFmpeg 不是默认依赖，DroidMux 也不会启动 `ffmpeg.exe`。使用内置的软件解码后端时，直接
依赖 control crate，并提供 `ffmpeg-sys-next` 能发现的 FFmpeg 开发 SDK：

```toml
[dependencies]
droidmux-control = { git = "https://github.com/poapoauu/DroidMux", default-features = false, features = ["decoder-ffmpeg"] }
```

创建 `FfmpegDecoder` 后传给 `MirrorSession::start_with_decoder`。应用负责随程序分发兼容的
FFmpeg shared libraries 和许可证声明。需要运行时切换解码器时，可以同时启用两个解码
Feature。

OpenH264 使用 BSD-2-Clause，但该版权许可证不授予 H.264 专利权。公开或商业分发启用
`control` 的二进制前，应评估目标地区的专利义务。只需要 ADB 协议能力时，可以关闭
`control` Feature。

`PackageManager::force_stop` 和 `PackageManager::kill` 的 Android 语义不同。`force_stop` 会
阻止应用正常重启，直到用户或调用方再次显式启动它；`kill` 对可调试进程优先发送与 Android
Studio 相同的 JDWP/DDMS `EXIT` 请求，然后按设备权限尝试 ActivityManager 和直接信号。
服务可能在 `kill` 后被 Android 重新拉起，受保护的系统进程也可能拒绝所有非 root 回退。

## 开发与测试

运行 CI 使用的检查：

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --features full -- -D warnings
cargo test --workspace --features full
cargo build --workspace --features full
```

需要设备的测试默认标记为 ignored，需要显式启用：

```powershell
$env:DROIDMUX_LIVE_ADB_ENDPOINT = "192.168.1.20:5555"
cargo test --workspace --features full -- --ignored --nocapture
```

直接 USB 测试要求先停止系统 ADB Server，并提供已授权的 PKCS#8 ADB 私钥：

```powershell
$env:DROIDMUX_LIVE_ADB_KEY = Join-Path $env:USERPROFILE ".android\adbkey"
$env:DROIDMUX_LIVE_USB_DEVICE = "2717:ff48"
cargo test -p droidmux --features full --test live_usb -- --ignored --nocapture
```

## 仓库结构

```text
crates/droidmux/                 对外提供的 feature-gated facade
crates/droidmux-protocol/        ADB 线协议
crates/droidmux-client/          认证和逻辑流复用
crates/droidmux-transport-*/     TCP 与直接 USB transport
crates/droidmux-pairing/         无线配对和 TLS
crates/droidmux-{shell,sync,...} 高层设备服务
docs/                            兼容性、许可证和发布说明
```

## 参与贡献

进行较大修改前，请先创建 GitHub Issue，提供可复现的问题或具体方案。协议行为应对照 AOSP
ADB 源码；新增能力应包含有界的失败处理和离线测试。提交实机结果时，应记录 Android 版本、
主机操作系统、transport，以及测试期间系统 ADB Server 是否正在运行。

## 许可证

DroidMux 采用以下任一许可证的双许可证模式：

- [MIT](LICENSE-MIT)
- [Apache-2.0](LICENSE-APACHE)

`droidmux-control` 包额外携带 scrcpy-server 的 `LICENSE-SCRCPY`。许可证选择和第三方分发
说明见[许可证说明](docs/licensing.md)。
