# DroidMux

[![CI](https://github.com/poapoauu/DroidMux/actions/workflows/ci.yml/badge.svg)](https://github.com/poapoauu/DroidMux/actions/workflows/ci.yml)

面向 Rust 的原生异步 Android Debug Bridge（ADB）协议 SDK。

DroidMux 直接连接 Android 设备上的 `adbd`，提供协议编解码、主机认证、无线
调试、逻辑流复用和常用设备服务。运行时不调用 `adb.exe`、`fastboot.exe`、ADB
Server 或 scrcpy 桌面客户端。

当前版本为 `0.1.0`，公开 API 在进入稳定版本前可能调整。

## 功能

- ADB 1.0.0/1.0.1 packet 编解码、校验和、长度限制和增量读取。
- 原生异步 TCP transport，支持 IPv4、IPv6、超时、取消和分片读取恢复。
- RSA-2048 ADB 主机认证，支持按顺序尝试多把已授权密钥。
- Android 11+ 六位码无线配对、SPAKE2、TLS 1.3 和 STLS 连接。
- 并发逻辑流、独立流关闭以及逐流 `WRTE`/`OKAY` 流量控制。
- Shell v2 和 legacy shell，包括 stdin、stdout、stderr、退出码、PTY 和窗口尺寸。
- Sync v1/v2 文件元数据、目录、上传、下载、进度和取消。
- 应用查询、启动、停止、kill、卸载、单 APK 和 Split APK session 安装。
- 流式 Logcat、PNG 截图和有界本地 TCP forwarding。
- 可选屏幕镜像、显示屏选择和输入控制。

## 安装

DroidMux 尚未发布到 crates.io，当前通过 Git 依赖使用：

```toml
[dependencies]
droidmux = { git = "https://github.com/poapoauu/DroidMux", features = ["full"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

默认启用 TCP、无线配对、Shell 和 Sync。只需要基础能力时可以关闭默认 Feature：

```toml
[dependencies]
droidmux = { git = "https://github.com/poapoauu/DroidMux", default-features = false, features = ["tcp", "shell"] }
```

## Feature

| Feature | 默认 | 能力 |
| --- | :---: | --- |
| `tcp` | 是 | Tokio TCP transport |
| `pairing` | 是 | Android 11+ 无线配对和 TLS；自动启用 `tcp` |
| `shell` | 是 | Shell v2 和 legacy shell |
| `sync` | 是 | Sync v1/v2 文件服务 |
| `package` | 否 | 应用与 APK 管理；自动启用 `shell`、`sync` |
| `logcat` | 否 | 有界流式 Logcat |
| `screenshot` | 否 | PNG 截图 |
| `forward` | 否 | 本地 TCP 到设备 `tcp:<port>` 的转发 |
| `control` | 否 | 屏幕镜像和输入控制；自动启用 `shell`、`sync` |
| `full` | 否 | 启用全部高级服务 |

主要公开模块：

| 模块 | 用途 |
| --- | --- |
| `protocol` | ADB header、packet、checksum 和边界验证 |
| `transport`, `tcp` | transport 抽象和 Tokio TCP 实现 |
| `auth`, `pairing` | RSA 主机身份、无线配对和 TLS |
| `client` | CNXN/AUTH 状态机和并发逻辑流 |
| `shell`, `sync`, `package` | Shell、文件和应用管理 |
| `logcat`, `screenshot` | 日志和截图 |
| `forward` | 有界 TCP forwarding |
| `control` | 屏幕镜像、显示屏选择和输入注入 |

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
`AdbClient::connect_authorized_with_authenticators`，该接口不会提交新公钥或触发
授权对话框。

## 安全

- 应用应将 ADB 私钥存入操作系统保护的凭据存储，不要明文持久化。
- 不要记录配对码、私钥、TLS exporter 或 session key。
- ADB 的 RSA/SHA-1 签名仅用于协议兼容，不应用于新的密码散列或完整性设计。
- packet、Shell 输出、Sync chunk、截图、日志和转发连接均设置了防御性上限。
- TCP forwarding 默认应绑定 `127.0.0.1` 或 `::1`。

## 屏幕控制

`control` Feature 携带 Apache-2.0 许可的 Android 端 `scrcpy-server`，但不执行
scrcpy 桌面客户端。视频由源码构建的 `openh264` 解码。

OpenH264 使用 BSD-2-Clause，但该许可证不授予 H.264 专利许可。公开或商业分发
启用 `control` 的二进制前，应评估目标地区的 H.264 专利义务。

## 兼容性

离线测试覆盖 packet 分片、认证、多流、Shell v2、Sync v1/v2、无线配对、Split
APK session、转发和异常关闭。当前实机基线为 Android 14 / API 34。

尚未实现：

- USB discovery/brokering；
- ADB Server 5037 兼容层；
- fastboot 协议；
- `delayed_ack` / burst mode；
- Sync v2 压缩；
- `abb`、`abb_exec`、root/unroot、reverse 和 reconnect 服务。

详细对比见 [`docs/compatibility.md`](docs/compatibility.md)。

## 开发与测试

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo build --workspace --all-features
```

需要设备的测试默认标记为 ignored：

```powershell
$env:DROIDMUX_LIVE_ADB_ENDPOINT = "192.168.1.20:5555"
cargo test --workspace --all-features -- --ignored --nocapture
```

## 许可证

DroidMux 采用 `MIT OR Apache-2.0` 双许可证：

- [MIT](LICENSE-MIT)
- [Apache-2.0](LICENSE-APACHE)

`droidmux-control` 归档额外携带 scrcpy-server 的 `LICENSE-SCRCPY`。许可证选择与
第三方分发说明见 [`docs/licensing.md`](docs/licensing.md)。
