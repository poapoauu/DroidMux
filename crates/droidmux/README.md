# DroidMux

面向 Rust 的原生异步 ADB 协议 SDK。

DroidMux 直接连接 Android 设备上的 `adbd`，实现 ADB packet、认证、无线
调试、逻辑流复用和常用设备服务。运行时不调用 `adb.exe`、`fastboot.exe`、
ADB Server 或 scrcpy 桌面程序。

> 当前版本为 `0.1.0`，尚未发布到 crates.io。
> 公开发布前仍会进行一次 crate 命名空间和 semver API 审查。

## 为什么叫 DroidMux

ADB 在一条设备连接上通过本地 ID 和远端 ID 复用多个逻辑服务流。DroidMux
直接描述了这个核心能力：Android 设备通信（Droid）和流复用（Mux）。

## 功能

- ADB 1.0.0/1.0.1 packet 编解码、校验和、长度限制和增量读取。
- 原生 TCP transport，支持 IPv4、IPv6、超时、分片读取和取消恢复。
- RSA-2048 ADB 主机认证，支持按顺序尝试多把已授权密钥。
- Android 11+ 六位码无线配对、SPAKE2、TLS 1.3 和 STLS 连接。
- 并发逻辑流、独立流关闭、逐流 `WRTE`/`OKAY` 流量控制。
- Shell v2：stdin、stdout、stderr、退出码、PTY、窗口尺寸和关闭 stdin。
- Sync v1/v2：目录、元数据、上传、下载、进度、取消和 64 位文件信息。
- 应用管理：查询、启动、停止、kill、卸载、单 APK 和 Split APK session 安装。
- 流式 Logcat、PNG 截图和有界本地 TCP forwarding。
- 可选屏幕镜像与输入控制，直接使用内嵌的 Android 端 scrcpy-server。

## 架构

DroidMux 是一个 feature-gated facade。协议层没有 Dioxus、SQLite 或桌面 UI
依赖，各底层 crate 也可以单独使用。

| 层 | crate | 职责 |
| --- | --- | --- |
| Wire | `droidmux-protocol` | ADB header、packet、checksum 和边界验证 |
| Transport | `droidmux-transport`, `droidmux-transport-tcp` | transport trait 和 Tokio TCP |
| Identity | `droidmux-auth` | RSA 主机身份、签名和 Android 公钥编码 |
| Pairing | `droidmux-pairing` | Android 11+ 配对、TLS 和凭据接口 |
| Multiplexing | `droidmux-client` | CNXN/AUTH 状态机和并发逻辑流 |
| Services | `droidmux-shell`, `droidmux-sync`, `droidmux-package` | Shell、文件和应用管理 |
| Observability | `droidmux-logcat`, `droidmux-screenshot` | 日志和截图 |
| Networking | `droidmux-forward` | 本地 TCP 到设备 `tcp:<port>` 的转发 |
| Screen | `droidmux-control` | scrcpy-server 会话、H.264 解码和输入注入 |

## Feature

| Feature | 默认 | 内容 |
| --- | :---: | --- |
| `tcp` | 是 | Tokio TCP transport |
| `pairing` | 是 | 无线调试配对和 TLS，自动启用 `tcp` |
| `shell` | 是 | Shell v2 和 legacy shell |
| `sync` | 是 | Sync v1/v2 文件服务 |
| `package` | 否 | 应用与 APK 管理，自动启用 `shell`、`sync` |
| `logcat` | 否 | 有界流式 Logcat |
| `screenshot` | 否 | PNG 截图 |
| `forward` | 否 | 有界 TCP forwarding |
| `control` | 否 | 屏幕镜像和控制，自动启用 `shell`、`sync` |
| `full` | 否 | 启用以上所有高级服务 |

## 环境要求

- Rust 1.85 或更高版本。
- Tokio runtime。
- 传统 TCP ADB，或 Android 11+ 无线调试端点。
- 设备端必须允许当前主机密钥；首次授权可能需要在设备上确认。

当前 workspace 中可使用路径依赖：

```toml
[dependencies]
droidmux = { path = "crates/droidmux", features = ["full"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

公开发布后，应用只需要依赖 facade crate：

```toml
[dependencies]
droidmux = { version = "0.1", features = ["full"] }
```

仓库源码目录仍使用 `crates/droidmux-*`，workspace 也保留 `adb-*` 依赖别名，避免应用层
出现无意义的源码迁移；其 crates.io package 名统一为 `droidmux-*`。维护者发布顺序
和 dry-run 方法记录在仓库根目录的 `docs/publishing.md`。

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

### 多主机密钥

`AdbClient::connect_with_authenticators` 会按给定顺序尝试已有密钥。全部签名
被拒绝后，只会提交第一把密钥的公钥用于首次授权。

后台扫描应使用 `connect_authorized_with_authenticators`。该入口永远不会提交
新公钥，因此不会意外弹出 Android 授权对话框。

### Split APK 安装

启用 `package` 后，`PackageManager::install_split_apks` 使用 Android package
installer session 流式写入 base APK 和 split APK。任何取消、写入失败或提交
失败都会尝试 `install-abandon`，不会在设备文件系统留下暂存 APK。

### TCP forwarding

启用 `forward` 后，`TcpForwarder` 将一个显式本地监听地址映射到设备端
`tcp:<port>` 服务。默认应绑定 `127.0.0.1` 或 `::1`；只有确实需要被其他主机
访问时才绑定局域网地址。

## 安全边界

- DroidMux 不负责明文保存私钥。应用必须实现或接入平台保护的凭据存储。
- 配对码、私钥、TLS exporter 和 session key 不应写入日志。
- 传统 ADB 的 RSA/SHA-1 签名是协议兼容要求，不用于密码散列或新数据完整性设计。
- packet、Shell 输出、Sync chunk、截图、日志和转发连接均有防御性上限。
- SDK 不会自动暴露本地转发端口，也不会在后台连接时申请新授权。

## 兼容性

当前离线测试覆盖 packet 分片、认证、多流、Shell v2、Sync v1/v2、无线配对、
Split APK session、转发和异常关闭。实机基线已覆盖 Android 14 / API 34；在
正式稳定版本前仍需补齐 Android 8、10、11、13 和 15 的设备矩阵。

暂未实现：

- USB discovery/brokering；
- ADB Server 5037 兼容层；
- fastboot 协议；
- `delayed_ack` / burst mode；
- Sync v2 压缩；
- `abb`、`abb_exec`、root/unroot、reverse 和 reconnect 服务。

未实现的 feature 不会在 CNXN 中声明。

## 屏幕控制说明

`control` feature 不执行 scrcpy 桌面客户端，但会携带并上传 Apache-2.0 许可的
`scrcpy-server` Android 组件。视频由源码构建的 `openh264` 解码。BSD-2-Clause
许可证不授予 H.264 专利权；公开或商业分发二进制前，应针对目标地区完成专利
评估。只需要 ADB 协议能力时，可以关闭 `control` feature。

## 测试

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo build --workspace --all-features
```

需要设备的测试默认标记为 ignored，通过环境变量显式启用：

```powershell
$env:DROIDMUX_LIVE_ADB_ENDPOINT = "192.168.1.20:5555"
cargo test --workspace --all-features -- --ignored --nocapture
```

## 许可证

DroidMux 采用 `MIT OR Apache-2.0` 双许可证，使用者可以任选其中一个许可证。MIT
便于开源和商业集成；Apache-2.0 额外提供明确的专利授权和贡献保护。直接依赖采用
兼容的 MIT、Apache-2.0、BSD 或 ISC 许可证，各依赖仍保留自己的版权和许可证。

- MIT: [`LICENSE-MIT`](LICENSE-MIT)
- Apache-2.0: [`LICENSE-APACHE`](LICENSE-APACHE)

可选的 `droidmux-control` 包含 Apache-2.0 的 scrcpy-server，并在其 crate 归档中
携带 `LICENSE-SCRCPY`。OpenH264 使用 BSD-2-Clause，但该版权许可证不授予 H.264
专利许可；发布包含 `control` feature 的二进制前仍需评估适用地区的专利义务。

## 项目边界

DroidMux 是直接连接 `adbd` 的客户端 SDK，不是 ADB CLI 的逐命令复刻，也不是
ADB Server 的替代品。应用层的设备列表、数据库、凭据 UI、任务调度和桌面界面
不属于 SDK。
