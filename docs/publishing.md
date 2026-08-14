# DroidMux 发布流程

本文记录 DroidMux SDK 的 crates.io 发布准备和顺序。执行 `cargo publish` 会产生
不可撤销的公开版本，因此本文中的验证命令默认只生成和检查本地归档，不执行真实
发布。

## 包命名

对外 facade 使用 `droidmux`，协议子包统一使用 `droidmux-*` 命名空间。仓库中的
`crates/droidmux-*` 目录和 `adb-*` workspace 依赖键是内部实现细节；Cargo 通过
`package = "droidmux-*"` 将它们映射到公开包名。

截至 2026-07-28，以下精确名称在 crates.io API 中均返回未注册。该检查不构成
名称保留，正式发布前必须再次确认。

## 发布前置条件

1. 确认所有 SDK package 的 `repository` URL 指向当前公开源码仓库。
2. 确认每个 `.crate` 归档包含 `LICENSE-MIT` 和 `LICENSE-APACHE`；包含
   scrcpy-server 的 `droidmux-control` 还必须保留对应第三方许可证。
3. 固定版本号，审查公开 API 与 feature 默认值，并更新 changelog。
4. 运行 workspace 的 fmt、clippy、test 和 build 门禁。
5. 对每个包运行 `cargo package --list` 和 `cargo package --no-verify`，检查归档内容。
6. 发布包含 `control` feature 的构建前完成 H.264 和 H.265/HEVC 专利评估。
7. 发布启用 `decoder-ffmpeg` 的二进制前，记录 FFmpeg 的构建配置、链接方式、运行库和
   许可证声明；SDK crate 本身不捆绑 FFmpeg 二进制文件。

## 依赖顺序

crates.io 只允许依赖已经存在于 registry 的包，因此必须从底层向 facade 发布：

1. `droidmux-protocol`、`droidmux-auth`
2. `droidmux-transport`
3. `droidmux-transport-tcp`、`droidmux-client`
4. `droidmux-shell`、`droidmux-sync`、`droidmux-logcat`、
   `droidmux-screenshot`、`droidmux-forward`、`droidmux-pairing`
5. `droidmux-package`、`droidmux-control`
6. `droidmux`

同一层的包没有运行时依赖关系，可以分别发布。每发布一层，应等待 crates.io index
可解析新版本，再验证下一层。

## 本地验证

最低层包可以执行完整 package 验证：

```powershell
cargo package -p droidmux-protocol --allow-dirty
cargo package -p droidmux-auth --allow-dirty
```

上层包在依赖尚未真正发布前，需要将全部 DroidMux package 放在同一次
`cargo package` 调用中。单独对 facade 使用 `--no-verify` 仍会查询 crates.io，
并因找不到尚未发布的内部依赖而失败：

```powershell
cargo package --allow-dirty --no-verify `
  -p droidmux-protocol -p droidmux-auth -p droidmux-transport `
  -p droidmux-transport-tcp -p droidmux-client -p droidmux-shell `
  -p droidmux-sync -p droidmux-logcat -p droidmux-screenshot `
  -p droidmux-forward -p droidmux-pairing -p droidmux-package `
  -p droidmux-control -p droidmux
cargo package -p droidmux --list --allow-dirty
```

底层依赖进入 crates.io index 后，必须去掉 `--no-verify`，对将要发布的包重新运行
`cargo package`。真实发布必须由维护者逐包执行，并在每次命令前核对包名与版本：

```powershell
cargo publish -p <exact-package-name> --dry-run
# 人工检查通过后，才执行不带 --dry-run 的命令。
```

## 版本策略

`0.x` 阶段允许 API 继续演进，但每次破坏性修改仍需在 changelog 中明确记录。
facade 和内部包当前统一版本，简化首个版本的依赖解析。稳定后可以让内部包独立
版本化，但 facade 的 feature 名和公开 re-export 路径应优先保持兼容。

如果某一层发布失败，不得跳过依赖顺序。修复后增加 patch 版本重新发布；crates.io
版本不能覆盖或删除，只能 yank，且 yank 不应被当作撤销已公开源码的手段。
