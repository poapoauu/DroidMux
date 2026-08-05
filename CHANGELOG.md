# Changelog

All notable changes to DroidMux will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows Semantic Versioning.

## [Unreleased]

### Added

- Direct libusb USB Host transport with Android interface discovery and per-device read queues.
- Wireless ADB mDNS discovery for TLS connect, TLS pairing, and legacy ADB services.
- Opt-in `delayed_ack` burst mode with per-stream byte windows and concurrent `WRTE` support.
- Per-device automatic reconnect with repeated CNXN/AUTH negotiation and cancellable backoff.
- Direct `abb`, `abb_exec`, root, unroot, reconnect, and reverse-forwarding service APIs.
- Sync v2 transfer compression with Brotli, LZ4, and Zstandard negotiation.
- Opt-in live TCP, Sync, Shell, and direct USB integration tests.

### Changed

- Isolated transport state, reconnect policy, control queues, flow-control credit, and resource
  limits by device session, stream, or transfer for concurrent multi-device embedding.
- Added bounded Shell output and decompressed Sync download limits with per-operation overrides.
- Made Sync downloads commit atomically without replacing an existing destination.
- Bounded each USB device reader queue and made transport shutdown wake blocked readers.

### Fixed

- Preserved queued final `WRTE` payloads and their acknowledgements when a remote `CLSE` arrives
  before the application consumes the stream, including zero-length `WRTE` in delayed-ACK mode.
- Cleared USB endpoint stalls and drained stale packets before starting a new USB session.
- Restored delayed-ACK flow-control credit using signed byte deltas as required by AOSP.
- Prevented reconnect delays and connector attempts from delaying an explicit device close.

### Security

- Enforced Shell output and decompressed download ceilings to limit memory and compression-bomb
  exposure without imposing a shared quota across devices.
- Kept download commits race-safe and prevented partial or racing files from overwriting targets.

## [0.1.0] - 2026-07-29

### Added

- Native ADB packet framing, authentication, transport, and stream multiplexing.
- Android 11+ wireless pairing and TLS transport.
- Shell v2, Sync v1/v2, package management, logcat, screenshots, and TCP forwarding.
- Optional scrcpy-server screen mirroring and input control.
- Feature-gated `droidmux` facade crate.

[Unreleased]: https://github.com/poapoauu/DroidMux/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/poapoauu/DroidMux/releases/tag/v0.1.0
