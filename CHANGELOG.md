# Changelog

All notable changes to DroidMux will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows Semantic Versioning.

## [Unreleased]

### Added

- A public `MirrorDecoder` interface, long-lived bounded decode pipeline, latest-frame delivery,
  reusable pixel buffers, decoder timing counters, and caller-selectable RGBA/BGRA output.
- An opt-in `decoder-ffmpeg` backend for `droidmux-control`; it links the caller-provided FFmpeg
  development libraries and never launches an external `ffmpeg` executable.
- Android Studio-compatible JDWP/DDMS process termination for debuggable applications, with
  ActivityManager and direct-signal fallbacks for other processes.
- Direct libusb USB Host transport with Android interface discovery and per-device read queues.
- Wireless ADB mDNS discovery for TLS connect, TLS pairing, and legacy ADB services.
- Opt-in `delayed_ack` burst mode with per-stream byte windows and concurrent `WRTE` support.
- Per-device automatic reconnect with repeated CNXN/AUTH negotiation and cancellable backoff.
- Direct `abb`, `abb_exec`, root, unroot, reconnect, and reverse-forwarding service APIs.
- Sync v2 transfer compression with Brotli, LZ4, and Zstandard negotiation.
- Opt-in live TCP, Sync, Shell, and direct USB integration tests.

### Changed

- Screen mirroring now decodes on a dedicated worker and preserves the native display resolution
  when `MirrorOptions::max_size` is zero.
- Package `kill` now verifies that the original process IDs exited without putting the package in
  Android's force-stopped state; `force_stop` remains a separate operation.
- Split the project documentation into synchronized English and Simplified Chinese READMEs,
  with a concise English crates.io landing page and explicit SDK scope and requirements.
- Isolated transport state, reconnect policy, control queues, flow-control credit, and resource
  limits by device session, stream, or transfer for concurrent multi-device embedding.
- Added bounded Shell output and decompressed Sync download limits with per-operation overrides.
- Made Sync downloads commit atomically without replacing an existing destination.
- Bounded each USB device reader queue and made transport shutdown wake blocked readers.

### Fixed

- Accepted JDWP handshakes coalesced with following protocol bytes instead of rejecting the stream
  as oversized, and bounded each process-termination attempt with a timeout.
- Preserved decoder metadata across delayed FFmpeg output and flushed the final decoded frame when
  a mirror stream ends.
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
