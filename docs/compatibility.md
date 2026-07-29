# ADB SDK compatibility review

Review date: 2026-07-28.

This review compares DroidMux's pure Rust ADB stack with:

- AOSP-compatible behavior used by `Genymobile/scrcpy`;
- `flyfishxu/Kadb` at `a3fc53404903c870ee5882e719b86d918023f861`;
- `mobile-dev-inc/dadb` at `55cb08849b9b17c1e627156a0de11d91d36034de`.

The AOSP protocol remains the source of truth. Kadb and Dadb are independent
implementations used to find missing behavior, useful API boundaries, and test
cases. No source from either project is copied into the Rust implementation.

## Architecture boundary

The reusable DroidMux SDK is exposed by `crates/droidmux`. It contains no Dioxus,
storage, device-list, or GUI dependencies. The lower-level crates remain usable
individually:

| Layer | Crates |
| --- | --- |
| Wire | `droidmux-protocol` |
| Transport | `droidmux-transport`, `droidmux-transport-tcp` |
| Identity and security | `droidmux-auth`, `droidmux-pairing` |
| Multiplexing | `droidmux-client` |
| Services | `droidmux-shell`, `droidmux-sync`, `droidmux-package`, `droidmux-logcat`, `droidmux-screenshot`, `droidmux-forward` |
| Optional screen protocol | `droidmux-control` |

The SDK talks directly to `adbd`. It is not an adb-server replacement and does
not require `adb.exe`, `fastboot.exe`, or the scrcpy desktop executable.

## Capability matrix

| Capability | DroidMux Rust | Kadb | Dadb | Notes |
| --- | --- | --- | --- | --- |
| Direct TCP connection | Yes | Yes | Yes | IPv4 and IPv6 are supported by the Rust transport |
| CNXN 1.0.1 / checksum skip | Yes | Yes | No | Dadb still advertises 1.0.0 |
| Host feature advertisement | Yes | Yes | No | Rust enables only the intersection with implemented features |
| RSA authentication | Yes | Yes | Yes | RSA-2048 signatures and Android public-key encoding |
| Multiple auth keys | Yes | Yes | No | Ordered signature attempts; the first key is the optional authorization identity |
| Android 11+ pairing | Yes | Yes | No | TLS 1.3, exporter, SPAKE2, and PeerInfo |
| STLS connection upgrade | Yes | Yes | No | Implemented by the wireless transport |
| Wireless mDNS | Yes | Optional module | No | adb, TLS connect, and TLS pairing services |
| Concurrent logical streams | Yes | Yes | Yes | Rust has per-stream flow control and close isolation |
| Shell v2 | Yes | Yes | Yes | stdout, stderr, exit, stdin, close-stdin, PTY, resize |
| Sync v1 | Yes | Yes | Yes | LIST, STAT, SEND, RECV, FAIL, and QUIT |
| Sync v2 metadata | Yes | Yes | No | STA2, LST2, LIS2 with 64-bit fields |
| Sync v2 transfer | Yes | Yes | No | SND2 and RCV2 with v1 fallback |
| Transfer cancellation/progress | Yes | Limited | No | Rust uses bounded memory and cancellation signals |
| Single APK install | Yes | Yes | Yes | Rust also supports listing, export, launch, stop, and kill |
| Split APK/session install | Yes | Yes | Yes | Shell v2 streaming, aggregate progress, cancellation, commit, and abandon-on-failure |
| TCP forwarding | Yes | Yes | Yes | Explicit local bind, bounded concurrency, lifecycle events, and clean cancellation |
| root/unroot adbd | No | No | Yes | Optional future service API |
| adb server port 5037 | No | No | Yes | Intentionally outside the direct-client core |
| USB discovery/brokering | No | No | Via adb server | Outside current scope |
| delayed_ack / burst mode | No | Yes | No | Must not be advertised until flow control is implemented |
| scrcpy video/control sockets | Yes | Consumer only | Consumer only | Kept behind the SDK's `control` feature |

## Findings fixed in this review

1. The host CNXN banner did not advertise implemented features. It now follows
   the modern `host::features=...` form and stores only the host/device
   intersection.
2. `supports_feature()` previously treated every device feature as negotiated,
   even if the host did not implement it.
3. Sync operations were limited to v1. Metadata and transfers now select v2
   when negotiated and automatically retain the v1 fallback.
4. Sync v1 directory completion left the remaining `sync_dent` bytes unread.
   Both v1 and v2 completion structures are now consumed in full.
5. An all-zero Sync v1 STAT response is now treated as the historical unknown
   failure instead of a valid file.
6. Sync v2 preserves sizes and timestamps beyond the 32-bit v1 limits and
   exposes inode, ownership, link-count, access-time, and change-time metadata.
7. Host-side TCP forwarding now opens one `tcp:<port>` logical stream per local
   connection with bounded concurrency, fixed buffers, open timeouts, lifecycle
   events, and per-direction byte statistics.
8. Traditional authentication can now try an ordered set of saved RSA host
   identities. Passive connections never offer a public key, while interactive
   connections offer only the first identity after every signature is rejected.
9. Split APK installation now validates and streams every APK through Android's
   package installer session API. Cancellation, write errors, and commit errors
   all attempt `install-abandon`, with both failures preserved if cleanup fails.

## Remaining protocol work

Priority 1:

- add live compatibility coverage across Android 8, 10, 11, 13, and 15.

Priority 2:

- implement `delayed_ack`/burst flow control before advertising the feature;
- add Sync v2 compression flags after benchmarking zstd, lz4, and brotli;
- add `abb`/`abb_exec`, root/unroot, reconnect, and reverse services where they
  make sense for a direct-client SDK;
- namespace internal package names and perform a semver/API review before a
  crates.io release.

## Licensing

DroidMux is dual-licensed under MIT or Apache-2.0. Kadb and Dadb
are Apache-2.0. scrcpy is Apache-2.0.
Protocol behavior and independently written implementations can be compared,
but copied code must retain its applicable notices. SkyADB is GPL-3.0 and is
used only as a behavioral reference; its application code is not included.

The current DroidMux application credential store still manages one primary
host identity. The multi-key client API is available to SDK consumers and for a
future credential-management UI without changing the wire protocol again.
