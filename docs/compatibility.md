# ADB SDK compatibility review

Review date: 2026-08-05.

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
| Transport | `droidmux-transport`, `droidmux-transport-tcp`, `droidmux-transport-usb` |
| Discovery | `droidmux-discovery` |
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
| Concurrent logical streams | Yes | Yes | Yes | Per-device sessions with per-stream flow control, queues, and close isolation |
| Shell v2 | Yes | Yes | Yes | stdout, stderr, exit, stdin, close-stdin, PTY, resize |
| Sync v1 | Yes | Yes | Yes | LIST, STAT, SEND, RECV, FAIL, and QUIT |
| Sync v2 metadata | Yes | Yes | No | STA2, LST2, LIS2 with 64-bit fields |
| Sync v2 transfer | Yes | Yes | No | SND2 and RCV2 with v1 fallback |
| Transfer cancellation/progress | Yes | Limited | No | Rust uses bounded memory and cancellation signals |
| Single APK install | Yes | Yes | Yes | Rust also supports listing, export, launch, stop, and kill |
| Split APK/session install | Yes | Yes | Yes | Shell v2 streaming, aggregate progress, cancellation, commit, and abandon-on-failure |
| TCP forwarding | Yes | Yes | Yes | Explicit local bind, bounded concurrency, lifecycle events, and clean cancellation |
| root/unroot adbd | Yes | No | Yes | Direct `root:` and `unroot:` service APIs; caller controls device policy |
| `abb` / `abb_exec` | Yes | No | No | Raw Binder Bridge stream plus bounded `abb_exec:<command>` helper |
| reconnect service | Yes | No | No | Direct `reconnect` request plus optional transport-factory auto reconnect |
| reverse forwarding | Yes | Yes | Yes | `reverse:forward`, list, kill, and kill-all service APIs |
| adb server port 5037 | No | No | Yes | Intentionally outside the direct-client core |
| USB Host transport | Optional | No | Via adb server | Direct libusb transport; exercised on Windows with a Xiaomi MI 8 Explorer Edition |
| Wireless mDNS discovery | Optional | Optional module | Via adb server | `droidmux-discovery`, modern `_adb-tls-connect._tcp.local.` and legacy `_adb._tcp.local.` |
| delayed_ack / burst mode | Yes | Yes | No | Explicit host opt-in, per-stream 32 MiB window, concurrent WRTE, signed 32-bit byte deltas, and close-safe inbound acknowledgements |
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
10. ADB burst mode is now an explicit client option and is covered by offline
    multiplexing tests plus a live Android 14 interactive shell test.
11. Optional transport-factory reconnect repeats CNXN/AUTH with exponential
    backoff and exposes a `Reconnecting` connection state; streams from the
    failed transport are closed rather than reused with stale remote IDs.
12. USB reads now use a bounded queue owned by each device transport. Endpoint
    stalls are cleared after claiming the interface, stale data from an older
    USB session is drained before the reader starts, and closing one transport
    wakes its blocked reader without affecting another device.
13. A remote `CLSE` no longer discards unread final `WRTE` payloads. Closed
    streams retain a per-stream acknowledgement tombstone until every queued
    payload, including a zero-length `WRTE` in delayed-ACK mode, has been consumed.

## Live USB coverage

The direct libusb path was tested on Windows with a Xiaomi MI 8 Explorer
Edition (`2717:ff48`) while the system adb server was stopped. The test performs
CNXN/AUTH using an existing authorized host key and runs two Shell v2 commands
concurrently over independent logical streams. One run plus a 20-run stability
loop completed successfully, including the observed `WRTE`/`CLSE` close race.

## Remaining protocol work

Priority 1:

- add live compatibility coverage across Android 8, 10, 11, 13, and 15.

- expand live USB coverage to Linux and macOS, additional USB controllers, and
  more Android device generations. Windows currently has one physical-device
  fixture in addition to the offline framing and endpoint tests.

Priority 2:

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
