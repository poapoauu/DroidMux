# DroidMux licensing decision

Decision date: 2026-07-28.

DroidMux is licensed under `MIT OR Apache-2.0`. A recipient may use the code
under either license.

## Why dual license

- MIT is short, widely understood, and convenient for desktop, mobile,
  embedded, proprietary, and open-source consumers.
- Apache-2.0 adds an explicit patent grant, patent retaliation terms, and a
  clearer contribution framework for a protocol and cryptography-heavy SDK.
- The Rust ecosystem commonly uses this combination, and DroidMux's direct
  dependencies use compatible MIT, Apache-2.0, BSD, or ISC terms.

The dual license applies to DroidMux-owned source code. Every dependency and
bundled component retains its own copyright and license.

## Third-party components

The optional `control` feature embeds `scrcpy-server` 4.1 from Genymobile,
licensed under Apache-2.0. Its unmodified license is stored at
`crates/droidmux-control/LICENSE-SCRCPY` and must remain with distributions
containing the server.

The default H.264 screen decoder builds `openh264` from source under BSD-2-Clause.
That copyright license does not grant patent rights. Cisco's patent coverage
for Cisco-distributed binary modules does not automatically cover third-party
source builds, so public or commercial binary distribution requires a separate
H.264 patent review for relevant jurisdictions.

The `decoder-ffmpeg` feature supplies H.264, H.265/HEVC, and AV1 decoding. It is opt-in and links FFmpeg libraries supplied by
the SDK consumer; DroidMux does not bundle them or execute `ffmpeg`. Consumers
must select an FFmpeg build whose enabled components and LGPL/GPL configuration
match their distribution model, retain its notices, and comply with dynamic- or
static-linking obligations. Enabling the backend does not change DroidMux-owned
source code from `MIT OR Apache-2.0`. H.265/HEVC distribution may require a
separate patent-license review in addition to FFmpeg copyright compliance.

Protocol behavior was compared with Apache-2.0 projects scrcpy, Kadb, and Dadb.
SkyADB is GPL-3.0 and was used only as a behavioral reference. No SkyADB source
is included, so DroidMux is not a derivative of that project.

## Distribution checklist

1. Keep `LICENSE-MIT` and `LICENSE-APACHE` with the repository and every
   published DroidMux crate archive.
2. Keep upstream copyright and license notices for all bundled components.
3. Include `crates/droidmux-control/LICENSE-SCRCPY` when distributing the
   `control` assets.
4. Generate a dependency license inventory from the release lockfile.
5. Review H.264 and H.265/HEVC patent obligations before distributing binaries with `control`.
6. When enabling `decoder-ffmpeg`, record the exact FFmpeg build configuration,
   libraries, notices, and linking method included in the distributed binary.

Crates.io package names use the `droidmux-*` namespace. The source directories
and workspace dependency aliases may retain their historical `adb-*` names;
this does not change the license or ownership of the packaged source.

This document records the engineering licensing decision and is not legal
advice.
