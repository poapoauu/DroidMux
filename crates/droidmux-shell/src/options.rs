/// Default combined stdout and stderr limit for output-collecting APIs.
pub const DEFAULT_MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;

/// Options controlling an ADB shell subprocess.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShellOptions {
    /// Use Shell v2 framing for exit codes and separated output.
    pub use_v2: bool,
    /// Allocate a pseudo-terminal instead of raw subprocess pipes.
    ///
    /// A Shell v2 PTY merges stderr into stdout, matching AOSP behavior.
    pub use_pty: bool,
    /// Initial terminal row count for a Shell v2 PTY.
    pub rows: u16,
    /// Initial terminal column count for a Shell v2 PTY.
    pub columns: u16,
    /// Combined stdout and stderr limit used by output-collecting APIs.
    ///
    /// This does not affect [`crate::open_shell`] streaming reads. Set this to
    /// `None` only when the caller provides another output bound.
    pub max_output_bytes: Option<usize>,
}

impl ShellOptions {
    /// Returns defaults suitable for an interactive terminal session.
    #[must_use]
    pub const fn interactive() -> Self {
        Self {
            use_v2: true,
            use_pty: true,
            rows: 24,
            columns: 80,
            max_output_bytes: Some(DEFAULT_MAX_OUTPUT_BYTES),
        }
    }

    /// Returns defaults suitable for an older device without Shell v2.
    #[must_use]
    pub const fn legacy() -> Self {
        Self {
            use_v2: false,
            use_pty: false,
            rows: 24,
            columns: 80,
            max_output_bytes: Some(DEFAULT_MAX_OUTPUT_BYTES),
        }
    }
}

impl Default for ShellOptions {
    fn default() -> Self {
        Self {
            use_v2: true,
            use_pty: false,
            rows: 24,
            columns: 80,
            max_output_bytes: Some(DEFAULT_MAX_OUTPUT_BYTES),
        }
    }
}
