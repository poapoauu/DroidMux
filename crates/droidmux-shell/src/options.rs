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
        }
    }
}
