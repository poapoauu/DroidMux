//! Native ADB shell and Shell v2 sessions.

mod codec;
mod error;
mod options;
mod session;

pub use error::{ShellError, ShellProtocolError};
pub use options::ShellOptions;
pub use session::{ShellOutput, ShellSession, execute, execute_with_options, open_shell};
