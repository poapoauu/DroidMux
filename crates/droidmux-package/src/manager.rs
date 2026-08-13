use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use adb_client::AdbClient;
use adb_shell::{ShellOptions, ShellSession, open_shell};
use adb_sync::{
    SyncError, TransferCompression, TransferOptions, pull_file_with_options, push_file_with_options,
};
use tokio::io::AsyncReadExt;
use tokio::time::{Duration, timeout};

use crate::{
    AndroidPackage, InstallOptions, PackageDetails, PackageError, PackageKind, PackageListScope,
    PackageTransferCancellation, PackageTransferProgress,
    jdwp::kill_debuggable_process,
    parse::{parse_apk_paths, parse_package_details, parse_package_list},
};

const PACKAGE_NAME_MAX: usize = 255;
const COMMAND_OUTPUT_MAX: usize = 16 * 1024 * 1024;
const PROCESS_LIST_COMMAND: &str = "ps -A -o PID,NAME";
const LEGACY_PROCESS_LIST_COMMAND: &str = "ps";
const MATCHED_PROCESS_MAX: usize = 64;
const PROCESS_KILL_TIMEOUT: Duration = Duration::from_secs(2);
const PROCESS_EXIT_WAIT: Duration = Duration::from_millis(150);
const SESSION_APK_MAX: usize = 1024;
const INSTALL_BUFFER_SIZE: usize = 64 * 1024;
static INSTALL_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static LEGACY_COMMAND_SEQUENCE: AtomicU64 = AtomicU64::new(1);

struct SessionApk {
    path: PathBuf,
    size: u64,
    name: String,
}

/// Native package-manager operations bound to one connected ADB client.
pub struct PackageManager<'a> {
    client: &'a AdbClient,
}

impl<'a> PackageManager<'a> {
    /// Binds package operations to an authenticated ADB client.
    #[must_use]
    pub const fn new(client: &'a AdbClient) -> Self {
        Self { client }
    }

    /// Lists user-installed or system-image packages and their base APK paths.
    ///
    /// # Errors
    ///
    /// Returns an error when Shell fails, Android rejects the request, or the
    /// response is malformed or exceeds defensive limits.
    pub async fn list_packages(
        &self,
        scope: PackageListScope,
    ) -> Result<Vec<AndroidPackage>, PackageError> {
        let (command, kind) = match scope {
            PackageListScope::User => ("pm list packages -f -3", PackageKind::User),
            PackageListScope::System => ("pm list packages -f -s", PackageKind::System),
        };
        let output = self.run_checked(command).await?;
        parse_package_list(&output.stdout, kind)
    }

    /// Loads version, install time, user ID, and split APK paths for a package.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid package names or failed Android commands.
    pub async fn package_details(
        &self,
        package_name: &str,
    ) -> Result<PackageDetails, PackageError> {
        validate_package_name(package_name)?;
        let quoted = shell_quote(package_name);
        let dumpsys = self
            .run_checked(&format!("dumpsys package {quoted}"))
            .await?;
        let paths = self.apk_paths(package_name).await?;
        Ok(parse_package_details(package_name, &dumpsys.stdout, paths))
    }

    /// Returns all base and split APK paths for a package.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid names, command failures, or malformed output.
    pub async fn apk_paths(&self, package_name: &str) -> Result<Vec<String>, PackageError> {
        validate_package_name(package_name)?;
        let output = self
            .run_checked(&format!("pm path {}", shell_quote(package_name)))
            .await?;
        let paths = parse_apk_paths(&output.stdout)?;
        if paths.is_empty() {
            return Err(PackageError::ApkPathUnavailable(package_name.to_owned()));
        }
        Ok(paths)
    }

    /// Launches a package's default launcher activity.
    ///
    /// # Errors
    ///
    /// Returns an error when no launcher activity exists or Android rejects it.
    pub async fn launch(&self, package_name: &str) -> Result<(), PackageError> {
        validate_package_name(package_name)?;
        let package = shell_quote(package_name);
        let output = self
            .run_checked(&format!(
                "monkey -p {package} -c android.intent.category.LAUNCHER 1"
            ))
            .await?;
        let combined = format!("{}\n{}", output.stdout, output.stderr);
        if combined.contains("No activities found") || combined.contains("monkey aborted") {
            return Err(PackageError::CommandFailed(
                "该应用没有可启动的桌面入口".to_owned(),
            ));
        }
        Ok(())
    }

    /// Force-stops all processes belonging to a package for the current user.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid names or a rejected Android command.
    pub async fn force_stop(&self, package_name: &str) -> Result<(), PackageError> {
        validate_package_name(package_name)?;
        self.run_checked(&force_stop_command(package_name)).await?;
        Ok(())
    }

    /// Kills running processes without placing the package in a force-stopped state.
    ///
    /// Debuggable processes receive the same DDMS `EXIT` request used by
    /// Android Studio. Other processes fall back to `ActivityManager` and, on
    /// rooted or userdebug devices, a direct `SIGKILL`. Android may restart a
    /// service immediately because this operation does not force-stop it.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid names or a rejected Android command.
    pub async fn kill(&self, package_name: &str) -> Result<(), PackageError> {
        validate_package_name(package_name)?;
        let process_ids = self.package_process_ids(package_name).await?;
        if process_ids.is_empty() {
            return Err(PackageError::ProcessNotFound(package_name.to_owned()));
        }
        for process_id in &process_ids {
            let _ = timeout(
                PROCESS_KILL_TIMEOUT,
                kill_debuggable_process(self.client, *process_id),
            )
            .await;
        }
        if self
            .original_processes_exited(package_name, &process_ids)
            .await?
        {
            return Ok(());
        }

        let _ = self
            .run_checked(&activity_manager_kill_command(package_name))
            .await;
        if self
            .original_processes_exited(package_name, &process_ids)
            .await?
        {
            return Ok(());
        }

        let _ = self.run_checked(&kill_process_command(&process_ids)).await;
        if self
            .original_processes_exited(package_name, &process_ids)
            .await?
        {
            return Ok(());
        }
        Err(PackageError::ProcessKillDenied {
            package_name: package_name.to_owned(),
        })
    }

    /// Uninstalls a package after confirmation has been handled by the caller.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid names or an unsuccessful uninstall response.
    pub async fn uninstall(&self, package_name: &str) -> Result<(), PackageError> {
        validate_package_name(package_name)?;
        let output = self
            .run_checked(&format!("pm uninstall {}", shell_quote(package_name)))
            .await?;
        expect_success(&output, "卸载")
    }

    /// Clears all application data after confirmation has been handled by the caller.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid names or an unsuccessful clear response.
    pub async fn clear_data(&self, package_name: &str) -> Result<(), PackageError> {
        validate_package_name(package_name)?;
        let output = self
            .run_checked(&format!("pm clear {}", shell_quote(package_name)))
            .await?;
        expect_success(&output, "清除数据")
    }

    /// Uploads an APK, asks Android to install it, and always removes the staged file.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid files, transfer cancellation, Sync failures,
    /// or an unsuccessful package-manager response.
    pub async fn install_apk<F>(
        &self,
        local_path: &Path,
        options: InstallOptions,
        cancellation: PackageTransferCancellation,
        mut progress: F,
    ) -> Result<(), PackageError>
    where
        F: FnMut(PackageTransferProgress) + Send,
    {
        let metadata = tokio::fs::metadata(local_path)
            .await
            .map_err(|_| PackageError::InvalidApkPath(local_path.to_owned()))?;
        let is_apk = local_path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case("apk"));
        if !metadata.is_file() || !is_apk {
            return Err(PackageError::InvalidApkPath(local_path.to_owned()));
        }

        let sequence = INSTALL_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let remote_path = format!(
            "/data/local/tmp/droidmux-install-{}-{sequence}.apk",
            std::process::id()
        );
        let transfer_options = TransferOptions {
            cancellation,
            compression: TransferCompression::Auto,
            file_mode: 0o644,
            ..TransferOptions::default()
        };
        let upload = push_file_with_options(
            self.client,
            local_path,
            &remote_path,
            &transfer_options,
            |update| progress(update.into()),
        )
        .await;
        if let Err(error) = upload {
            let _ = self.remove_staged_apk(&remote_path).await;
            return Err(map_sync_error(error));
        }

        let mut flags = String::new();
        if options.replace_existing {
            flags.push_str(" -r");
        }
        if options.allow_downgrade {
            flags.push_str(" -d");
        }
        let install = self
            .run_checked(&format!("pm install{flags} {}", shell_quote(&remote_path)))
            .await
            .and_then(|output| expect_success(&output, "安装"));
        let cleanup = self.remove_staged_apk(&remote_path).await;
        install.and(cleanup)
    }

    /// Streams one base APK and its splits through an Android install session.
    ///
    /// All paths are validated before `install-create`. APK contents are sent
    /// directly over Shell v2 stdin and are never staged on the device. Once a
    /// session exists, every write, cancellation, or commit failure attempts
    /// `install-abandon` before returning.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty or oversized selection, invalid APK files,
    /// devices without Shell v2, cancellation, local reads, or rejected package
    /// installer commands. An abandon failure preserves both error causes.
    pub async fn install_split_apks<F>(
        &self,
        local_paths: &[PathBuf],
        options: InstallOptions,
        cancellation: PackageTransferCancellation,
        mut progress: F,
    ) -> Result<(), PackageError>
    where
        F: FnMut(PackageTransferProgress) + Send,
    {
        if !self.client.supports_feature("shell_v2") {
            return Err(PackageError::SplitInstallUnsupported);
        }
        let (apks, total_size) = validate_session_apks(local_paths).await?;
        if cancellation.is_canceled() {
            return Err(PackageError::Canceled);
        }

        let create = self
            .run_checked(&install_create_command(options, total_size))
            .await?;
        let session_id = parse_install_session_id(&create.stdout)?;
        let install = self
            .write_and_commit_session(session_id, &apks, total_size, &cancellation, &mut progress)
            .await;
        match install {
            Ok(()) => Ok(()),
            Err(install) => match self.abandon_install_session(session_id).await {
                Ok(()) => Err(install),
                Err(abandon) => Err(PackageError::SessionAbandonFailed {
                    session_id,
                    install: Box::new(install),
                    abandon: Box::new(abandon),
                }),
            },
        }
    }

    /// Exports a package's base APK to a new local path.
    ///
    /// # Errors
    ///
    /// Returns an error when the package has no APK path, the destination exists,
    /// transfer is canceled, or Sync fails.
    pub async fn export_base_apk<F>(
        &self,
        package_name: &str,
        local_path: &Path,
        cancellation: PackageTransferCancellation,
        mut progress: F,
    ) -> Result<(), PackageError>
    where
        F: FnMut(PackageTransferProgress) + Send,
    {
        let paths = self.apk_paths(package_name).await?;
        let remote_path = paths
            .iter()
            .find(|path| path.ends_with("/base.apk"))
            .or_else(|| paths.first())
            .ok_or_else(|| PackageError::ApkPathUnavailable(package_name.to_owned()))?;
        let transfer_options = TransferOptions {
            cancellation,
            ..TransferOptions::default()
        };
        pull_file_with_options(
            self.client,
            remote_path,
            local_path,
            &transfer_options,
            |update| progress(update.into()),
        )
        .await
        .map_err(map_sync_error)
    }

    async fn remove_staged_apk(&self, remote_path: &str) -> Result<(), PackageError> {
        self.run_checked(&format!("rm -f -- {}", shell_quote(remote_path)))
            .await?;
        Ok(())
    }

    async fn write_and_commit_session<F>(
        &self,
        session_id: u32,
        apks: &[SessionApk],
        total_size: u64,
        cancellation: &PackageTransferCancellation,
        progress: &mut F,
    ) -> Result<(), PackageError>
    where
        F: FnMut(PackageTransferProgress) + Send,
    {
        let mut transferred = 0_u64;
        progress(PackageTransferProgress {
            transferred_bytes: 0,
            total_bytes: Some(total_size),
        });
        for apk in apks {
            if cancellation.is_canceled() {
                return Err(PackageError::Canceled);
            }
            self.write_session_apk(
                session_id,
                apk,
                total_size,
                cancellation,
                &mut transferred,
                progress,
            )
            .await?;
        }
        if cancellation.is_canceled() {
            return Err(PackageError::Canceled);
        }
        let commit = self
            .run_checked(&format!("cmd package install-commit {session_id}"))
            .await?;
        expect_session_success(&commit, "install-commit")
    }

    async fn write_session_apk<F>(
        &self,
        session_id: u32,
        apk: &SessionApk,
        total_size: u64,
        cancellation: &PackageTransferCancellation,
        transferred: &mut u64,
        progress: &mut F,
    ) -> Result<(), PackageError>
    where
        F: FnMut(PackageTransferProgress) + Send,
    {
        let command = format!(
            "cmd package install-write -S {} {session_id} {} -",
            apk.size, apk.name
        );
        let session = Arc::new(open_shell(self.client, &command, ShellOptions::default()).await?);
        let upload_session = Arc::clone(&session);
        let output_session = Arc::clone(&session);
        let upload = async {
            let result = stream_apk(
                &upload_session,
                apk,
                total_size,
                cancellation,
                transferred,
                progress,
            )
            .await;
            if result.is_err() {
                let _ = upload_session.cancel().await;
            }
            result
        };
        let (upload, output) = tokio::join!(upload, collect_command_output(output_session));
        upload?;
        let output = output?;
        expect_session_success(&output, "install-write")
    }

    async fn abandon_install_session(&self, session_id: u32) -> Result<(), PackageError> {
        let output = self
            .run_checked(&format!("cmd package install-abandon {session_id}"))
            .await?;
        expect_session_success(&output, "install-abandon")
    }

    async fn run_checked(&self, command: &str) -> Result<CommandOutput, PackageError> {
        let output = run_bounded_command(self.client, command).await?;
        if output.exit_code == Some(0)
            || (output.exit_code.is_none() && output.stderr.trim().is_empty())
        {
            Ok(output)
        } else {
            Err(PackageError::CommandFailed(output.failure_detail()))
        }
    }

    async fn package_process_ids(&self, package_name: &str) -> Result<Vec<u32>, PackageError> {
        let output = match self.run_checked(PROCESS_LIST_COMMAND).await {
            Ok(output) => output,
            Err(_) => self.run_checked(LEGACY_PROCESS_LIST_COMMAND).await?,
        };
        parse_package_process_ids(&output.stdout, package_name)
    }

    async fn original_processes_exited(
        &self,
        package_name: &str,
        original_process_ids: &[u32],
    ) -> Result<bool, PackageError> {
        tokio::time::sleep(PROCESS_EXIT_WAIT).await;
        let running = self.package_process_ids(package_name).await?;
        Ok(!running
            .iter()
            .any(|process_id| original_process_ids.contains(process_id)))
    }
}

struct CommandOutput {
    stdout: String,
    stderr: String,
    exit_code: Option<u8>,
}

impl CommandOutput {
    fn failure_detail(&self) -> String {
        let detail = if self.stderr.trim().is_empty() {
            self.stdout.trim()
        } else {
            self.stderr.trim()
        };
        if detail.is_empty() {
            format!("Android 命令退出状态 {:?}", self.exit_code)
        } else {
            detail.to_owned()
        }
    }
}

async fn run_bounded_command(
    client: &AdbClient,
    command: &str,
) -> Result<CommandOutput, PackageError> {
    let options = if client.supports_feature("shell_v2") {
        ShellOptions::default()
    } else {
        ShellOptions::legacy()
    };
    if options.use_v2 {
        let session = Arc::new(open_shell(client, command, options).await?);
        collect_command_output(session).await
    } else {
        let marker = legacy_exit_marker();
        let wrapped = wrap_legacy_command(command, &marker);
        let session = Arc::new(open_shell(client, &wrapped, options).await?);
        let mut output = collect_command_output(session).await?;
        output.exit_code = Some(parse_legacy_exit_code(&mut output.stdout, &marker)?);
        Ok(output)
    }
}

fn legacy_exit_marker() -> String {
    let sequence = LEGACY_COMMAND_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("__DROIDMUX_EXIT_{sequence:016x}__")
}

fn wrap_legacy_command(command: &str, marker: &str) -> String {
    format!("{command}\n__droidmux_exit=$?\nprintf '\\n{marker}%s\\n' \"$__droidmux_exit\"")
}

fn parse_legacy_exit_code(output: &mut String, marker: &str) -> Result<u8, PackageError> {
    let marker_start = output.rfind(marker).ok_or_else(|| {
        PackageError::InvalidResponse("legacy shell response omitted the exit status".to_owned())
    })?;
    let status_start = marker_start + marker.len();
    let status_end = output[status_start..]
        .find(['\r', '\n'])
        .map_or(output.len(), |offset| status_start + offset);
    let trailing = output[status_end..].trim_matches(['\r', '\n']);
    if !trailing.is_empty() {
        return Err(PackageError::InvalidResponse(
            "legacy shell response contained data after the exit status".to_owned(),
        ));
    }
    let status = output[status_start..status_end]
        .parse::<u8>()
        .map_err(|_| {
            PackageError::InvalidResponse("legacy shell exit status is invalid".to_owned())
        })?;
    let content_end = output[..marker_start]
        .strip_suffix('\n')
        .map_or(marker_start, str::len);
    output.truncate(content_end);
    Ok(status)
}

async fn collect_command_output(session: Arc<ShellSession>) -> Result<CommandOutput, PackageError> {
    let stdout_session = Arc::clone(&session);
    let stderr_session = Arc::clone(&session);
    let (stdout, stderr, exit_code) = tokio::join!(
        collect_bounded(stdout_session, false),
        collect_bounded(stderr_session, true),
        session.wait(),
    );
    let stdout = stdout?;
    let stderr = stderr?;
    Ok(CommandOutput {
        stdout: String::from_utf8_lossy(&stdout).into_owned(),
        stderr: String::from_utf8_lossy(&stderr).into_owned(),
        exit_code: exit_code?,
    })
}

async fn stream_apk<F>(
    session: &ShellSession,
    apk: &SessionApk,
    total_size: u64,
    cancellation: &PackageTransferCancellation,
    transferred: &mut u64,
    progress: &mut F,
) -> Result<(), PackageError>
where
    F: FnMut(PackageTransferProgress) + Send,
{
    let mut file =
        tokio::fs::File::open(&apk.path)
            .await
            .map_err(|source| PackageError::LocalIo {
                path: apk.path.clone(),
                source,
            })?;
    let mut buffer = vec![0_u8; INSTALL_BUFFER_SIZE];
    let mut apk_transferred = 0_u64;
    loop {
        let read = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(PackageError::Canceled),
            result = file.read(&mut buffer) => result.map_err(|source| PackageError::LocalIo {
                path: apk.path.clone(),
                source,
            })?,
        };
        if read == 0 {
            break;
        }
        apk_transferred = apk_transferred
            .checked_add(read as u64)
            .ok_or(PackageError::InstallSizeOverflow)?;
        if apk_transferred > apk.size {
            return Err(PackageError::ApkSizeChanged {
                path: apk.path.clone(),
                expected: apk.size,
                actual: apk_transferred,
            });
        }
        tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(PackageError::Canceled),
            result = session.write_stdin(buffer[..read].to_vec()) => result?,
        }
        *transferred = transferred
            .checked_add(read as u64)
            .ok_or(PackageError::InstallSizeOverflow)?;
        progress(PackageTransferProgress {
            transferred_bytes: *transferred,
            total_bytes: Some(total_size),
        });
    }
    if apk_transferred != apk.size {
        return Err(PackageError::ApkSizeChanged {
            path: apk.path.clone(),
            expected: apk.size,
            actual: apk_transferred,
        });
    }
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(PackageError::Canceled),
        result = session.close_stdin() => result.map_err(PackageError::from),
    }
}

async fn validate_session_apks(
    local_paths: &[PathBuf],
) -> Result<(Vec<SessionApk>, u64), PackageError> {
    if local_paths.is_empty() {
        return Err(PackageError::NoApks);
    }
    if local_paths.len() > SESSION_APK_MAX {
        return Err(PackageError::TooManyApks {
            limit: SESSION_APK_MAX,
            actual: local_paths.len(),
        });
    }
    let mut apks = Vec::with_capacity(local_paths.len());
    let mut total_size = 0_u64;
    for (index, path) in local_paths.iter().enumerate() {
        let metadata = tokio::fs::metadata(path)
            .await
            .map_err(|_| PackageError::InvalidApkPath(path.clone()))?;
        let is_apk = path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case("apk"));
        if !metadata.is_file() || !is_apk || metadata.len() == 0 {
            return Err(PackageError::InvalidApkPath(path.clone()));
        }
        total_size = total_size
            .checked_add(metadata.len())
            .filter(|size| i64::try_from(*size).is_ok())
            .ok_or(PackageError::InstallSizeOverflow)?;
        apks.push(SessionApk {
            path: path.clone(),
            size: metadata.len(),
            name: format!("split-{index:04}.apk"),
        });
    }
    Ok((apks, total_size))
}

fn install_create_command(options: InstallOptions, total_size: u64) -> String {
    let mut flags = String::new();
    if options.replace_existing {
        flags.push_str(" -r");
    }
    if options.allow_downgrade {
        flags.push_str(" -d");
    }
    format!("cmd package install-create{flags} -S {total_size}")
}

fn parse_install_session_id(output: &str) -> Result<u32, PackageError> {
    let id = output.lines().find_map(|line| {
        let success = line.trim().strip_prefix("Success:")?;
        let start = success.rfind('[')? + 1;
        let end = success.get(start..)?.find(']')? + start;
        success.get(start..end)?.parse::<u32>().ok()
    });
    id.filter(|id| *id != 0).ok_or_else(|| {
        PackageError::InvalidResponse("install-create did not return a session ID".to_owned())
    })
}

fn expect_session_success(output: &CommandOutput, operation: &str) -> Result<(), PackageError> {
    let reported_success = output.stdout.lines().any(|line| {
        let line = line.trim();
        line == "Success" || line.starts_with("Success:")
    });
    if output.exit_code == Some(0) && reported_success {
        Ok(())
    } else {
        Err(PackageError::CommandFailed(format!(
            "{operation} did not report success: {}",
            output.failure_detail()
        )))
    }
}

async fn collect_bounded(
    session: Arc<ShellSession>,
    stderr: bool,
) -> Result<Vec<u8>, PackageError> {
    let mut output = Vec::new();
    loop {
        let chunk = if stderr {
            session.read_stderr().await
        } else {
            session.read_stdout().await
        };
        let Some(chunk) = chunk else {
            return Ok(output);
        };
        if output.len().saturating_add(chunk.len()) > COMMAND_OUTPUT_MAX {
            let _ = session.cancel().await;
            return Err(PackageError::OutputTooLarge {
                limit: COMMAND_OUTPUT_MAX,
            });
        }
        output.extend_from_slice(&chunk);
    }
}

fn expect_success(output: &CommandOutput, operation: &str) -> Result<(), PackageError> {
    if output.stdout.lines().any(|line| line.trim() == "Success") {
        Ok(())
    } else {
        Err(PackageError::CommandFailed(format!(
            "{operation}未返回 Success：{}",
            output.failure_detail()
        )))
    }
}

fn validate_package_name(package_name: &str) -> Result<(), PackageError> {
    let valid = !package_name.is_empty()
        && package_name.len() <= PACKAGE_NAME_MAX
        && package_name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_'));
    if valid {
        Ok(())
    } else {
        Err(PackageError::InvalidPackageName(package_name.to_owned()))
    }
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn force_stop_command(package_name: &str) -> String {
    format!("am force-stop --user current {}", shell_quote(package_name))
}

fn activity_manager_kill_command(package_name: &str) -> String {
    format!("am kill --user current {}", shell_quote(package_name))
}

fn parse_package_process_ids(output: &str, package_name: &str) -> Result<Vec<u32>, PackageError> {
    let mut lines = output.lines().filter(|line| !line.trim().is_empty());
    let header = lines
        .next()
        .ok_or_else(|| PackageError::InvalidResponse("进程列表为空".to_owned()))?;
    let columns = header.split_whitespace().collect::<Vec<_>>();
    let pid_column = columns
        .iter()
        .position(|column| column.eq_ignore_ascii_case("PID"))
        .ok_or_else(|| PackageError::InvalidResponse("进程列表缺少 PID 列".to_owned()))?;
    let name_column = columns
        .iter()
        .position(|column| {
            column.eq_ignore_ascii_case("NAME") || column.eq_ignore_ascii_case("CMDLINE")
        })
        .ok_or_else(|| PackageError::InvalidResponse("进程列表缺少 NAME 列".to_owned()))?;
    let child_prefix = format!("{package_name}:");
    let mut process_ids = Vec::new();
    for line in lines {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        let (Some(pid), Some(name)) = (fields.get(pid_column), fields.get(name_column)) else {
            continue;
        };
        if *name != package_name && !name.starts_with(&child_prefix) {
            continue;
        }
        let pid = pid
            .parse::<u32>()
            .map_err(|_| PackageError::InvalidResponse(format!("进程列表包含无效 PID：{pid}")))?;
        if pid == 0 {
            return Err(PackageError::InvalidResponse(
                "进程列表包含零 PID".to_owned(),
            ));
        }
        process_ids.push(pid);
        if process_ids.len() > MATCHED_PROCESS_MAX {
            return Err(PackageError::InvalidResponse(format!(
                "包 {package_name} 的匹配进程超过 {MATCHED_PROCESS_MAX} 个"
            )));
        }
    }
    process_ids.sort_unstable();
    process_ids.dedup();
    Ok(process_ids)
}

fn kill_process_command(process_ids: &[u32]) -> String {
    let process_ids = process_ids
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(" ");
    format!("kill -9 {process_ids}")
}

fn map_sync_error(error: SyncError) -> PackageError {
    if matches!(&error, SyncError::Canceled) {
        PackageError::Canceled
    } else {
        PackageError::Sync(error)
    }
}

#[cfg(test)]
mod tests {
    use crate::PackageError;

    use super::{
        CommandOutput, activity_manager_kill_command, expect_session_success, force_stop_command,
        install_create_command, kill_process_command, parse_install_session_id,
        parse_legacy_exit_code, parse_package_process_ids, shell_quote, validate_package_name,
        wrap_legacy_command,
    };

    #[test]
    fn package_names_are_strictly_validated() {
        assert!(validate_package_name("com.example.application_2").is_ok());
        assert!(matches!(
            validate_package_name("com.example.app;reboot"),
            Err(PackageError::InvalidPackageName(_))
        ));
        assert!(validate_package_name("android").is_ok());
    }

    #[test]
    fn shell_quote_treats_apostrophes_as_data() {
        assert_eq!(shell_quote("one'two"), "'one'\"'\"'two'");
    }

    #[test]
    fn process_commands_use_current_user_and_validated_pids() {
        assert_eq!(
            force_stop_command("com.example.app"),
            "am force-stop --user current 'com.example.app'"
        );
        assert_eq!(
            activity_manager_kill_command("com.example.app"),
            "am kill --user current 'com.example.app'"
        );
        assert_eq!(kill_process_command(&[123, 456]), "kill -9 123 456");
    }

    #[test]
    fn process_list_matches_only_the_package_and_its_child_processes() {
        let output = "PID NAME\n100 com.example.app\n101 com.example.app:remote\n102 com.example.application\n103 system_server\n";

        assert_eq!(
            parse_package_process_ids(output, "com.example.app").expect("processes should parse"),
            vec![100, 101]
        );
    }

    #[test]
    fn session_commands_use_numeric_sizes_and_validated_flags() {
        assert_eq!(
            install_create_command(
                crate::InstallOptions {
                    replace_existing: true,
                    allow_downgrade: true,
                },
                12_345,
            ),
            "cmd package install-create -r -d -S 12345"
        );
    }

    #[test]
    fn parses_only_successful_nonzero_install_session_ids() {
        assert_eq!(
            parse_install_session_id("Success: created install session [481]\n")
                .expect("the session ID should parse"),
            481
        );
        assert!(parse_install_session_id("Failure [INSTALL_FAILED] [481]").is_err());
        assert!(parse_install_session_id("Success: created install session [0]").is_err());
    }

    #[test]
    fn session_success_requires_a_zero_shell_v2_exit_code() {
        let output = CommandOutput {
            stdout: "Success\n".to_owned(),
            stderr: String::new(),
            exit_code: Some(1),
        };
        assert!(expect_session_success(&output, "install-write").is_err());
    }

    #[test]
    fn legacy_command_wrapper_recovers_success_failure_and_stdout() {
        let marker = "__DROIDMUX_EXIT_test__";
        assert_eq!(
            wrap_legacy_command("pm list packages", marker),
            "pm list packages\n__droidmux_exit=$?\nprintf '\\n__DROIDMUX_EXIT_test__%s\\n' \"$__droidmux_exit\""
        );

        let mut success = format!("package:one\npackage:two\n\n{marker}0\n");
        assert_eq!(
            parse_legacy_exit_code(&mut success, marker).expect("status should parse"),
            0
        );
        assert_eq!(success, "package:one\npackage:two\n");

        let mut failure = format!("Unknown option: -o\n\n{marker}1\n");
        assert_eq!(
            parse_legacy_exit_code(&mut failure, marker).expect("status should parse"),
            1
        );
        assert_eq!(failure, "Unknown option: -o\n");
    }

    #[test]
    fn legacy_exit_parser_rejects_missing_or_ambiguous_status() {
        let marker = "__DROIDMUX_EXIT_test__";
        let mut missing = "command output".to_owned();
        assert!(parse_legacy_exit_code(&mut missing, marker).is_err());

        let mut trailing = format!("output\n{marker}0\nunexpected");
        assert!(parse_legacy_exit_code(&mut trailing, marker).is_err());
    }
}
