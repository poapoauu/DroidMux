use std::collections::BTreeSet;

use adb_client::AdbClient;
use adb_shell::{ShellOptions, execute_with_options};

use crate::MirrorError;

const DISPLAY_QUERY: &str = "dumpsys display";
const MAX_DISPLAY_ID: u32 = i32::MAX as u32;

/// One Android logical display addressable by scrcpy's `display_id` option.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct AndroidDisplay {
    /// Android logical display identifier.
    pub id: u32,
}

/// Lists logical displays currently exposed by Android's display manager.
///
/// Android's primary display is returned as display `0` when an older vendor
/// build omits logical display details from `dumpsys display`.
///
/// # Errors
///
/// Returns an error when the display-manager command cannot be executed.
pub async fn list_displays(client: &AdbClient) -> Result<Vec<AndroidDisplay>, MirrorError> {
    let options = if client.supports_feature("shell_v2") {
        ShellOptions::default()
    } else {
        ShellOptions::legacy()
    };
    let output = execute_with_options(client, DISPLAY_QUERY, options).await?;
    if output.exit_code.is_some_and(|code| code != 0) {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let detail = if stderr.trim().is_empty() {
            stdout.trim()
        } else {
            stderr.trim()
        };
        return Err(MirrorError::DisplayQuery(if detail.is_empty() {
            "Android display manager returned a non-zero status".to_owned()
        } else {
            detail.to_owned()
        }));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut ids = parse_display_ids(&stdout);
    if ids.is_empty() {
        ids.push(0);
    }
    Ok(ids.into_iter().map(|id| AndroidDisplay { id }).collect())
}

fn parse_display_ids(output: &str) -> Vec<u32> {
    let mut ids = BTreeSet::new();
    for line in output.lines() {
        for marker in ["mDisplayId=", "displayId=", "displayId ", "Display Id="] {
            if let Some(id) = digits_after(line, marker) {
                ids.insert(id);
            }
        }

        let trimmed = line.trim_start();
        if let Some(rest) = trimmed.strip_prefix("Display ")
            && rest.contains(':')
            && let Some(id) = leading_display_id(rest)
        {
            ids.insert(id);
        }
    }
    ids.into_iter().collect()
}

fn digits_after(line: &str, marker: &str) -> Option<u32> {
    let rest = line.split_once(marker)?.1;
    leading_display_id(rest)
}

fn leading_display_id(value: &str) -> Option<u32> {
    let digits = value
        .trim_start()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>();
    let id = digits.parse::<u32>().ok()?;
    (id <= MAX_DISPLAY_ID).then_some(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sorts_and_deduplicates_common_dumpsys_formats() {
        let output = r"
            Logical Displays: size=3
              Display 2:
                mDisplayId=2
              Display 0:
                mDisplayId=0
              Display 1:
                DisplayInfo{displayId=1, real 1920 x 1080}
        ";

        assert_eq!(parse_display_ids(output), vec![0, 1, 2]);
    }

    #[test]
    fn ignores_negative_oversized_and_unrelated_numbers() {
        let output = r"
            mDisplayId=-1
            mDisplayId=2147483648
            DisplayDeviceInfo{1080 x 1920, modeId 7}
        ";

        assert!(parse_display_ids(output).is_empty());
    }
}
