use crate::{AndroidPackage, PackageDetails, PackageError, PackageKind};

pub(crate) fn parse_package_list(
    output: &str,
    kind: PackageKind,
) -> Result<Vec<AndroidPackage>, PackageError> {
    let mut packages = Vec::new();
    for line in output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let value = line.strip_prefix("package:").ok_or_else(|| {
            PackageError::InvalidResponse(format!("unexpected package-list line: {line}"))
        })?;
        let (apk_path, package_name) = value.rsplit_once('=').ok_or_else(|| {
            PackageError::InvalidResponse(format!("package path is missing its name: {line}"))
        })?;
        if apk_path.is_empty() || package_name.is_empty() {
            return Err(PackageError::InvalidResponse(format!(
                "package list contains an empty field: {line}"
            )));
        }
        packages.push(AndroidPackage {
            package_name: package_name.to_owned(),
            apk_path: apk_path.to_owned(),
            kind,
        });
    }
    packages.sort_by(|left, right| left.package_name.cmp(&right.package_name));
    packages.dedup_by(|left, right| left.package_name == right.package_name);
    Ok(packages)
}

pub(crate) fn parse_package_details(
    package_name: &str,
    dumpsys: &str,
    paths: Vec<String>,
) -> PackageDetails {
    PackageDetails {
        package_name: package_name.to_owned(),
        version_name: field_value(dumpsys, "versionName=").map(str::to_owned),
        version_code: field_value(dumpsys, "versionCode=")
            .and_then(|value| value.split_whitespace().next())
            .and_then(|value| value.parse().ok()),
        user_id: field_value(dumpsys, "userId=")
            .or_else(|| field_value(dumpsys, "appId="))
            .and_then(|value| value.parse().ok()),
        first_install_time: field_value(dumpsys, "firstInstallTime=").map(str::to_owned),
        last_update_time: field_value(dumpsys, "lastUpdateTime=").map(str::to_owned),
        apk_paths: paths,
    }
}

pub(crate) fn parse_apk_paths(output: &str) -> Result<Vec<String>, PackageError> {
    let mut paths = Vec::new();
    for line in output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let path = line.strip_prefix("package:").ok_or_else(|| {
            PackageError::InvalidResponse(format!("unexpected APK path line: {line}"))
        })?;
        if path.is_empty() {
            return Err(PackageError::InvalidResponse(
                "Android returned an empty APK path".to_owned(),
            ));
        }
        paths.push(path.to_owned());
    }
    Ok(paths)
}

fn field_value<'a>(output: &'a str, prefix: &str) -> Option<&'a str> {
    output
        .lines()
        .map(str::trim)
        .find_map(|line| line.strip_prefix(prefix).map(str::trim))
        .filter(|value| !value.is_empty() && *value != "null")
}

#[cfg(test)]
mod tests {
    use crate::PackageKind;

    use super::{parse_apk_paths, parse_package_details, parse_package_list};

    #[test]
    fn parses_and_sorts_package_paths() {
        let packages = parse_package_list(
            "package:/data/app/second/base.apk=com.example.second\n\
             package:/data/app/first/base.apk=com.example.first\n",
            PackageKind::User,
        )
        .expect("package list should parse");

        assert_eq!(packages.len(), 2);
        assert_eq!(packages[0].package_name, "com.example.first");
        assert_eq!(packages[0].kind, PackageKind::User);
        assert_eq!(packages[1].apk_path, "/data/app/second/base.apk");
    }

    #[test]
    fn parses_version_identity_and_timestamps() {
        let output = "Package [com.example.app]\n\
            userId=10123\n\
            versionCode=42 minSdk=23 targetSdk=34\n\
            versionName=2.4.0\n\
            firstInstallTime=2026-07-20 12:30:00\n\
            lastUpdateTime=2026-07-21 08:15:00\n";
        let details = parse_package_details(
            "com.example.app",
            output,
            vec!["/data/app/base.apk".to_owned()],
        );

        assert_eq!(details.version_name.as_deref(), Some("2.4.0"));
        assert_eq!(details.version_code, Some(42));
        assert_eq!(details.user_id, Some(10_123));
        assert_eq!(details.apk_paths.len(), 1);
    }

    #[test]
    fn split_apk_paths_are_preserved() {
        let paths = parse_apk_paths(
            "package:/data/app/example/base.apk\npackage:/data/app/example/split_config.apk\n",
        )
        .expect("paths should parse");
        assert_eq!(paths.len(), 2);
        assert!(paths[0].ends_with("base.apk"));
    }

    #[test]
    fn malformed_package_lines_are_rejected() {
        assert!(parse_package_list("com.example.app\n", PackageKind::User).is_err());
        assert!(parse_apk_paths("/data/app/base.apk\n").is_err());
    }
}
