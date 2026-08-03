/// Command safety classifier — adapted from Agent-Forge's DESTRUCTIVE_PATTERNS
/// and SENSITIVE_FILE_PATTERNS.
///
/// Classifies shell commands and file paths as safe, destructive, or sensitive
/// using string matching. No regex dependency required.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SafetyClass {
    Safe,
    Destructive(String),
    SensitiveFile(String),
}

/// Classify a shell command for safety.
///
/// Returns `SafetyClass::Safe` if no destructive patterns match.
/// Ported from Agent-Forge's proven pattern set.
pub fn classify_command(command: &str) -> SafetyClass {
    let lower = command.to_lowercase();

    let destructive_checks: &[(&str, &str)] = &[
        ("rm -rf", "rm -rf (recursive force delete)"),
        ("rm -r ", "rm -r (recursive delete)"),
        ("--no-preserve-root", "rm with --no-preserve-root"),
        ("rmdir", "rmdir (directory deletion)"),
        ("drop table", "DROP statement (data loss)"),
        ("drop database", "DROP statement (data loss)"),
        ("drop schema", "DROP statement (data loss)"),
        ("truncate table", "TRUNCATE TABLE (data loss)"),
        ("mkfs", "mkfs (filesystem creation)"),
        ("git push --force", "git push --force (history rewrite)"),
        ("git push -f ", "git push --force (history rewrite)"),
        (
            "git reset --hard",
            "git reset --hard (uncommitted changes lost)",
        ),
        ("git clean -fd", "git clean -fd (untracked files deleted)"),
    ];

    for (pattern, desc) in destructive_checks {
        if lower.contains(pattern) {
            return SafetyClass::Destructive(desc.to_string());
        }
    }

    SafetyClass::Safe
}

/// Classify a file path for sensitivity.
///
/// Returns `SafetyClass::Safe` if no sensitive patterns match.
pub fn classify_path(path: &str) -> SafetyClass {
    let lower = path.to_lowercase();

    let sensitive_checks: &[(&str, &str)] = &[
        (".env", ".env file (may contain secrets)"),
        ("credentials", "credentials file"),
        (".ssh/", "SSH directory"),
        ("package-lock.json", "package-lock.json (lockfile)"),
        ("yarn.lock", "yarn.lock (lockfile)"),
        ("poetry.lock", "poetry.lock (lockfile)"),
        ("cargo.lock", "Cargo.lock (lockfile)"),
    ];

    for (pattern, desc) in sensitive_checks {
        if lower.contains(pattern) {
            return SafetyClass::SensitiveFile(desc.to_string());
        }
    }

    SafetyClass::Safe
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_command_passes_through() {
        assert_eq!(classify_command("ls -la"), SafetyClass::Safe);
        assert_eq!(classify_command("echo hello"), SafetyClass::Safe);
        assert_eq!(classify_command("cargo build"), SafetyClass::Safe);
        assert_eq!(classify_command("git status"), SafetyClass::Safe);
    }

    #[test]
    fn rm_rf_is_destructive() {
        assert!(matches!(
            classify_command("rm -rf /tmp/test"),
            SafetyClass::Destructive(_)
        ));
    }

    #[test]
    fn rm_r_is_destructive() {
        assert!(matches!(
            classify_command("rm -r /tmp/test"),
            SafetyClass::Destructive(_)
        ));
    }

    #[test]
    fn rmdir_is_destructive() {
        assert!(matches!(
            classify_command("rmdir /tmp/empty"),
            SafetyClass::Destructive(_)
        ));
    }

    #[test]
    fn drop_table_is_destructive() {
        assert!(matches!(
            classify_command("DROP TABLE users"),
            SafetyClass::Destructive(_)
        ));
    }

    #[test]
    fn truncate_is_destructive() {
        assert!(matches!(
            classify_command("TRUNCATE TABLE logs"),
            SafetyClass::Destructive(_)
        ));
    }

    #[test]
    fn git_force_push_is_destructive() {
        assert!(matches!(
            classify_command("git push --force origin main"),
            SafetyClass::Destructive(_)
        ));
    }

    #[test]
    fn git_reset_hard_is_destructive() {
        assert!(matches!(
            classify_command("git reset --hard HEAD~1"),
            SafetyClass::Destructive(_)
        ));
    }

    #[test]
    fn git_clean_fd_is_destructive() {
        assert!(matches!(
            classify_command("git clean -fd"),
            SafetyClass::Destructive(_)
        ));
    }

    #[test]
    fn mkfs_is_destructive() {
        assert!(matches!(
            classify_command("mkfs.ext4 /dev/sda1"),
            SafetyClass::Destructive(_)
        ));
    }

    #[test]
    fn env_file_is_sensitive() {
        assert!(matches!(
            classify_path(".env"),
            SafetyClass::SensitiveFile(_)
        ));
        assert!(matches!(
            classify_path("config/.env.production"),
            SafetyClass::SensitiveFile(_)
        ));
    }

    #[test]
    fn credentials_file_is_sensitive() {
        assert!(matches!(
            classify_path("credentials.json"),
            SafetyClass::SensitiveFile(_)
        ));
    }

    #[test]
    fn ssh_dir_is_sensitive() {
        assert!(matches!(
            classify_path("/home/user/.ssh/id_rsa"),
            SafetyClass::SensitiveFile(_)
        ));
    }

    #[test]
    fn lockfiles_are_sensitive() {
        assert!(matches!(
            classify_path("package-lock.json"),
            SafetyClass::SensitiveFile(_)
        ));
        assert!(matches!(
            classify_path("yarn.lock"),
            SafetyClass::SensitiveFile(_)
        ));
        assert!(matches!(
            classify_path("poetry.lock"),
            SafetyClass::SensitiveFile(_)
        ));
        assert!(matches!(
            classify_path("Cargo.lock"),
            SafetyClass::SensitiveFile(_)
        ));
    }

    #[test]
    fn normal_paths_are_safe() {
        assert_eq!(classify_path("src/main.rs"), SafetyClass::Safe);
        assert_eq!(classify_path("README.md"), SafetyClass::Safe);
        assert_eq!(classify_path("tests/unit.rs"), SafetyClass::Safe);
    }

    #[test]
    fn classification_is_case_insensitive() {
        assert!(matches!(
            classify_command("RM -RF /tmp"),
            SafetyClass::Destructive(_)
        ));
        assert!(matches!(
            classify_command("Drop Table users"),
            SafetyClass::Destructive(_)
        ));
    }
}
