//! Freeze project sources for acceptance without requiring version control.
use std::{
    fs,
    io::Read,
    path::{Component, Path},
    time::Duration,
};

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};

use crate::runtime_plugin::{path_str, run_bounded};

pub(super) struct SourceSnapshot {
    pub revision: String,
    pub change_summary: String,
}

pub(super) async fn capture(project: &Path, archive: &Path) -> Result<SourceSnapshot> {
    let commit = run_bounded(
        Path::new("git"),
        &[
            "-C",
            path_str(project)?,
            "rev-parse",
            "--verify",
            "HEAD^{commit}",
        ],
        Duration::from_secs(15),
    )
    .await
    .ok()
    .map(|text| text.trim().to_owned())
    .filter(|text| !text.is_empty());
    let (revision, provenance, files) = if let Some(commit) = commit {
        run_bounded(
            Path::new("git"),
            &[
                "-C",
                path_str(project)?,
                "archive",
                "--format=tar",
                "--output",
                path_str(archive)?,
                &commit,
            ],
            Duration::from_secs(120),
        )
        .await?;
        let status = run_bounded(
            Path::new("git"),
            &["-C", path_str(project)?, "status", "--short"],
            Duration::from_secs(15),
        )
        .await
        .unwrap_or_default();
        let diff = run_bounded(
            Path::new("git"),
            &["-C", path_str(project)?, "diff", "--stat", &commit],
            Duration::from_secs(30),
        )
        .await
        .unwrap_or_default();
        let files = run_bounded(
            Path::new("git"),
            &[
                "-C",
                path_str(project)?,
                "ls-tree",
                "-r",
                "--name-only",
                &commit,
            ],
            Duration::from_secs(30),
        )
        .await
        .unwrap_or_default();
        (
            commit,
            format!(
                "源码来源：Git 提交快照\n工作区状态：{}\n变更统计：{}",
                status.trim(),
                diff.trim()
            ),
            files,
        )
    } else {
        let project = project.to_owned();
        let archive = archive.to_owned();
        let (revision, files) =
            tokio::task::spawn_blocking(move || capture_directory(&project, &archive)).await??;
        (
            revision,
            "源码来源：项目目录快照（无 Git 提交）".into(),
            files,
        )
    };
    let archive = archive.to_owned();
    let manifests = tokio::task::spawn_blocking(move || snapshot_manifests(&archive)).await??;
    Ok(SourceSnapshot {
        revision,
        change_summary: format!(
            "{provenance}\n项目文件：{}\n项目配置：{manifests}",
            files.trim()
        ),
    })
}

fn snapshot_manifests(archive: &Path) -> Result<String> {
    let mut manifests = Vec::new();
    for entry in tar::Archive::new(fs::File::open(archive)?).entries_with_seek()? {
        let entry = entry?;
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let path = entry.path()?.into_owned();
        if [
            "package.json",
            "README.md",
            "pyproject.toml",
            "Cargo.toml",
            "pubspec.yaml",
            "pubspec.lock",
            ".fvmrc",
            "vite.config.ts",
            "vite.config.js",
        ]
        .iter()
        .any(|name| path == Path::new(name))
        {
            // Use frozen regular files and avoid following host-side manifest links.
            let mut bytes = Vec::new();
            entry.take(48_000).read_to_end(&mut bytes)?;
            manifests.push(format!(
                "--- {} ---\n{}",
                path.display(),
                String::from_utf8_lossy(&bytes)
                    .chars()
                    .take(12_000)
                    .collect::<String>()
            ));
        }
    }
    Ok(manifests.join("\n"))
}

fn capture_directory(project: &Path, archive: &Path) -> Result<(String, String)> {
    let mut builder =
        tar::Builder::new(fs::File::create(archive).context("create acceptance source snapshot")?);
    builder.mode(tar::HeaderMode::Deterministic);
    builder.follow_symlinks(false);
    let mut files = String::new();
    append_directory(&mut builder, project, project, archive, &mut files)?;
    builder.into_inner()?.sync_all()?;
    let mut hasher = Sha256::new();
    let mut file = fs::File::open(archive)?;
    let mut buffer = [0; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok((format!("snapshot-sha256:{:x}", hasher.finalize()), files))
}

fn append_directory(
    builder: &mut tar::Builder<fs::File>,
    root: &Path,
    directory: &Path,
    archive: &Path,
    files: &mut String,
) -> Result<()> {
    let mut entries = fs::read_dir(directory)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        if path == archive
            || archive.parent().is_some_and(|output| {
                output != root && output.starts_with(root) && path.starts_with(output)
            })
        {
            continue;
        }
        let metadata = fs::symlink_metadata(&path)?;
        if excluded(&entry.file_name().to_string_lossy(), metadata.is_dir()) {
            continue;
        }
        let relative = path.strip_prefix(root)?;
        if metadata.is_symlink() {
            let target = fs::read_link(&path)?;
            if !internal_link(relative, &target) {
                continue;
            }
        } else if !metadata.is_dir() && !metadata.is_file() {
            // Sockets and named pipes are runtime state, not application sources.
            continue;
        }
        builder
            .append_path_with_name(&path, relative)
            .with_context(|| format!("snapshot project file {}", relative.display()))?;
        if metadata.is_dir() {
            append_directory(builder, root, &path, archive, files)?;
        } else if files.len() < 16_000 {
            files.push_str(&relative.to_string_lossy());
            files.push('\n');
        }
    }
    Ok(())
}

fn excluded(name: &str, directory: bool) -> bool {
    if matches!(
        name,
        ".git" | ".hg" | ".svn" | ".ssh" | ".aws" | ".gnupg" | ".npmrc" | ".pypirc" | ".netrc"
    ) {
        return true;
    }
    if name == ".env"
        || (name.starts_with(".env.")
            && ![".example", ".sample", ".template"]
                .iter()
                .any(|suffix| name.ends_with(suffix)))
    {
        return true;
    }
    directory
        && matches!(
            name,
            "node_modules"
                | ".dart_tool"
                | ".pub-cache"
                | ".gradle"
                | ".venv"
                | "venv"
                | "__pycache__"
                | ".pytest_cache"
                | ".mypy_cache"
                | ".ruff_cache"
                | ".cache"
                | "target"
        )
}

fn internal_link(relative: &Path, target: &Path) -> bool {
    let mut depth = relative
        .parent()
        .map(|parent| parent.components().count())
        .unwrap_or(0);
    for component in target.components() {
        match component {
            Component::Normal(_) => depth += 1,
            Component::CurDir => {}
            Component::ParentDir => match depth.checked_sub(1) {
                Some(next) => depth = next,
                None => return false,
            },
            Component::RootDir | Component::Prefix(_) => return false,
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashMap, path::PathBuf};

    fn fixture() -> (PathBuf, PathBuf) {
        let root = std::env::temp_dir().join(format!("acceptance-source-{}", uuid::Uuid::new_v4()));
        let project = root.join("project");
        fs::create_dir_all(project.join("src")).unwrap();
        fs::write(project.join("src/main.txt"), "current application").unwrap();
        fs::write(project.join("package.json"), r#"{"name":"fixture"}"#).unwrap();
        (root, project)
    }

    fn archive_files(path: &Path) -> HashMap<PathBuf, Vec<u8>> {
        tar::Archive::new(fs::File::open(path).unwrap())
            .entries()
            .unwrap()
            .filter_map(|entry| {
                let mut entry = entry.unwrap();
                if !entry.header().entry_type().is_file() {
                    return None;
                }
                let path = entry.path().unwrap().into_owned();
                let mut bytes = Vec::new();
                entry.read_to_end(&mut bytes).unwrap();
                Some((path, bytes))
            })
            .collect()
    }

    #[tokio::test]
    async fn captures_plain_directories_and_repositories_without_commits() {
        for initialized in [false, true] {
            let (root, project) = fixture();
            if initialized {
                git(&project, &["init", "--quiet"]);
            }
            let archive = root.join("source.tar");
            let snapshot = capture(&project, &archive).await.unwrap();
            assert!(snapshot.revision.starts_with("snapshot-sha256:"));
            assert!(snapshot.change_summary.contains("项目目录快照"));
            assert!(snapshot.change_summary.contains("src/main.txt"));
            assert!(snapshot.change_summary.contains("package.json"));
            assert_eq!(
                archive_files(&archive)[Path::new("src/main.txt")],
                b"current application"
            );
            assert_eq!(project.join(".git").exists(), initialized);
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn directory_snapshots_are_stable_frozen_and_exclude_local_state() {
        let (root, project) = fixture();
        for (path, bytes) in [
            (".env", "local secret"),
            (".env.local", "another secret"),
            (".env.example", "API_URL="),
            (".npmrc", "auth token"),
            ("node_modules/pkg/index.js", "installed dependency"),
            (".dart_tool/package_config.json", "host paths"),
            (".git/config", "local git settings"),
            (".venv/bin/python", "host executable"),
            ("build/index.html", "application artifact"),
        ] {
            let path = project.join(path);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, bytes).unwrap();
        }
        let first = root.join("first.tar");
        let (revision, _) = capture_directory(&project, &first).unwrap();
        fs::OpenOptions::new()
            .write(true)
            .open(project.join("src/main.txt"))
            .unwrap()
            .set_modified(std::time::SystemTime::now() + Duration::from_secs(60))
            .unwrap();
        fs::write(
            project.join("node_modules/pkg/index.js"),
            "changed installed dependency",
        )
        .unwrap();
        let second = root.join("second.tar");
        assert_eq!(capture_directory(&project, &second).unwrap().0, revision);
        let files = archive_files(&first);
        assert!(files.contains_key(Path::new(".env.example")));
        assert!(files.contains_key(Path::new("build/index.html")));
        assert_eq!(files.len(), 4);
        fs::write(project.join("src/main.txt"), "next application version").unwrap();
        assert_ne!(capture_directory(&project, &second).unwrap().0, revision);
        assert_eq!(
            archive_files(&first)[Path::new("src/main.txt")],
            b"current application"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn snapshot_output_inside_project_is_not_archived() {
        let (root, project) = fixture();
        let output = project.join("run");
        fs::create_dir_all(&output).unwrap();
        fs::write(output.join("context.json"), "runtime metadata").unwrap();
        let archive = output.join("source.tar");
        capture_directory(&project, &archive).unwrap();
        assert_eq!(archive_files(&archive).len(), 2);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn directory_snapshots_preserve_executables_and_internal_links() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let (root, project) = fixture();
        fs::write(root.join("outside-secret"), "not part of the project").unwrap();
        symlink(root.join("outside-secret"), project.join("README.md")).unwrap();
        fs::write(project.join("start.sh"), "#!/bin/sh\necho ready\n").unwrap();
        fs::set_permissions(project.join("start.sh"), fs::Permissions::from_mode(0o755)).unwrap();
        symlink("../start.sh", project.join("src/start")).unwrap();
        symlink("../outside-secret", project.join("external")).unwrap();
        symlink(root.join("outside-secret"), project.join("absolute")).unwrap();
        let archive = root.join("source.tar");
        capture_directory(&project, &archive).unwrap();
        assert!(
            !snapshot_manifests(&archive)
                .unwrap()
                .contains("not part of the project")
        );
        let extracted = root.join("extracted");
        tar::Archive::new(fs::File::open(&archive).unwrap())
            .unpack(&extracted)
            .unwrap();
        assert!(
            fs::symlink_metadata(extracted.join("src/start"))
                .unwrap()
                .is_symlink()
        );
        assert_eq!(
            fs::read_to_string(extracted.join("src/start")).unwrap(),
            "#!/bin/sh\necho ready\n"
        );
        assert_ne!(
            fs::metadata(extracted.join("start.sh"))
                .unwrap()
                .permissions()
                .mode()
                & 0o111,
            0
        );
        assert!(!extracted.join("external").exists());
        assert!(!extracted.join("absolute").exists());
        fs::remove_dir_all(root).unwrap();
    }

    fn git(project: &Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(project)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    #[tokio::test]
    async fn git_projects_keep_the_recorded_commit_snapshot() {
        let (root, project) = fixture();
        git(&project, &["init", "--quiet"]);
        git(&project, &["add", "."]);
        git(
            &project,
            &[
                "-c",
                "user.name=Acceptance Test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "fixture",
            ],
        );
        let commit = git(&project, &["rev-parse", "HEAD"]);
        fs::write(
            project.join("package.json"),
            r#"{"name":"uncommitted-config"}"#,
        )
        .unwrap();
        fs::write(project.join("src/main.txt"), "uncommitted edit").unwrap();
        fs::write(project.join("untracked.txt"), "untracked edit").unwrap();
        let archive = root.join("source.tar");
        let snapshot = capture(&project, &archive).await.unwrap();
        assert_eq!(snapshot.revision, commit);
        assert!(snapshot.change_summary.contains(r#"{"name":"fixture"}"#));
        assert!(!snapshot.change_summary.contains("uncommitted-config"));
        let files = archive_files(&archive);
        assert_eq!(files[Path::new("src/main.txt")], b"current application");
        assert!(!files.contains_key(Path::new("untracked.txt")));
        fs::remove_dir_all(root).unwrap();
    }
}
