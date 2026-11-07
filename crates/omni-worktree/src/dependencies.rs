use crate::git_output;
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path, PathBuf},
};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DependencyState {
    pub fingerprint: Option<String>,
    pub detached: bool,
    pub ecosystems: Vec<String>,
    pub artifacts: Vec<SharedArtifact>,
    pub notes: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SharedArtifact {
    Symlink { path: PathBuf, target: PathBuf },
    File { path: PathBuf, content: String },
}
impl SharedArtifact {
    fn path(&self) -> &Path {
        match self {
            Self::Symlink { path, .. } | Self::File { path, .. } => path,
        }
    }
}
fn dependency_file(path: &Path) -> bool {
    let name = path.file_name().and_then(|v| v.to_str()).unwrap_or("");
    matches!(
        name,
        "package.json"
            | "package-lock.json"
            | "npm-shrinkwrap.json"
            | "yarn.lock"
            | "pnpm-lock.yaml"
            | "pnpm-workspace.yaml"
            | "bun.lock"
            | "bun.lockb"
            | ".npmrc"
            | ".yarnrc"
            | ".yarnrc.yml"
            | ".pnp.cjs"
            | ".pnp.loader.mjs"
            | "pubspec.yaml"
            | "pubspec.lock"
            | "pubspec_overrides.yaml"
            | ".metadata"
            | ".fvmrc"
            | "Cargo.toml"
            | "Cargo.lock"
            | "rust-toolchain"
            | "rust-toolchain.toml"
            | "composer.json"
            | "composer.lock"
    ) || path.components().any(|c| c.as_os_str() == "patches")
        || (path.parent().is_some_and(|p| p.ends_with(".cargo"))
            && matches!(name, "config" | "config.toml"))
}
fn scan(root: &Path, relative: &Path, files: &mut BTreeSet<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(root.join(relative))? {
        let entry = entry?;
        let path = relative.join(entry.file_name());
        let kind = entry.file_type()?;
        if kind.is_dir() {
            if !matches!(
                entry.file_name().to_str(),
                Some(
                    ".git"
                        | "node_modules"
                        | "vendor"
                        | "target"
                        | "build"
                        | ".dart_tool"
                        | ".pub-cache"
                        | ".cargo-cache"
                        | ".venv"
                        | ".cache"
                        | ".next"
                        | ".gradle"
                        | "Pods"
                )
            ) {
                scan(root, &path, files)?;
            }
        } else if dependency_file(&path) {
            ensure!(
                !kind.is_symlink(),
                "dependency input is a symlink: {}",
                path.display()
            );
            files.insert(path);
        }
    }
    Ok(())
}
fn inputs(root: &Path) -> Result<BTreeSet<PathBuf>> {
    let mut files = BTreeSet::new();
    scan(root, Path::new(""), &mut files)?;
    Ok(files)
}
fn fingerprint(root: &Path) -> Result<String> {
    let mut hash = Sha256::new();
    for path in inputs(root)? {
        let bytes = fs::read(root.join(&path))?;
        let name = path.to_str().context("dependency path must be UTF-8")?;
        hash.update((name.len() as u64).to_le_bytes());
        hash.update(name.as_bytes());
        hash.update((bytes.len() as u64).to_le_bytes());
        hash.update(bytes);
    }
    Ok(format!("{:x}", hash.finalize()))
}
pub(crate) fn plan(source: &Path, destination: &Path) -> Result<DependencyState> {
    let source_hash = fingerprint(source)?;
    let mut state = DependencyState::default();
    if source_hash != fingerprint(destination)? {
        state
            .notes
            .push("Dependency inputs differ; install dependencies in this worktree.".into());
        return Ok(state);
    }
    state.fingerprint = Some(source_hash);
    let mut candidates = BTreeSet::new();
    for input in inputs(source)? {
        let parent = input.parent().unwrap();
        match input.file_name().and_then(|v| v.to_str()) {
            Some("package.json") => {
                state.ecosystems.push("node".into());
                candidates.insert(parent.join("node_modules"));
            }
            Some("composer.json") => {
                state.ecosystems.push("composer".into());
                candidates.insert(parent.join("vendor"));
            }
            Some("Cargo.toml") => {
                state.ecosystems.push("rust".into());
                candidates.insert(parent.join("vendor"));
            }
            Some("pubspec.yaml") => {
                state.ecosystems.push("flutter/dart".into());
                let relative = parent.join(".dart_tool/package_config.json");
                let origin = source.join(&relative);
                if origin.is_file()
                    && !destination.join(&relative).exists()
                    && ignored(destination, &relative)?
                {
                    let content = rewrite_pub_config(&origin, source, destination)?;
                    state.artifacts.push(SharedArtifact::File {
                        path: relative,
                        content,
                    });
                }
            }
            _ => {}
        }
    }
    state.ecosystems.sort();
    state.ecosystems.dedup();
    if state.ecosystems.iter().any(|e| e == "rust") {
        state.notes.push("Cargo registry/git caches are shared via CARGO_HOME; target remains isolated. Repository vendor directories are aliased when available.".into());
    }
    if state.ecosystems.iter().any(|e| e == "flutter/dart") {
        state.notes.push("Pub cache packages are reused through a rewritten package_config.json; local package roots point into this worktree. Flutter build/plugin artifacts remain isolated.".into());
    }
    for relative in candidates {
        let origin = source.join(&relative);
        if origin.is_dir() && fs::symlink_metadata(destination.join(&relative)).is_err() {
            if ignored(destination, &relative.join(""))? {
                // Split only subtrees containing workspace links. External packages still
                // alias their complete directories; local packages point at this checkout.
                for entry in fs::read_dir(&origin)? {
                    alias_tree(
                        source,
                        destination,
                        &relative.join(entry?.file_name()),
                        &origin,
                        &mut state.artifacts,
                    )?;
                }
            } else {
                state.notes.push(format!(
                    "{} is not Git-ignored; sharing skipped",
                    relative.display()
                ));
            }
        }
    }
    Ok(state)
}
fn ignored(root: &Path, relative: &Path) -> Result<bool> {
    let mut name = relative
        .to_str()
        .context("invalid dependency path")?
        .to_string();
    if matches!(
        relative.file_name().and_then(|n| n.to_str()),
        Some("node_modules" | "vendor")
    ) {
        name.push('/');
    }
    let output = git_output(root, &["check-ignore", "-q", "--", &name])?;
    match output.status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => anyhow::bail!("git check-ignore failed"),
    }
}
fn local_link(path: &Path, source: &Path, dependency_root: &Path) -> Result<bool> {
    let kind = fs::symlink_metadata(path)?;
    if kind.is_symlink() {
        let target = fs::canonicalize(path)
            .with_context(|| format!("broken dependency link: {}", path.display()))?;
        return Ok(target.starts_with(source) && !target.starts_with(dependency_root));
    }
    if kind.is_dir() {
        for entry in fs::read_dir(path)? {
            if local_link(&entry?.path(), source, dependency_root)? {
                return Ok(true);
            }
        }
    }
    Ok(false)
}
fn alias_tree(
    source: &Path,
    destination: &Path,
    relative: &Path,
    dependency_root: &Path,
    artifacts: &mut Vec<SharedArtifact>,
) -> Result<()> {
    let origin = source.join(relative);
    let metadata = fs::symlink_metadata(&origin)?;
    if metadata.is_symlink() {
        let resolved = origin.canonicalize()?;
        let target = if resolved.starts_with(source) && !resolved.starts_with(dependency_root) {
            destination.join(resolved.strip_prefix(source)?)
        } else {
            resolved
        };
        artifacts.push(SharedArtifact::Symlink {
            path: relative.into(),
            target,
        });
    } else if metadata.is_dir() && local_link(&origin, source, dependency_root)? {
        for entry in fs::read_dir(origin)? {
            alias_tree(
                source,
                destination,
                &relative.join(entry?.file_name()),
                dependency_root,
                artifacts,
            )?;
        }
    } else {
        artifacts.push(SharedArtifact::Symlink {
            path: relative.into(),
            target: origin,
        });
    }
    Ok(())
}
fn rewrite_pub_config(path: &Path, source: &Path, destination: &Path) -> Result<String> {
    let mut config: serde_json::Value = serde_json::from_slice(&fs::read(path)?)?;
    ensure!(
        config["configVersion"] == 2,
        "unsupported Pub package config version"
    );
    let base =
        url::Url::from_file_path(path).map_err(|_| anyhow::anyhow!("invalid Pub config path"))?;
    let packages = config["packages"]
        .as_array_mut()
        .context("invalid Pub package list")?;
    for package in packages {
        let uri = package["rootUri"].as_str().context("missing Pub rootUri")?;
        let resolved = base
            .join(uri)?
            .to_file_path()
            .map_err(|_| anyhow::anyhow!("non-file Pub rootUri"))?;
        let mapped = if let Ok(relative) = resolved.strip_prefix(source) {
            destination.join(relative)
        } else {
            resolved
        };
        package["rootUri"] = url::Url::from_directory_path(mapped)
            .map_err(|_| anyhow::anyhow!("invalid package root"))?
            .to_string()
            .into();
    }
    Ok(serde_json::to_string_pretty(&config)?)
}
// Never follow a replaced parent directory while installing or unlinking artifacts.
fn safe_path(root: &Path, relative: &Path) -> Result<PathBuf> {
    let mut path = root.to_path_buf();
    let parts: Vec<_> = relative.components().collect();
    for (i, part) in parts.iter().enumerate() {
        ensure!(matches!(part, Component::Normal(_)), "unsafe artifact path");
        path.push(part.as_os_str());
        if i + 1 < parts.len()
            && let Ok(meta) = fs::symlink_metadata(&path)
        {
            ensure!(
                meta.is_dir() && !meta.is_symlink(),
                "artifact parent changed: {}",
                path.display()
            );
        }
    }
    Ok(path)
}
pub(crate) fn install(state: &DependencyState, source: &Path, destination: &Path) -> Result<()> {
    if let Some(baseline) = &state.fingerprint {
        ensure!(
            &fingerprint(source)? == baseline && &fingerprint(destination)? == baseline,
            "dependencies changed during creation"
        );
    }
    for artifact in &state.artifacts {
        let path = safe_path(destination, artifact.path())?;
        fs::create_dir_all(path.parent().unwrap())?;
        match artifact {
            SharedArtifact::Symlink { target, .. } => symlink(target, &path)?,
            SharedArtifact::File { content, .. } => {
                use std::io::Write;
                let temp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
                let result = (|| -> Result<()> {
                    let mut file = fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&temp)?;
                    file.write_all(content.as_bytes())?;
                    file.sync_all()?;
                    // Atomically publish a complete file without replacing another writer.
                    fs::hard_link(&temp, &path)?;
                    Ok(())
                })();
                let _ = fs::remove_file(temp);
                result?;
            }
        }
    }
    Ok(())
}
#[cfg(unix)]
fn symlink(target: &Path, path: &Path) -> Result<()> {
    Ok(std::os::unix::fs::symlink(target, path)?)
}
#[cfg(windows)]
fn symlink(target: &Path, path: &Path) -> Result<()> {
    if target.is_dir() {
        std::os::windows::fs::symlink_dir(target, path)?;
    } else {
        std::os::windows::fs::symlink_file(target, path)?;
    }
    Ok(())
}
fn owned(artifact: &SharedArtifact, path: &Path) -> bool {
    match artifact {
        SharedArtifact::Symlink { target, .. } => {
            fs::read_link(path).is_ok_and(|actual| actual == *target)
        }
        SharedArtifact::File { content, .. } => {
            fs::symlink_metadata(path).is_ok_and(|m| m.is_file() && !m.is_symlink())
                && fs::read(path).is_ok_and(|actual| actual == content.as_bytes())
        }
    }
}
pub(crate) fn detach(state: &mut DependencyState, destination: &Path) -> Result<()> {
    let mut errors = Vec::new();
    for artifact in &state.artifacts {
        let path = match safe_path(destination, artifact.path()) {
            Ok(path) => path,
            Err(error) => {
                errors.push(error.to_string());
                continue;
            }
        };
        if owned(artifact, &path)
            && let Err(error) = fs::remove_file(&path)
        {
            errors.push(format!("{}: {error}", path.display()));
            continue;
        }
        // Remove only empty directories created to contain our aliases/config.
        let mut parent = path.parent();
        while let Some(dir) = parent.filter(|p| *p != destination) {
            if fs::remove_dir(dir).is_err() {
                break;
            }
            parent = dir.parent();
        }
    }
    state.detached = errors.is_empty();
    ensure!(
        errors.is_empty(),
        "could not detach all artifacts: {}",
        errors.join("; ")
    );
    Ok(())
}
pub(crate) fn reconcile(
    state: &mut DependencyState,
    source: &Path,
    destination: &Path,
) -> Result<bool> {
    if state.detached || state.fingerprint.is_none() {
        return Ok(false);
    }
    let baseline = state.fingerprint.as_ref().unwrap();
    // Missing sources, unreadable manifests, or removed aliases also invalidate sharing.
    let matches = fingerprint(source).is_ok_and(|v| &v == baseline)
        && fingerprint(destination).is_ok_and(|v| &v == baseline)
        && state
            .artifacts
            .iter()
            .all(|a| safe_path(destination, a.path()).is_ok_and(|p| owned(a, &p)));
    if !matches {
        detach(state, destination)?;
        state.notes.push("Dependency sharing detached; install dependencies in this worktree. Shared global Cargo/Pub caches are retained.".into());
        return Ok(true);
    }
    Ok(false)
}
fn generated_roots(root: &Path) -> Result<Vec<PathBuf>> {
    let mut roots = vec![];
    for input in inputs(root)? {
        let parent = input.parent().unwrap();
        let names: &[&str] = match input.file_name().and_then(|v| v.to_str()) {
            Some("package.json") => &["node_modules"],
            Some("Cargo.toml") => &["target", "vendor"],
            Some("composer.json") => &["vendor"],
            Some("pubspec.yaml") => &[
                ".dart_tool",
                "build",
                ".flutter-plugins-dependencies",
                "linux/flutter/ephemeral",
                "windows/flutter/ephemeral",
                "macos/Flutter/ephemeral",
                "ios/Flutter/ephemeral",
                "ios/Pods",
                "macos/Pods",
                "android/.gradle",
            ],
            _ => &[],
        };
        roots.extend(names.iter().map(|name| parent.join(name)));
    }
    Ok(roots)
}
pub(crate) fn ensure_no_unowned_ignored(state: &DependencyState, root: &Path) -> Result<()> {
    let generated = generated_roots(root)?;
    let output = git_output(
        root,
        &[
            "ls-files",
            "--others",
            "--ignored",
            "--exclude-standard",
            "-z",
        ],
    )?;
    ensure!(output.status.success(), "cannot inspect ignored files");
    for item in output.stdout.split(|b| *b == 0).filter(|p| !p.is_empty()) {
        let relative = Path::new(std::str::from_utf8(item)?);
        let ours = state
            .artifacts
            .iter()
            .any(|a| a.path() == relative && safe_path(root, relative).is_ok_and(|p| owned(a, &p)));
        ensure!(
            ours || generated.iter().any(|dir| relative.starts_with(dir)),
            "unowned ignored file would be deleted: {} (remove or move it explicitly)",
            relative.display()
        );
    }
    Ok(())
}
