//! Persistent Git worktree lifecycle. All mutations take an OS file lock in Git's
//! common directory, coordinating independent CLI and library callers.
#[cfg(feature = "cli")]
pub mod cli;
mod dependencies;
use anyhow::{Context, Result, bail, ensure};
pub use dependencies::{DependencyState, SharedArtifact};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    process::{Command, Output},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Worktree {
    pub id: String,
    pub branch: String,
    pub path: PathBuf,
    pub source: PathBuf,
    pub target_branch: String,
    pub initial_head: String,
    pub initially_unmerged: bool,
    pub auto_remove: bool,
    pub dependencies: DependencyState,
    pub last_error: Option<String>,
}
#[derive(Debug, Clone)]
pub struct CreateOptions {
    pub branch: String,
    pub base: Option<String>,
    pub path: Option<PathBuf>,
    pub existing: bool,
    pub share_dependencies: bool,
    pub auto_remove: bool,
}
impl CreateOptions {
    pub fn new(branch: impl Into<String>) -> Self {
        Self {
            branch: branch.into(),
            base: None,
            path: None,
            existing: false,
            share_dependencies: true,
            auto_remove: true,
        }
    }
}
#[derive(Debug, Clone, Serialize)]
pub struct Entry {
    pub path: PathBuf,
    pub head: String,
    pub branch: Option<String>,
    pub locked: bool,
    pub managed: Option<Worktree>,
}
#[derive(Debug, Default, Serialize)]
pub struct SyncReport {
    pub detached: Vec<String>,
    pub removed: Vec<String>,
    pub errors: Vec<String>,
}
#[derive(Debug, Serialize, Deserialize, Default)]
struct Registry {
    worktrees: Vec<Worktree>,
}
#[derive(Debug, Clone)]
pub struct Manager {
    root: PathBuf,
    common: PathBuf,
}
impl Manager {
    pub fn open(repo: impl AsRef<Path>) -> Result<Self> {
        let root =
            PathBuf::from(git(repo.as_ref(), &["rev-parse", "--show-toplevel"])?).canonicalize()?;
        let common = PathBuf::from(git(
            &root,
            &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        )?)
        .canonicalize()?;
        Ok(Self { root, common })
    }
    fn storage(&self) -> PathBuf {
        self.common.join("omni-worktree")
    }
    fn lock(&self) -> Result<File> {
        fs::create_dir_all(self.storage())?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.storage().join("lock"))?;
        file.lock()?;
        Ok(file)
    }
    fn load(&self) -> Result<Registry> {
        match fs::read(self.storage().join("registry.json")) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes)
                .context("invalid worktree registry; refusing to overwrite it")?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Registry::default()),
            Err(e) => Err(e.into()),
        }
    }
    fn save(&self, registry: &Registry) -> Result<()> {
        let temp = self.storage().join("registry.tmp");
        let mut file = File::create(&temp)?;
        use std::io::Write;
        file.write_all(&serde_json::to_vec_pretty(registry)?)?;
        file.sync_all()?;
        fs::rename(temp, self.storage().join("registry.json"))?;
        Ok(())
    }
    pub fn list(&self) -> Result<Vec<Entry>> {
        let _lock = self.lock()?;
        let registry = self.load()?;
        let mut entries = self.git_entries()?;
        for entry in &mut entries {
            entry.managed = registry
                .worktrees
                .iter()
                .find(|w| w.path == entry.path)
                .cloned();
        }
        Ok(entries)
    }
    pub fn managed(&self) -> Result<Vec<Worktree>> {
        let _lock = self.lock()?;
        Ok(self.load()?.worktrees)
    }
    pub fn create(&self, options: CreateOptions) -> Result<Worktree> {
        let _lock = self.lock()?;
        let mut registry = self.load()?;
        validate_branch(&self.root, &options.branch)?;
        let target = match options.base {
            Some(base) => base,
            None => git(&self.root, &["symbolic-ref", "--short", "HEAD"])?,
        };
        validate_branch(&self.root, &target)?;
        ensure!(
            options.branch != target,
            "worktree branch must differ from its merge target"
        );
        git(
            &self.root,
            &[
                "rev-parse",
                "--verify",
                &format!("refs/heads/{target}^{{commit}}"),
            ],
        )?;
        let id = uuid::Uuid::new_v4().to_string();
        let path = options.path.unwrap_or_else(|| {
            self.root
                .parent()
                .unwrap()
                .join(format!(
                    ".{}.worktrees",
                    self.root.file_name().unwrap().to_string_lossy()
                ))
                .join(&id)
        });
        let path = if path.is_absolute() {
            path
        } else {
            std::env::current_dir()?.join(path)
        };
        ensure!(
            !path.exists() && fs::symlink_metadata(&path).is_err(),
            "destination already exists"
        );
        fs::create_dir_all(path.parent().context("destination needs a parent")?)?;
        let path = path
            .parent()
            .unwrap()
            .canonicalize()?
            .join(path.file_name().context("invalid destination")?);
        ensure!(
            !path.starts_with(&self.root) && !path.starts_with(&self.common),
            "worktree must be outside the source checkout and Git directory"
        );
        let path_arg = path.to_str().context("worktree path must be UTF-8")?;
        if options.existing {
            git(
                &self.root,
                &["worktree", "add", "--", path_arg, &options.branch],
            )?;
        } else {
            git(
                &self.root,
                &[
                    "worktree",
                    "add",
                    "-b",
                    &options.branch,
                    "--",
                    path_arg,
                    &format!("refs/heads/{target}"),
                ],
            )?;
        }
        let initial_head = git(&path, &["rev-parse", "HEAD"])?;
        let ancestry = git_output(
            &self.root,
            &[
                "merge-base",
                "--is-ancestor",
                &initial_head,
                &format!("refs/heads/{target}"),
            ],
        )?;
        ensure!(
            matches!(ancestry.status.code(), Some(0 | 1)),
            "cannot determine initial merge state"
        );
        let initially_unmerged = ancestry.status.code() == Some(1);
        let mut worktree = Worktree {
            id,
            branch: options.branch,
            path,
            source: self.root.clone(),
            target_branch: target,
            initial_head,
            initially_unmerged,
            auto_remove: options.auto_remove,
            dependencies: DependencyState::default(),
            last_error: None,
        };
        // Persist ownership before touching dependencies, including intended artifacts.
        registry.worktrees.push(worktree.clone());
        self.save(&registry)?;
        if options.share_dependencies {
            match dependencies::plan(&worktree.source, &worktree.path) {
                Ok(state) => worktree.dependencies = state,
                Err(error) => {
                    let reason = format!("dependency sharing skipped: {error:#}");
                    worktree.dependencies.notes.push(reason.clone());
                    worktree.last_error = Some(reason);
                }
            }
            *registry.worktrees.last_mut().unwrap() = worktree.clone();
            self.save(&registry)?;
            if let Err(error) =
                dependencies::install(&worktree.dependencies, &worktree.source, &worktree.path)
            {
                let reason = format!("dependency sharing incomplete: {error:#}");
                worktree.dependencies.notes.push(reason.clone());
                worktree.last_error = Some(reason);
                dependencies::detach(&mut worktree.dependencies, &worktree.path)?;
            }
            *registry.worktrees.last_mut().unwrap() = worktree.clone();
            self.save(&registry)?;
        }
        Ok(worktree)
    }
    /// Reconcile dependencies and optionally remove merged worktrees. Embedding hosts
    /// pass busy paths, e.g. checkouts with running agent sessions.
    pub fn sync(&self, cleanup: bool, busy_paths: &HashSet<PathBuf>) -> Result<SyncReport> {
        let _lock = self.lock()?;
        let mut registry = self.load()?;
        let mut report = SyncReport::default();
        let entries = self.git_entries()?;
        let shared_sources: HashSet<_> = registry
            .worktrees
            .iter()
            .filter(|w| !w.dependencies.detached && !w.dependencies.artifacts.is_empty())
            .map(|w| w.source.clone())
            .collect();
        for worktree in &mut registry.worktrees {
            worktree.last_error = None;
            let result = (|| -> Result<()> {
                self.verify_identity(worktree, &entries)?;
                if dependencies::reconcile(
                    &mut worktree.dependencies,
                    &worktree.source,
                    &worktree.path,
                )? {
                    report.detached.push(worktree.id.clone());
                }
                if cleanup
                    && worktree.auto_remove
                    && !busy_paths.contains(&worktree.path)
                    && self.is_merged(worktree)?
                {
                    ensure!(
                        !shared_sources.contains(&worktree.path),
                        "other worktrees share this checkout; detach their dependencies first"
                    );
                    self.verify(worktree, &entries)?;
                    ensure!(
                        !busy_paths.iter().any(|p| p.starts_with(&worktree.path)),
                        "worktree has a busy descendant"
                    );
                    self.remove_inner(worktree)?;
                    report.removed.push(worktree.id.clone());
                }
                Ok(())
            })();
            if let Err(error) = result {
                let error = format!("{}: {error:#}", worktree.branch);
                worktree.last_error = Some(error.clone());
                report.errors.push(error);
            }
        }
        registry
            .worktrees
            .retain(|w| !report.removed.contains(&w.id));
        self.save(&registry)?;
        Ok(report)
    }
    pub fn detach(&self, selector: &str) -> Result<Worktree> {
        let _lock = self.lock()?;
        let mut registry = self.load()?;
        let index = select(&registry, selector)?;
        let worktree = &mut registry.worktrees[index];
        self.verify_identity(worktree, &self.git_entries()?)?;
        dependencies::detach(&mut worktree.dependencies, &worktree.path)?;
        let result = worktree.clone();
        self.save(&registry)?;
        Ok(result)
    }
    pub fn configure(&self, selector: &str, auto_remove: bool) -> Result<Worktree> {
        let _lock = self.lock()?;
        let mut registry = self.load()?;
        let index = select(&registry, selector)?;
        registry.worktrees[index].auto_remove = auto_remove;
        let result = registry.worktrees[index].clone();
        self.save(&registry)?;
        Ok(result)
    }
    pub fn remove(&self, selector: &str) -> Result<()> {
        let _lock = self.lock()?;
        let mut registry = self.load()?;
        let index = select(&registry, selector)?;
        self.verify(&registry.worktrees[index], &self.git_entries()?)?;
        ensure_source_unused(&registry, &registry.worktrees[index].path)?;
        self.remove_inner(&registry.worktrees[index])?;
        registry.worktrees.remove(index);
        self.save(&registry)
    }
    pub fn merge(&self, selector: &str) -> Result<Worktree> {
        let _lock = self.lock()?;
        let mut registry = self.load()?;
        let index = select(&registry, selector)?;
        let worktree = registry.worktrees[index].clone();
        let entries = self.git_entries()?;
        self.verify(&worktree, &entries)?;
        ensure_clean(&worktree.path)?;
        let target = entries
            .iter()
            .find(|e| e.branch.as_deref() == Some(&worktree.target_branch))
            .context("target branch must be checked out in a worktree before merging")?;
        ensure!(!target.locked, "target worktree is locked");
        ensure_clean(&target.path)?;
        git(
            &target.path,
            &[
                "merge",
                "--no-edit",
                "--no-overwrite-ignore",
                &format!("refs/heads/{}", worktree.branch),
            ],
        )?;
        if worktree.auto_remove {
            let cleanup = ensure_source_unused(&registry, &worktree.path)
                .and_then(|()| self.remove_inner(&worktree));
            if let Err(error) = cleanup {
                let message = format!("merge succeeded, but cleanup was blocked: {error:#}");
                registry.worktrees[index].last_error = Some(message.clone());
                self.save(&registry)?;
                bail!("{message}");
            }
            registry.worktrees.remove(index);
            self.save(&registry)?;
        }
        Ok(worktree)
    }
    /// Drop metadata only for paths already removed externally. Never deletes files.
    pub fn prune(&self) -> Result<Vec<String>> {
        let _lock = self.lock()?;
        let mut registry = self.load()?;
        let mut removed = vec![];
        registry.worktrees.retain(|w| {
            let missing = fs::symlink_metadata(&w.path)
                .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound);
            if missing {
                removed.push(w.id.clone());
            }
            !missing
        });
        git(&self.root, &["worktree", "prune"])?;
        self.save(&registry)?;
        Ok(removed)
    }
    fn verify(&self, w: &Worktree, entries: &[Entry]) -> Result<()> {
        self.verify_identity(w, entries)?;
        let entry = entries.iter().find(|e| e.path == w.path).unwrap();
        ensure!(!entry.locked, "worktree is locked");
        ensure!(
            entry.branch.as_deref() == Some(&w.branch),
            "worktree branch changed; refusing automatic cleanup"
        );
        Ok(())
    }
    fn verify_identity(&self, w: &Worktree, entries: &[Entry]) -> Result<()> {
        entries
            .iter()
            .find(|e| e.path == w.path)
            .context("worktree is missing; use prune after external removal")?;
        ensure!(
            w.path.canonicalize()? == w.path,
            "worktree path was replaced by a symlink"
        );
        ensure!(
            Manager::open(&w.path)?.common == self.common,
            "worktree belongs to another repository"
        );
        Ok(())
    }
    fn is_merged(&self, w: &Worktree) -> Result<bool> {
        let head = git(&w.path, &["rev-parse", "HEAD"])?;
        if head == w.initial_head && !w.initially_unmerged {
            return Ok(false);
        }
        let output = git_output(
            &self.root,
            &[
                "merge-base",
                "--is-ancestor",
                &head,
                &format!("refs/heads/{}", w.target_branch),
            ],
        )?;
        match output.status.code() {
            Some(0) => Ok(true),
            Some(1) => Ok(false),
            _ => bail!(
                "cannot determine merge state: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
        }
    }
    fn remove_inner(&self, w: &Worktree) -> Result<()> {
        if let Ok(cwd) = std::env::current_dir() {
            ensure!(
                !cwd.starts_with(&w.path),
                "cannot remove the current working directory; run from another checkout"
            );
        }
        ensure_clean(&w.path)?;
        dependencies::ensure_no_unowned_ignored(&w.dependencies, &w.path)?;
        git(
            &self.root,
            &[
                "worktree",
                "remove",
                "--",
                w.path.to_str().context("invalid path")?,
            ],
        )?;
        // Preserve the branch, including unmerged commits, on explicit removal.
        Ok(())
    }
    fn git_entries(&self) -> Result<Vec<Entry>> {
        let output = git_output(&self.root, &["worktree", "list", "--porcelain", "-z"])?;
        ensure!(output.status.success(), "git worktree list failed");
        let mut entries = Vec::new();
        let mut current: Option<Entry> = None;
        for field in output.stdout.split(|b| *b == 0) {
            let field = std::str::from_utf8(field).context("non-UTF-8 Git worktree metadata")?;
            if let Some(path) = field.strip_prefix("worktree ") {
                if let Some(entry) = current.take() {
                    entries.push(entry);
                }
                current = Some(Entry {
                    path: path.into(),
                    head: String::new(),
                    branch: None,
                    locked: false,
                    managed: None,
                });
            } else if let Some(entry) = current.as_mut() {
                if let Some(head) = field.strip_prefix("HEAD ") {
                    entry.head = head.into();
                }
                if let Some(branch) = field.strip_prefix("branch refs/heads/") {
                    entry.branch = Some(branch.into());
                }
                if field == "locked" || field.starts_with("locked ") {
                    entry.locked = true;
                }
            }
        }
        if let Some(entry) = current {
            entries.push(entry);
        }
        Ok(entries)
    }
}
fn ensure_source_unused(registry: &Registry, path: &Path) -> Result<()> {
    ensure!(
        !registry.worktrees.iter().any(|w| w.source == path
            && !w.dependencies.detached
            && !w.dependencies.artifacts.is_empty()),
        "other worktrees share this checkout; detach their dependencies first"
    );
    Ok(())
}
fn select(registry: &Registry, selector: &str) -> Result<usize> {
    registry
        .worktrees
        .iter()
        .position(|w| w.id == selector || w.branch == selector || w.path.to_str() == Some(selector))
        .context("managed worktree not found (use its ID, branch, or absolute path)")
}
fn validate_branch(root: &Path, branch: &str) -> Result<()> {
    ensure!(
        !branch.starts_with('-') && !branch.starts_with('@'),
        "invalid branch name"
    );
    git(root, &["check-ref-format", "--branch", branch])?;
    Ok(())
}
pub(crate) fn git_output(root: &Path, args: &[&str]) -> Result<Output> {
    Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_COMMON_DIR")
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .context("failed to run git")
}
pub(crate) fn git(root: &Path, args: &[&str]) -> Result<String> {
    let output = git_output(root, args)?;
    ensure!(
        output.status.success(),
        "git {}: {}",
        args.first().unwrap_or(&""),
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(String::from_utf8(output.stdout)?
        .trim_end_matches(['\r', '\n'])
        .to_string())
}
fn ensure_clean(root: &Path) -> Result<()> {
    let output = git_output(
        root,
        &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
    )?;
    ensure!(output.status.success(), "cannot inspect worktree status");
    ensure!(
        output.stdout.is_empty(),
        "worktree has uncommitted or untracked files"
    );
    let git_dir = PathBuf::from(git(root, &["rev-parse", "--absolute-git-dir"])?);
    for marker in [
        "MERGE_HEAD",
        "rebase-merge",
        "rebase-apply",
        "CHERRY_PICK_HEAD",
        "REVERT_HEAD",
        "BISECT_LOG",
    ] {
        ensure!(
            !git_dir.join(marker).exists(),
            "worktree has an in-progress Git operation"
        );
    }
    Ok(())
}
