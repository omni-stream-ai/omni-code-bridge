//! Shared command parser/runner for the standalone CLI and host subcommands.
use crate::{CreateOptions, Manager};
use anyhow::{Result, ensure};
use clap::{Args, Subcommand};
use std::{collections::HashSet, path::PathBuf, time::Duration};

#[derive(Debug, Args)]
pub struct WorktreeArgs {
    /// Repository or linked worktree path
    #[arg(long, short = 'C', default_value = ".", global = true)]
    pub repo: PathBuf,
    #[command(subcommand)]
    pub command: WorktreeCommand,
}
#[derive(Debug, Subcommand)]
pub enum WorktreeCommand {
    /// Print the bundled coding-agent skill (no repository needed)
    Skill,
    /// Create an isolated checkout and share unchanged dependencies
    Create {
        branch: String,
        /// Local target branch; defaults to the current branch
        #[arg(long)]
        base: Option<String>,
        #[arg(long)]
        path: Option<PathBuf>,
        /// Check out an existing branch instead of creating one
        #[arg(long)]
        existing: bool,
        #[arg(long)]
        no_share: bool,
        #[arg(long)]
        keep: bool,
    },
    /// List all Git worktrees with managed dependency state (JSON)
    #[command(alias = "status")]
    List,
    /// Detach changed dependencies; optionally clean up merged checkouts
    Sync {
        #[arg(long)]
        cleanup: bool,
    },
    /// Detach managed dependencies before running a package installer
    Detach { worktree: String },
    /// Merge into the recorded target; clean up unless --keep was set
    Merge { worktree: String },
    /// Remove a clean managed checkout, preserving its branch
    Remove { worktree: String },
    /// Remove stale metadata for externally deleted checkouts
    Prune,
    /// Change whether a managed checkout is removed after merging
    Configure {
        worktree: String,
        #[arg(long, action = clap::ArgAction::Set)]
        auto_remove: bool,
    },
    /// Monitor dependency changes and external merges until interrupted
    Watch {
        #[arg(long, default_value_t = 2)]
        interval: u64,
        #[arg(long)]
        no_cleanup: bool,
        /// Perform a single polling cycle
        #[arg(long)]
        once: bool,
    },
}
pub fn run(args: WorktreeArgs) -> Result<()> {
    if matches!(args.command, WorktreeCommand::Skill) {
        print!("{}", include_str!("../skills/omni-worktree/SKILL.md"));
        return Ok(());
    }
    let manager = Manager::open(args.repo)?;
    match args.command {
        WorktreeCommand::Skill => unreachable!(),
        WorktreeCommand::Create {
            branch,
            base,
            path,
            existing,
            no_share,
            keep,
        } => {
            output(&manager.create(CreateOptions {
                branch,
                base,
                path,
                existing,
                share_dependencies: !no_share,
                auto_remove: !keep,
            })?)?;
        }
        WorktreeCommand::List => output(&manager.list()?)?,
        WorktreeCommand::Sync { cleanup } => {
            let report = manager.sync(cleanup, &HashSet::new())?;
            output(&report)?;
            ensure!(
                report.errors.is_empty(),
                "some worktrees could not be reconciled"
            );
        }
        WorktreeCommand::Detach { worktree } => output(&manager.detach(&worktree)?)?,
        WorktreeCommand::Merge { worktree } => output(&manager.merge(&worktree)?)?,
        WorktreeCommand::Remove { worktree } => {
            manager.remove(&worktree)?;
            output(&serde_json::json!({"removed": worktree}))?;
        }
        WorktreeCommand::Prune => output(&manager.prune()?)?,
        WorktreeCommand::Configure {
            worktree,
            auto_remove,
        } => output(&manager.configure(&worktree, auto_remove)?)?,
        WorktreeCommand::Watch {
            interval,
            no_cleanup,
            once,
        } => {
            ensure!(interval > 0, "interval must be positive");
            loop {
                let report = manager.sync(!no_cleanup, &HashSet::new())?;
                if once
                    || !report.detached.is_empty()
                    || !report.removed.is_empty()
                    || !report.errors.is_empty()
                {
                    output(&report)?;
                }
                if once {
                    ensure!(
                        report.errors.is_empty(),
                        "some worktrees could not be reconciled"
                    );
                    break;
                }
                std::thread::sleep(Duration::from_secs(interval));
            }
        }
    }
    Ok(())
}
fn output(value: &impl serde::Serialize) -> Result<()> {
    use std::io::Write;
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, value)?;
    writeln!(stdout)?;
    Ok(())
}
