use clap::Parser;
#[derive(Parser)]
#[command(
    name = "omni-worktree",
    version,
    about = "Git worktrees with dependency sharing and merge cleanup"
)]
struct Cli {
    #[command(flatten)]
    args: omni_worktree::cli::WorktreeArgs,
}
fn main() {
    if let Err(error) = omni_worktree::cli::run(Cli::parse().args) {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}
