# omni-worktree

A reusable Rust library and CLI for Git worktree lifecycle and dependency sharing.
The bridge exposes the identical interface as omni-code-bridge worktree.

~~~sh
cargo install --path crates/omni-worktree
omni-worktree --repo /path/to/repo create feature/my-change --base main
omni-worktree --repo /path/to/repo watch
# In another terminal, work in the path returned by create.
omni-worktree --repo /path/to/repo sync
omni-worktree --repo /path/to/repo merge feature/my-change
~~~

Commands: create, list (alias status), sync, detach, merge, remove, prune,
configure, watch, and skill. Use --help on any command.
Operational results are JSON; errors also produce a nonzero exit code. Watch
prints JSON reports when something changes or a checkout needs attention.
Watch --once executes one monitoring cycle, useful for scheduled jobs.

## Dependency reuse

| Project | Reused | Isolated |
| --- | --- | --- |
| npm / Yarn / pnpm / Bun | Installed packages through symlinks; workspace package links remapped | Checkout source and build output |
| Flutter / Dart | Existing Pub cache and rewritten .dart_tool/package_config.json | Local package roots, plugin setup, build output |
| Rust | Cargo registry/git caches; repository vendor package links | target build output |
| Composer | Existing vendor package links | Checkout source |

Dependency sharing requires identical manifest, lockfile, relevant configuration,
and patch contents. Inputs in nested packages are included. Differences on either
side detach owned links and generated configuration on the next sync/watch cycle.
Directories must already be Git-ignored. Directory contents are linked so common
ignore rules such as node_modules/ continue to work. Missing dependencies are not
installed automatically. Global Cargo/Pub caches are inherently shared; they are
never deleted when dependencies change. Locally replaced artifacts are preserved.

Run detach <id-or-branch> **before package installation**, including installs in
source checkouts that other worktrees share. Shared folders are writable symlinks;
a polling watcher cannot intercept an arbitrary installer. There is deliberately
no automatic reattachment after invalidation: install independently instead.
Flutter plugins may require flutter pub get in the new checkout. Custom package
locations, build outputs, Python virtualenvs, and Yarn PnP artifacts are not aliased.

## Cleanup semantics

Merge merges into the recorded local target and removes the checkout by default.
Use create --keep or configure <id> --auto-remove false to retain it. A running
watch or sync --cleanup detects external regular/fast-forward merges by commit
ancestry. Fresh unchanged worktrees are not automatically removed. Squash/rebase
merges whose original commits are absent require an explicit reviewed remove.
No network fetch is performed. Git branches are preserved on removal.

Removal refuses uncommitted/untracked changes, unknown ignored files, locked
worktrees, and in-progress Git operations. Known generated dependency/build paths
(node_modules, vendor, Rust target, Flutter build/.dart_tool/platform caches) are
removed with the checkout. Unknown ignored files such as .env must be explicitly
moved/deleted first. Merge conflicts remain available for
normal Git resolution. The tool never force-deletes a checkout. Unmanaged Git
worktrees are visible in list but are not mutated.

Records live in the repository's common Git directory under omni-worktree/.
An OS file lock coordinates CLI and library processes; atomic writes preserve
state across restart. External Git/file operations do not participate in that
lock. Run monitoring and cleanup from a persistent checkout. Unix symlinks are
supported; Windows requires symlink permissions and has not been runtime-tested.

## Rust integration

~~~toml
[dependencies]
omni-worktree = { path = "path/to/crates/omni-worktree", default-features = false }
~~~

~~~rust
use omni_worktree::{CreateOptions, Manager};
use std::collections::HashSet;
let manager = Manager::open("/path/to/repo")?;
let tree = manager.create(CreateOptions::new("feature/example"))?;
// A host should include paths with running sessions in busy_paths.
let busy_paths = HashSet::new();
let report = manager.sync(true, &busy_paths)?;
~~~

The optional cli feature exposes cli::WorktreeArgs and cli::run for embedding
as a host subcommand. The bundled agent skill is printed by omni-worktree skill
and is discovered from this repository's .agents/skills/omni-worktree.

Run tests with cargo test --manifest-path crates/omni-worktree/Cargo.toml.

For an optional smoke test with real Flutter/Rust applications and populated
local package caches, build the CLI and run:

~~~sh
cargo build --manifest-path crates/omni-worktree/Cargo.toml
python3 crates/omni-worktree/tests/verify_apps.py
~~~

This verifies shared dependency paths, isolated build directories, Flutter
analysis without re-running Pub, and cleanup after building and merging.
