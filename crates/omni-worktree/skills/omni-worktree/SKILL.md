---
name: omni-worktree
description: Manage isolated Git worktrees with the Omni CLI, reuse compatible Node, Flutter/Dart and Rust dependencies, detach changed dependencies, and clean up merged worktrees. Use when a task calls for an isolated checkout or worktree lifecycle management.
---

# Omni worktree

Use the installed bridge command, or the standalone CLI. Both use the same Rust library and repository registry:

~~~sh
omni-code-bridge worktree --repo /path/to/repo …
omni-worktree --repo /path/to/repo …
# From the bridge source checkout, without installation:
cargo run --manifest-path crates/omni-worktree/Cargo.toml -- --repo /path/to/repo …
~~~

Output is JSON (except help and skill). Select a managed worktree by its branch, full ID, or absolute path. Use the returned path as the working directory for editing and testing; do not guess it from the branch name.

## Working in an isolated checkout

Inspect list before creating a checkout. Create with create feature/name --base main; the base is a local branch and defaults to the current branch. --existing checks out an existing branch. --path selects a destination outside the source checkout; --no-share disables dependency aliases. --keep disables automatic removal for that worktree.

Run watch from a durable source checkout while working when automatic dependency detection or external-merge cleanup is wanted. Keep track of the foreground process handle; stop it when finished. Without a watcher, use sync after dependency edits and before testing; sync --cleanup also checks for merged branches. Polling runs every two seconds by default; it is not an atomic guard around arbitrary external shell commands.

Sharing compares the contents of dependency manifests, lockfiles, package-manager configuration, and patches in both checkouts. Node and vendor packages use symlinks; Node workspace links point into the new checkout. Flutter/Dart gets a rewritten package configuration reusing Pub cache packages while mapping local packages into the worktree. Rust reuses Cargo's registry/git cache and aliases repository vendor packages. Build outputs stay local.

**Before running a dependency installer or any command that writes inside shared dependency folders, run detach <worktree>.** Do this even if manifests have not changed yet, and in each dependent worktree before installing in its source checkout. Watching cannot prevent an installer from writing through a live symlink. Then run the appropriate installer in the worktree: npm ci, pnpm install, flutter pub get, or the project's equivalent. Detachment is persistent; reverting a manifest does not silently reattach potentially stale dependencies. Cargo/Pub global caches remain available because they identify packages by version/source.

Read dependencies.notes and last_error from create/list output: missing installations, unequal dependency inputs, and unsupported inputs may skip sharing. A successful checkout does not imply dependencies were installed. For Flutter plugins, run flutter pub get after detaching when platform plugin generation is needed; platform build artifacts are not borrowed from the source.

## Merging and cleanup

Use merge <worktree> when merging is within the user's task scope. The recorded target branch must be checked out and clean. A conflict leaves normal Git merge state for resolution or git merge --abort; cleanup does not run after a failed merge.

Watch and sync --cleanup recognize ancestry-based local merges into the recorded target. They do not infer squash merges or fetch remote branches. After a squash merge, inspect the result and use explicit remove when appropriate. A newly created branch with no new commits is retained. Configure <worktree> --auto-remove false disables cleanup, and true enables it again.

Remove <worktree> removes a clean managed checkout and preserves its Git branch. Known generated dependencies and Rust/Flutter build caches are removed with the checkout. Dirty, untracked, locked, in-progress-operation, and unknown ignored-file states (such as .env) block removal. Resolve the reported condition without discarding unrelated files. Prune removes stale records for checkouts already deleted externally; it does not recursively delete directories. Run cleanup from a different checkout so the calling process does not lose its current directory.
