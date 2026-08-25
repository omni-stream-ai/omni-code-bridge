# Incomplete handoff: NixOS support

## Completed

- Added `flake.nix` and `flake.lock` with x86_64-linux/aarch64-linux package outputs, a nightly Rust dev shell, and exports for NixOS and Home Manager modules.
- Added `nix/package.nix`. It puts the bridge and its path dependency `pi_agent_rust` next to each other before invoking Cargo, so the existing `../pi_agent_rust` Cargo dependency resolves reproducibly.
- Added system (`nix/nixos-module.nix`) and per-user (`nix/home-manager-module.nix`) systemd service modules. Both run `settings-validate` before `serve`, support external environment files, an explicit settings path, and injected agent packages.
- Generated and locked Nix inputs (`nixpkgs`, `fenix`, and `pi-agent-rust`).
- `nix flake show --all-systems` passed before the last minimal-toolchain adjustment.

## Blocking issue

The bridge currently depends on APIs (`prompt_with_content` and `revert_incomplete_response`) present in the local sibling checkout of `pi_agent_rust` at `95b233f…`, but that commit is not available from the public GitHub remote. The public remote revision locked in `flake.lock` (`3b87c05…`) lacks those APIs, so `nix build .#omni-code-bridge` fails while compiling the bridge.

A direct flake pin to `95b233f…` was tested and GitHub returned HTTP 404. Do not use that pin until the commit is pushed/released.

## Validation performed

- Initial package plumbing was validated through Cargo vendoring and a Nix build.
- Stable Rust fails on `fsqlite-pager`'s nightly feature; the package was switched to Fenix nightly.
- With nightly and public `pi_agent_rust`, compilation reaches the bridge and fails only on the two unavailable SDK methods above.

## Next agent starting point

1. Confirm a public `pi_agent_rust` revision containing `prompt_with_content` and `revert_incomplete_response`, then pin it in `flake.nix` and regenerate `flake.lock`.
2. Stage the current final edits to `flake.nix` and `nix/package.nix`; the index still contains older versions because the interruption occurred during edits.
3. Run `nix flake show --all-systems` and `nix build .#omni-code-bridge --no-link -L`.
4. Add NixOS/Home Manager usage documentation to both README files, including flake input/module snippets and the `agentPackages` note.
5. Validate the NixOS and Home Manager modules in minimal `nixosSystem`/Home Manager evaluations, then amend the implementation commit if necessary.

## Repository state

The unrelated modified Rust files and `_user-screenshot.png` predate this work. Do not add them to the Nix-support commit.
