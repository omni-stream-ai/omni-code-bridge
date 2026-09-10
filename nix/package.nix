{ pkgs, fenix, pi-agent-rust }:
let
  manifest = builtins.fromTOML (builtins.readFile ../Cargo.toml);
  pname = "omni-code-bridge";
  version = manifest.package.version;
  toolchain = fenix.packages.${pkgs.stdenv.hostPlatform.system}.latest.withComponents [
    "cargo"
    "rustc"
  ];
  rustPlatform = pkgs.makeRustPlatform {
    cargo = toolchain;
    rustc = toolchain;
  };
  projectSrc = pkgs.lib.cleanSource ../.;

  piAgentSdkPatch = pkgs.writeText "pi-agent-sdk.patch" ''
    diff --git a/src/sdk.rs b/src/sdk.rs
    --- a/src/sdk.rs
    +++ b/src/sdk.rs
    @@ -1278,6 +1278,16 @@ impl AgentSessionHandle {
             self.session.run_text(input.into(), combined).await
         }

    +    /// Send a structured prompt containing text and/or inline images.
    +    pub async fn prompt_with_content(
    +        &mut self,
    +        content: Vec<ContentBlock>,
    +        on_event: impl Fn(AgentEvent) + Send + Sync + 'static,
    +    ) -> Result<AssistantMessage> {
    +        let combined = self.make_combined_callback(on_event);
    +        self.session.run_with_content(content, combined).await
    +    }
    +
         /// Send one user prompt through the agent loop with an explicit abort signal.
         pub async fn prompt_with_abort(
             &mut self,
    @@ -1311,6 +1321,16 @@ impl AgentSessionHandle {
                 .await
         }

    +    /// Remove the incomplete assistant response left by a failed provider
    +    /// request, while preserving the prompt and completed tool cycles.
    +    ///
    +    /// Hosts should call this before [`Self::continue_turn`] when retrying a
    +    /// transient provider failure. The operation is idempotent and returns
    +    /// whether a trailing incomplete response was removed.
    +    pub async fn revert_incomplete_response(&mut self) -> Result<bool> {
    +        self.session.revert_incomplete_response().await
    +    }
    +
         /// Continue the current agent loop with an explicit abort signal.
         pub async fn continue_turn_with_abort(
             &mut self,
  '';

  patchedPiAgentRust = pkgs.applyPatches {
    name = "pi-agent-rust-source";
    src = pi-agent-rust;
    patches = [ piAgentSdkPatch ];
  };

  # The bridge currently consumes pi_agent_rust through ../pi_agent_rust.  Keep
  # both checkouts adjacent in the Nix build source so Cargo resolves that path
  # exactly as it does in the development checkout.
  src = pkgs.runCommand "${pname}-source" { } ''
    mkdir -p "$out/${pname}"
    cp -a ${projectSrc}/. "$out/${pname}/"
    chmod -R u+w "$out/${pname}"
    ln -s ${patchedPiAgentRust} "$out/pi_agent_rust"
  '';
in
rustPlatform.buildRustPackage {
  inherit pname version src;

  nativeCheckInputs = [ pkgs.python3 ];
  dontUseCargoParallelTests = true;
  checkFlags = [
    "--skip"
    "adapter::tests::codex_provider_cancel_interrupts_live_domain_turn"
  ];
  preCheck = ''
    export HOME="$TMPDIR/home"
    mkdir -p "$HOME"
  '';

  sourceRoot = "${pname}-source";
  cargoRoot = pname;
  preBuild = "cd ${pname}";
  cargoLock.lockFile = ../Cargo.lock;

  meta = with pkgs.lib; {
    description = "HTTP and SSE bridge between Omni Code and local coding agents";
    homepage = "https://github.com/omni-stream-ai/omni-code-bridge";
    license = licenses.mit;
    mainProgram = pname;
    platforms = platforms.linux;
  };
}
