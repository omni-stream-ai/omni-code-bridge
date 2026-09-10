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
  src = pkgs.runCommand "${pname}-source" { } ''
    mkdir -p "$out/${pname}"
    cp -a ${projectSrc}/. "$out/${pname}/"
    chmod -R u+w "$out/${pname}"
    substituteInPlace "$out/${pname}/Cargo.toml" \
      --replace-fail 'git = "https://github.com/omni-stream-ai/pi_agent_rust", rev = "95b233f27ff2b8b62cf9642b90a60d11632a1560"' 'path = "../pi_agent_rust"'
    sed -i '\|^source = "git+https://github.com/omni-stream-ai/pi_agent_rust?rev=95b233f27ff2b8b62cf9642b90a60d11632a1560#95b233f27ff2b8b62cf9642b90a60d11632a1560"$|d' "$out/${pname}/Cargo.lock"
    ln -s ${pi-agent-rust} "$out/pi_agent_rust"
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
  cargoLock.lockFile = ./Cargo.lock;

  meta = with pkgs.lib; {
    description = "HTTP and SSE bridge between Omni Code and local coding agents";
    homepage = "https://github.com/omni-stream-ai/omni-code-bridge";
    license = licenses.mit;
    mainProgram = pname;
    platforms = platforms.linux;
  };
}
