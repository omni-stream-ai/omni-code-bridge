{ pkgs, fenix, pi-agent-rust }:
let
  pname = "omni-code-bridge";
  version = "0.7.0";
  toolchain = fenix.packages.${pkgs.stdenv.hostPlatform.system}.latest.withComponents [
    "cargo"
    "rustc"
  ];
  rustPlatform = pkgs.makeRustPlatform {
    cargo = toolchain;
    rustc = toolchain;
  };
  projectSrc = pkgs.lib.cleanSource ../.;

  # The bridge currently consumes pi_agent_rust through ../pi_agent_rust.  Keep
  # both checkouts adjacent in the Nix build source so Cargo resolves that path
  # exactly as it does in the development checkout.
  src = pkgs.runCommand "${pname}-source" { } ''
    mkdir -p "$out/${pname}"
    cp -a ${projectSrc}/. "$out/${pname}/"
    chmod -R u+w "$out/${pname}"
    ln -s ${pi-agent-rust} "$out/pi_agent_rust"
  '';
in
rustPlatform.buildRustPackage {
  inherit pname version src;

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
