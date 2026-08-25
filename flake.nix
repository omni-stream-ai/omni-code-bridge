{
  description = "Nix packaging for Omni Code Bridge";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    pi-agent-rust = {
      url = "github:omni-stream-ai/pi_agent_rust";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, fenix, pi-agent-rust }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
    in {
      packages = forAllSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
          package = import ./nix/package.nix {
            inherit pkgs fenix pi-agent-rust;
          };
        in {
          default = package;
          omni-code-bridge = package;
        });

      devShells = forAllSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
          toolchain = fenix.packages.${system}.latest.withComponents [
            "cargo"
            "rustc"
            "rustfmt"
            "clippy"
          ];
        in {
          default = pkgs.mkShell {
            packages = [ toolchain ];
          };
        });

      nixosModules.default = import ./nix/nixos-module.nix { inherit self; };
      homeManagerModules.default = import ./nix/home-manager-module.nix { inherit self; };
    };
}
