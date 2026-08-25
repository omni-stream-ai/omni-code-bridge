{ self }:
{ config, lib, pkgs, ... }:
let
  inherit (lib) mkEnableOption mkIf mkOption mkPackageOption optional types;
  cfg = config.services.omni-code-bridge;
in {
  options.services.omni-code-bridge = {
    enable = mkEnableOption "Omni Code Bridge user service";

    package = mkPackageOption self.packages.${pkgs.stdenv.hostPlatform.system}
      "omni-code-bridge" { };

    environmentFile = mkOption {
      type = types.nullOr types.str;
      default = null;
      example = "%h/.config/omni-code-bridge/env";
      description = "Optional systemd user EnvironmentFile. Keep secrets outside the Nix store.";
    };

    settingsPath = mkOption {
      type = types.nullOr types.str;
      default = null;
      example = "%h/.omni-code/settings.json";
      description = "Optional path passed as OMNI_CODE_SETTINGS_PATH.";
    };

    agentPackages = mkOption {
      type = types.listOf types.package;
      default = [ ];
      description = "Packages whose bin directories are available to the bridge's agent processes.";
    };
  };

  config = mkIf cfg.enable {
    home.packages = [ cfg.package ];

    systemd.user.services.omni-code-bridge = {
      Unit = {
        Description = "Omni Code Bridge";
        After = [ "network-online.target" ];
        Wants = [ "network-online.target" ];
      };
      Service = {
        Type = "simple";
        ExecStartPre = "${cfg.package}/bin/omni-code-bridge settings-validate";
        ExecStart = "${cfg.package}/bin/omni-code-bridge serve";
        Environment = [ "RUST_LOG=info" ] ++ optional (cfg.settingsPath != null)
          "OMNI_CODE_SETTINGS_PATH=${cfg.settingsPath}";
        EnvironmentFile = optional (cfg.environmentFile != null) cfg.environmentFile;
        Restart = "on-failure";
        RestartSec = 3;
      };
      Install.WantedBy = [ "default.target" ];
      path = cfg.agentPackages;
    };
  };
}
