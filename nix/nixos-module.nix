{ self }:
{ config, lib, pkgs, ... }:
let
  inherit (lib) mkEnableOption mkIf mkOption mkPackageOption optional types;
  cfg = config.services.omni-code-bridge;
in {
  options.services.omni-code-bridge = {
    enable = mkEnableOption "Omni Code Bridge";

    package = mkPackageOption self.packages.${pkgs.stdenv.hostPlatform.system}
      "omni-code-bridge" { };

    user = mkOption {
      type = types.str;
      default = "omni-code-bridge";
      description = "User account used by the bridge service.";
    };

    group = mkOption {
      type = types.str;
      default = "omni-code-bridge";
      description = "Primary group used by the bridge service.";
    };

    home = mkOption {
      type = types.str;
      default = "/var/lib/omni-code-bridge";
      description = "Home directory containing bridge state and settings.";
    };

    createUser = mkOption {
      type = types.bool;
      default = true;
      description = "Whether to create the configured system user and group.";
    };

    environmentFile = mkOption {
      type = types.nullOr types.str;
      default = null;
      example = "/run/secrets/omni-code-bridge.env";
      description = "Optional systemd EnvironmentFile. Keep secrets outside the Nix store.";
    };

    settingsPath = mkOption {
      type = types.nullOr types.str;
      default = null;
      example = "/var/lib/omni-code-bridge/.omni-code/settings.json";
      description = "Optional absolute path passed as OMNI_CODE_SETTINGS_PATH.";
    };

    agentPackages = mkOption {
      type = types.listOf types.package;
      default = [ ];
      description = "Packages whose bin directories are available to the bridge's agent processes.";
    };
  };

  config = mkIf cfg.enable {
    users.groups = mkIf cfg.createUser {
      ${cfg.group} = { };
    };

    users.users = mkIf cfg.createUser {
      ${cfg.user} = {
        isSystemUser = true;
        inherit (cfg) group home;
        createHome = true;
      };
    };

    systemd.services.omni-code-bridge = {
      description = "Omni Code Bridge";
      wantedBy = [ "multi-user.target" ];
      after = [ "network-online.target" ];
      wants = [ "network-online.target" ];
      path = cfg.agentPackages;

      serviceConfig = {
        Type = "simple";
        User = cfg.user;
        Group = cfg.group;
        WorkingDirectory = cfg.home;
        Environment = [
          "HOME=${cfg.home}"
          "RUST_LOG=info"
        ] ++ optional (cfg.settingsPath != null)
          "OMNI_CODE_SETTINGS_PATH=${cfg.settingsPath}";
        EnvironmentFile = optional (cfg.environmentFile != null) cfg.environmentFile;
        ExecStartPre = "${cfg.package}/bin/omni-code-bridge settings-validate";
        ExecStart = "${cfg.package}/bin/omni-code-bridge serve";
        Restart = "on-failure";
        RestartSec = 3;
      };
    };
  };
}
