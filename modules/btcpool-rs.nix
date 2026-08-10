{
  inputs,
  lib,
  ...
}:
let
  projectVersion = (fromTOML (builtins.readFile ../Cargo.toml)).package.version;

  nixosModule =
    {
      config,
      lib,
      pkgs,
      ...
    }:
    let
      cfg = config.services.btcpool-rs;
      tomlFormat = pkgs.formats.toml { };
      defaultSettings = {
        pool = {
          listen_addr = "0.0.0.0:3333";
          coinbase_tag = "/btcpool-rs/";
          initial_difficulty = 2048;
          extranonce1_size = 4;
          extranonce2_size = 4;
          max_connections = 256;
          idle_timeout_secs = 300;
          found_block_dir = "found-blocks";
          confirmation_depth = 6;
          strict_gbt_rules = true;
        };

        sv2 = {
          enabled = true;
          persist_authority_key = true;
          authority_key_file = "sv2-authority.key";
          cert_validity_secs = 31536000;
        };

        bitcoin_rpc = {
          url = "http://127.0.0.1:8332";
          cookie_path = "~/.bitcoin/.cookie";
          timeout_secs = 10;
        };

        zmq = {
          hashblock_endpoint = "tcp://127.0.0.1:28332";
          poll_fallback = true;
          poll_interval_ms = 1000;
        };

        vardiff = {
          target_share_time_secs = 5;
          retarget_interval_secs = 100;
          deadzone_low = 0.667;
          deadzone_high = 1.5;
          min_difficulty = 256;
          max_difficulty = 4000000;
          max_retarget_factor = 10.0;
        };

        security = {
          max_connections_per_ip = 5;
          max_shares_per_sec = 500;
          ban_duration_secs = 600;
          max_invalid_shares = 5;
          max_message_bytes = 4096;
          max_worker_name_len = 128;
          max_authorizations_per_session = 8;
        };

        metrics = {
          prometheus_addr = "0.0.0.0:9090";
          stats_db_path = "pool_stats.sqlite";
        };

        logging = {
          level = "info";
          json = false;
          # Empty = journal only. Set a directory to also keep rotating files
          # on disk; the unit would then want a matching LogsDirectory. `json`
          # above formats those files; the journal stays human-readable.
          log_dir = "";
        };
      };
      generatedConfig = tomlFormat.generate "btcpool-rs.toml" cfg.settings;

      portFromAddress =
        name: address:
        let
          matched = builtins.match ".*:([0-9]+)" address;
        in
        if matched == null then
          throw "services.btcpool-rs.settings.${name} must end in :<port>"
        else
          lib.toInt (builtins.head matched);

      firewallPorts = lib.unique (
        [ (portFromAddress "pool.listen_addr" cfg.settings.pool.listen_addr) ]
        ++ lib.optional (cfg.settings.metrics.prometheus_addr != "") (
          portFromAddress "metrics.prometheus_addr" cfg.settings.metrics.prometheus_addr
        )
      );
    in
    {
      options.services.btcpool-rs = {
        enable = lib.mkEnableOption "btcpool-rs Bitcoin mining pool";

        package = lib.mkOption {
          type = lib.types.package;
          default = inputs.self.packages.${pkgs.stdenv.hostPlatform.system}.default;
          defaultText = lib.literalExpression "inputs.btcpool-rs.packages.${pkgs.system}.default";
          description = "The btcpool-rs package to run.";
        };

        settings = lib.mkOption {
          type = tomlFormat.type;
          default = { };
          description = ''
            Pool configuration written to /etc/btcpool-rs/config.toml.
            These values are stored in the world-readable Nix store; use
            environmentFiles for secrets.
          '';
        };

        environmentFiles = lib.mkOption {
          type = lib.types.listOf lib.types.str;
          default = [ ];
          example = [ "/run/secrets/btcpool-rs.env" ];
          description = ''
            Runtime environment files containing BTCPOOL_* configuration
            overrides. Use absolute string paths produced by a secret manager,
            not Nix path literals, so their contents never enter the Nix store.
          '';
        };

        user = lib.mkOption {
          type = lib.types.str;
          default = "btcpool-rs";
          description = "User account under which the service runs.";
        };

        group = lib.mkOption {
          type = lib.types.str;
          default = "btcpool-rs";
          description = "Primary group under which the service runs.";
        };

        createUser = lib.mkOption {
          type = lib.types.bool;
          default = true;
          description = "Whether to create the configured service user and group.";
        };

        extraGroups = lib.mkOption {
          type = lib.types.listOf lib.types.str;
          default = [ ];
          example = [ "bitcoin" ];
          description = "Supplementary groups, typically used to read the Bitcoin RPC cookie.";
        };

        openFirewall = lib.mkOption {
          type = lib.types.bool;
          default = false;
          description = "Whether to open the Stratum and enabled metrics ports from settings.";
        };

        after = lib.mkOption {
          type = lib.types.listOf lib.types.str;
          default = [ ];
          example = [ "bitcoind.service" ];
          description = "Additional systemd units that the pool should start after.";
        };
      };

      config = lib.mkIf cfg.enable {
        services.btcpool-rs.settings = lib.mapAttrsRecursive (
          _: value: lib.mkDefault value
        ) defaultSettings;

        assertions = [
          {
            assertion = !(cfg.settings.bitcoin_rpc ? password);
            message = ''
              services.btcpool-rs.settings.bitcoin_rpc.password would be
              exposed in the Nix store; provide it through environmentFiles
              as BTCPOOL_BITCOIN_RPC__PASSWORD instead
            '';
          }
          {
            assertion = builtins.all (lib.hasPrefix "/") cfg.environmentFiles;
            message = "services.btcpool-rs.environmentFiles entries must be absolute paths";
          }
        ];

        environment.etc."btcpool-rs/config.toml".source = generatedConfig;

        users = lib.mkIf cfg.createUser {
          groups.${cfg.group} = { };
          users.${cfg.user} = {
            isSystemUser = true;
            group = cfg.group;
            home = "/var/lib/btcpool-rs";
          };
        };

        systemd.tmpfiles.rules = [
          "d /var/lib/btcpool-rs 0700 ${cfg.user} ${cfg.group} -"
          "Z /var/lib/btcpool-rs - ${cfg.user} ${cfg.group} -"
        ];

        networking.firewall.allowedTCPPorts = lib.mkIf cfg.openFirewall firewallPorts;

        systemd.services.btcpool-rs = {
          description = "btcpool-rs Bitcoin mining pool";
          documentation = [ "https://github.com/edifus/btcpool-rs" ];
          wantedBy = [ "multi-user.target" ];
          wants = [ "network-online.target" ];
          after = [ "network-online.target" ] ++ cfg.after;

          serviceConfig = {
            Type = "simple";
            ExecStart = "${lib.getExe cfg.package} /etc/btcpool-rs/config.toml";
            EnvironmentFile = cfg.environmentFiles;
            User = cfg.user;
            Group = cfg.group;
            SupplementaryGroups = cfg.extraGroups;
            StateDirectory = "btcpool-rs";
            WorkingDirectory = "/var/lib/btcpool-rs";
            Restart = "on-failure";
            RestartSec = 5;
            UMask = "0077";

            NoNewPrivileges = true;
            ProtectSystem = "strict";
            ProtectHome = "read-only";
            PrivateTmp = true;
            PrivateDevices = true;
            ProtectKernelTunables = true;
            ProtectKernelModules = true;
            ProtectControlGroups = true;
            RestrictAddressFamilies = [
              "AF_INET"
              "AF_INET6"
              "AF_UNIX"
            ];
            RestrictNamespaces = true;
            LockPersonality = true;
            MemoryDenyWriteExecute = true;
          };
        };
      };
    };
in
{
  flake = {
    modules.nixos.btcpool-rs = nixosModule;
    nixosModules = {
      default = nixosModule;
      btcpool-rs = nixosModule;
    };
  };

  perSystem =
    { pkgs, ... }:
    let
      package = pkgs.rustPlatform.buildRustPackage {
        pname = "btcpool-rs";
        version = projectVersion;

        src = lib.fileset.toSource {
          root = ../.;
          fileset = lib.fileset.unions [
            ../Cargo.toml
            ../Cargo.lock
            ../src
            ../tests
            ../examples
            ../LICENSE-MIT
            ../LICENSE-APACHE
          ];
        };

        cargoLock.lockFile = ../Cargo.lock;

        nativeBuildInputs = with pkgs; [
          cmake
          pkg-config
        ];

        buildInputs = with pkgs; [
          sqlite
          zeromq
        ];

        passthru = { inherit projectVersion; };

        meta = {
          description = "Solo Bitcoin mining pool with Stratum V1 and V2 support";
          homepage = "https://github.com/edifus/btcpool-rs";
          license = with lib.licenses; [
            mit
            asl20
          ];
          mainProgram = "btcpool-rs";
          platforms = lib.platforms.linux;
        };
      };

      moduleTest = inputs.nixpkgs.lib.nixosSystem {
        system = pkgs.stdenv.hostPlatform.system;
        modules = [
          nixosModule
          {
            boot.isContainer = true;
            system.stateVersion = "26.05";

            services.btcpool-rs = {
              enable = true;
              inherit package;
              openFirewall = true;
              environmentFiles = [ "/run/secrets/btcpool-rs.env" ];
              settings.pool.listen_addr = "0.0.0.0:4444";
            };
          }
        ];
      };

      moduleConfig = moduleTest.config;
      moduleCheck =
        assert
          moduleConfig.networking.firewall.allowedTCPPorts == [
            4444
            9090
          ];
        assert
          moduleConfig.systemd.services.btcpool-rs.serviceConfig.EnvironmentFile == [
            "/run/secrets/btcpool-rs.env"
          ];
        assert builtins.elem "d /var/lib/btcpool-rs 0700 btcpool-rs btcpool-rs -"
          moduleConfig.systemd.tmpfiles.rules;
        assert builtins.elem "Z /var/lib/btcpool-rs - btcpool-rs btcpool-rs -"
          moduleConfig.systemd.tmpfiles.rules;
        pkgs.runCommand "btcpool-rs-nixos-module-check"
          {
            generatedConfig = moduleConfig.environment.etc."btcpool-rs/config.toml".source;
          }
          ''
            grep -F 'listen_addr = "0.0.0.0:4444"' "$generatedConfig"
            grep -F 'timeout_secs = 10' "$generatedConfig"
            grep -F '[security]' "$generatedConfig"
            grep -F '[sv2]' "$generatedConfig"
            grep -F 'strict_gbt_rules = true' "$generatedConfig"
            touch "$out"
          '';
    in
    {
      packages = {
        default = package;
        btcpool-rs = package;
      };

      apps.default = {
        type = "app";
        program = lib.getExe package;
      };

      checks = {
        package = package;
        nixos-module = moduleCheck;
      };

      formatter = pkgs.writeShellApplication {
        name = "format-btcpool-rs-nix";
        runtimeInputs = with pkgs; [
          findutils
          nixfmt
        ];
        text = ''
          has_path=false
          for arg in "$@"; do
            if [[ "$arg" != -* ]]; then
              has_path=true
            fi
          done

          if ! "$has_path"; then
            mapfile -d "" files < <(find . -path ./.git -prune -o -name '*.nix' -print0)
            set -- "$@" "''${files[@]}"
          fi

          exec nixfmt "$@"
        '';
      };

      devShells.default = pkgs.mkShell {
        inputsFrom = [ package ];
        packages = with pkgs; [
          bitcoin
          cargo
          clippy
          rust-analyzer
          rustc
          rustfmt
          nixfmt
        ];
      };
    };
}
