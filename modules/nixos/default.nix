{ flake, ... }:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.octoflow;

  common = {
    NATS_URL = cfg.natsUrl;
    NATS_STREAM_REPLICAS = "1";
  };

  hardening = {
    DynamicUser = true;
    Restart = "always";
    RestartSec = 5;
    PrivateTmp = true;
    ProtectSystem = "strict";
    ProtectHome = true;
    NoNewPrivileges = true;
    ProtectKernelTunables = true;
    ProtectKernelModules = true;
    ProtectControlGroups = true;
    RestrictAddressFamilies = [
      "AF_INET"
      "AF_INET6"
      "AF_UNIX"
    ];
    LockPersonality = true;
  };

  mkService =
    {
      description,
      # subcommand of the octoflow binary
      service,
      environment ? { },
      environmentFile ? null,
    }:
    {
      inherit description;
      wantedBy = [ "multi-user.target" ];
      wants = [ "network-online.target" ];
      after = [ "network-online.target" ] ++ lib.optional cfg.nats.enable "nats.service";
      requires = lib.optional cfg.nats.enable "nats.service";
      environment =
        common
        // lib.optionalAttrs (cfg.metricsListen.${service} or "" != "") {
          METRICS_LISTEN_ADDR = cfg.metricsListen.${service};
        }
        // environment;
      serviceConfig = hardening // {
        ExecStart = "${lib.getExe cfg.package} ${service}";
        EnvironmentFile = lib.optional (environmentFile != null) environmentFile;
      };
    };
in
{
  options.services.octoflow = {
    enable = lib.mkEnableOption "octoflow, GitHub events into NATS JetStream, S3 and SSE";

    package = lib.mkOption {
      type = lib.types.package;
      default = flake.packages.${pkgs.stdenv.hostPlatform.system}.default;
      defaultText = lib.literalExpression "octoflow.packages.\${system}.default";
    };

    natsUrl = lib.mkOption {
      type = lib.types.str;
      default = "nats://127.0.0.1:4222";
    };

    nats.enable = lib.mkOption {
      type = lib.types.bool;
      default = true;
      description = "Run a local NATS server with JetStream, bound to 127.0.0.1.";
    };

    metricsListen = lib.mkOption {
      type = lib.types.attrsOf lib.types.str;
      default = {
        webhook = "127.0.0.1:9180";
        dispatcher = "127.0.0.1:9181";
        sse = "127.0.0.1:9182";
        s3 = "127.0.0.1:9183";
        public-ingest = "127.0.0.1:9184";
      };
      description = "Prometheus /metrics address per service; an empty string disables it.";
    };

    domain = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      example = "events.nixpkgs.org";
      description = "When set, nginx serves this name with ACME: /webhook goes to the receiver, the rest to the SSE server.";
    };

    webhook = {
      listen = lib.mkOption {
        type = lib.types.str;
        default = "127.0.0.1:8180";
      };
      environmentFile = lib.mkOption {
        type = lib.types.nullOr lib.types.path;
        default = null;
        description = "systemd EnvironmentFile with WEBHOOK_SECRET=. The receiver refuses to start without one unless the file also sets WEBHOOK_ALLOW_UNSIGNED=true.";
      };
    };

    sse.listen = lib.mkOption {
      type = lib.types.str;
      default = "127.0.0.1:8181";
    };

    s3 = {
      bucket = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = "Bucket for the parquet history. The s3 service runs only when it is set.";
      };
      endpoint = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        example = "https://<account>.r2.cloudflarestorage.com";
      };
      region = lib.mkOption {
        type = lib.types.str;
        default = "auto";
        description = "R2 expects auto.";
      };
      keyPrefix = lib.mkOption {
        type = lib.types.str;
        default = "github-events";
      };
      environmentFile = lib.mkOption {
        type = lib.types.nullOr lib.types.path;
        default = null;
        description = "systemd EnvironmentFile with AWS_ACCESS_KEY_ID= and AWS_SECRET_ACCESS_KEY=.";
      };
    };

    publicIngest = {
      repos = lib.mkOption {
        type = lib.types.listOf lib.types.str;
        default = [ ];
        example = [ "NixOS/nixpkgs" ];
        description = ''
          Repositories read from the Events API and GH Archive, for those
          without a webhook. Do not list a repository that also has a
          webhook: the two sources use different ids, so its events would
          arrive twice.
        '';
      };
      environmentFile = lib.mkOption {
        type = lib.types.nullOr lib.types.path;
        default = null;
        description = "systemd EnvironmentFile with GITHUB_TOKEN=. Without it the Events API allows 60 requests per hour.";
      };
    };
  };

  config = lib.mkIf cfg.enable {
    services.nats = lib.mkIf cfg.nats.enable {
      enable = true;
      jetstream = true;
      settings.host = "127.0.0.1";
      # the receiver accepts 25 MB deliveries, GitHub's limit; the stored event adds an envelope.
      settings.max_payload = 32 * 1024 * 1024;
    };

    systemd.services = {
      octoflow-webhook = mkService {
        description = "octoflow webhook receiver";
        service = "webhook";
        environment.WEBHOOK_LISTEN_ADDR = cfg.webhook.listen;
        inherit (cfg.webhook) environmentFile;
      };

      octoflow-dispatcher = mkService {
        description = "octoflow event dispatcher";
        service = "dispatcher";
      };

      octoflow-sse = mkService {
        description = "octoflow SSE server";
        service = "sse";
        environment.SSE_LISTEN_ADDR = cfg.sse.listen;
      };
    }
    // lib.optionalAttrs (cfg.s3.bucket != null) {
      octoflow-s3 = mkService {
        description = "octoflow S3 parquet writer";
        service = "s3";
        environment = {
          S3_BUCKET = cfg.s3.bucket;
          AWS_REGION = cfg.s3.region;
          S3_KEY_PREFIX = cfg.s3.keyPrefix;
        }
        // lib.optionalAttrs (cfg.s3.endpoint != null) { S3_ENDPOINT = cfg.s3.endpoint; };
        inherit (cfg.s3) environmentFile;
      };
    }
    // lib.optionalAttrs (cfg.publicIngest.repos != [ ]) {
      octoflow-public-ingest = mkService {
        description = "octoflow Events API and GH Archive ingest";
        service = "public-ingest";
        environment.PUBLIC_REPOS = lib.concatStringsSep "," cfg.publicIngest.repos;
        inherit (cfg.publicIngest) environmentFile;
      };
    };

    services.nginx = lib.mkIf (cfg.domain != null) {
      enable = true;
      recommendedProxySettings = true;
      recommendedTlsSettings = true;
      virtualHosts.${cfg.domain} = {
        enableACME = true;
        forceSSL = true;
        locations."= /webhook".proxyPass = "http://${cfg.webhook.listen}";
        locations."/" = {
          proxyPass = "http://${cfg.sse.listen}";
          # SSE streams stay open; nginx must not buffer or time them out.
          extraConfig = ''
            proxy_buffering off;
            proxy_cache off;
            proxy_read_timeout 1h;
            proxy_http_version 1.1;
            proxy_set_header Connection "";
          '';
        };
      };
    };
  };
}
