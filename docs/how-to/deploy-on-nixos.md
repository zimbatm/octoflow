# Deploy on NixOS

The flake exports `nixosModules.default`. It runs NATS with JetStream on
loopback, one systemd unit per service, and nginx with ACME in front of
the receiver and the SSE server.

## 1. Import the module

```nix
{
  inputs.octoflow.url = "github:zimbatm/octoflow";

  # in the host configuration
  imports = [ inputs.octoflow.nixosModules.default ];
}
```

## 2. Provide the secrets

The units read secrets from systemd `EnvironmentFile`s, so they never
enter the Nix store. With sops-nix or agenix, create:

- `octoflow-webhook`: `WEBHOOK_SECRET=...`
- `octoflow-s3`: `AWS_ACCESS_KEY_ID=...` and `AWS_SECRET_ACCESS_KEY=...`
- `octoflow-github` (optional): `GITHUB_TOKEN=...` for public-ingest

## 3. Configure the service

```nix
services.octoflow = {
  enable = true;
  domain = "events.example.org";
  webhook.environmentFile = config.sops.secrets.octoflow-webhook.path;
  s3 = {
    bucket = "octoflow";
    endpoint = "https://<account>.r2.cloudflarestorage.com";
    environmentFile = config.sops.secrets.octoflow-s3.path;
  };
  publicIngest = {
    repos = [ "NixOS/nixpkgs" ];
    environmentFile = config.sops.secrets.octoflow-github.path;
  };
};
```

Leave `s3` out and no s3 unit is created; leave `publicIngest.repos`
empty and no public-ingest unit is created. `domain` enables nginx:
`/webhook` goes to the receiver, everything else to the SSE server, with
buffering off. The options are listed in
[NixOS module](../reference/nixos-module.md).

## 4. Point GitHub at it

Follow [Configure the GitHub webhook](configure-github-webhook.md) with
`https://events.example.org/webhook` as the payload URL.

## 5. Check the units

```bash
systemctl status 'octoflow-*'
journalctl -u octoflow-webhook -f
curl -s http://127.0.0.1:9180/metrics | grep octoflow_webhook_requests_total
curl -s http://127.0.0.1:9181/metrics | grep octoflow_stream_messages
```

Each unit exposes Prometheus metrics on its own loopback port (9180 to
9184, see `services.octoflow.metricsListen`). Scrape them with your
Prometheus or `services.prometheus.scrapeConfigs`.

## 6. Run the VM test

`nix flake check` builds the package, runs the Go tests and boots a VM
with this module, delivers a signed event, reads it back over SSE and
checks the S3 upload. Run it before upgrading a host.
