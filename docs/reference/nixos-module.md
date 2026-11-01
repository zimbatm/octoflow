# NixOS module

`nixosModules.default` of the flake, defined in
`modules/nixos/default.nix`. Options live under `services.octoflow`.

## Options

| Option | Type | Default | Meaning |
|---|---|---|---|
| `enable` | bool | `false` | |
| `package` | package | the flake's package | |
| `natsUrl` | string | `nats://127.0.0.1:4222` | `NATS_URL` of every unit |
| `nats.enable` | bool | `true` | run `services.nats` with JetStream on 127.0.0.1 |
| `domain` | string or null | `null` | when set, nginx serves it with ACME: `= /webhook` to the receiver, `/` to the SSE server with buffering off and a 1 h read timeout |
| `metricsListen` | attrs of string | `webhook = "127.0.0.1:9180"`, `dispatcher = ...9181`, `sse = ...9182`, `s3 = ...9183`, `public-ingest = ...9184` | `METRICS_LISTEN_ADDR` per service; `""` disables one |
| `webhook.listen` | string | `127.0.0.1:8180` | `WEBHOOK_LISTEN_ADDR` |
| `webhook.environmentFile` | path or null | `null` | systemd `EnvironmentFile` with `WEBHOOK_SECRET=`; without a secret the unit fails unless the file sets `WEBHOOK_ALLOW_UNSIGNED=true` |
| `sse.listen` | string | `127.0.0.1:8181` | `SSE_LISTEN_ADDR` |
| `s3.bucket` | string or null | `null` | `S3_BUCKET`; the s3 unit exists only when set |
| `s3.endpoint` | string or null | `null` | `S3_ENDPOINT` |
| `s3.region` | string | `auto` | `AWS_REGION`; R2 expects `auto` |
| `s3.keyPrefix` | string | `github-events` | `S3_KEY_PREFIX` |
| `s3.environmentFile` | path or null | `null` | `AWS_ACCESS_KEY_ID=` and `AWS_SECRET_ACCESS_KEY=` |
| `publicIngest.repos` | list of string | `[]` | `PUBLIC_REPOS`; the public-ingest unit exists only when non-empty |
| `publicIngest.environmentFile` | path or null | `null` | `GITHUB_TOKEN=` |

## Units

| Unit | Runs | Exists when |
|---|---|---|
| `octoflow-webhook` | `octoflow webhook` | always |
| `octoflow-dispatcher` | `octoflow dispatcher` | always |
| `octoflow-sse` | `octoflow sse` | always |
| `octoflow-s3` | `octoflow s3` | `s3.bucket` is set |
| `octoflow-public-ingest` | `octoflow public-ingest` | `publicIngest.repos` is non-empty |

Every unit: `DynamicUser`, `Restart=always` after 5 s, `ProtectSystem=strict`,
`ProtectHome`, `PrivateTmp`, `NoNewPrivileges`, kernel and cgroup
protection, `AF_INET`, `AF_INET6` and `AF_UNIX` only. All set
`NATS_STREAM_REPLICAS=1` and wait for `network-online.target` and, when
`nats.enable`, `nats.service`.

The dashboard is not deployed by the module; serve `web/index.html`
from the package's `share/octoflow/web` with any static server if you
want it, and open it with `?sse=https://<domain>`.

## Test

`checks/nixos.nix` boots a VM with the module, rustfs as the S3 store
and a webhook secret, then checks: a forged delivery gets 401 and a
signed one 200; the event arrives once on the org, repo and pr SSE
routes; every unit serves metrics and the dispatcher counted the event;
stopping the s3 unit uploads the batch. `nix flake check` runs it.
