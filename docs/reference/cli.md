# Command line, make targets and scripts

## `octoflow`

One binary; the argument names the service.

| Command | Runs |
|---|---|
| `octoflow webhook` | the GitHub webhook receiver |
| `octoflow dispatcher` | the fan-out from primary subjects to repo, user, pr and issue subjects |
| `octoflow s3` | the Parquet writer; needs `S3_BUCKET` |
| `octoflow sse` | the SSE server |
| `octoflow dashboard` | the static file server for `web/` |
| `octoflow public-ingest` | the Events API and GH Archive reader; needs `PUBLIC_REPOS` |
| `octoflow all` | every service in one process on one NATS connection; `s3` only with `S3_BUCKET`, `public-ingest` only with `PUBLIC_REPOS` |
| `octoflow restore --from DATE [--to DATE] [--event a,b] [--repo ID] [--org ID] [--dry-run]` | publishes archived events from S3 back into the stream; dates are UTC and inclusive, matched against `received_at`; needs the s3 settings and, unless `--dry-run`, NATS |
| `octoflow version` | prints the build version |

Exit status: 2 for a usage error, 1 when a service fails or cannot
start, 0 after a clean shutdown on SIGINT or SIGTERM. In `all` mode a
failing service stops the others.

## `make`

| Target | Does |
|---|---|
| `build` | `bin/octoflow`, then `go build ./...` (examples included) |
| `test` | `go test ./...`; several packages start an embedded NATS server |
| `test-race` | the same under the race detector |
| `test-coverage` | writes `coverage.txt` |
| `lint` | `golangci-lint run`, configured by `.golangci.yml` |
| `fmt` | `gofmt -w .` and `go mod tidy` |
| `run` | `scripts/start-local.sh`: checks `.env`, then hivemind with the Procfile |
| `nats-server` | a local NATS with JetStream |
| `clean` | removes `bin/` and `coverage.txt` |

## Nix

| Command | Does |
|---|---|
| `nix develop` | the development shell: Go, golangci-lint, nats-server, nats CLI, rustfs, hivemind, cloudflared |
| `nix build` | the package, `result/bin/octoflow`; runs `go test ./...` |
| `nix flake check` | the package and `checks/nixos.nix`, a VM test of the NixOS module |

After a `go.mod` change, set `vendorHash` in `package.nix` to
`pkgs.lib.fakeHash`, run `nix build`, and copy the hash it prints.

## Procfile

`make run` starts, in one hivemind session: `nats` (with JetStream and
monitoring on 8222), `s3-store` (rustfs on 127.0.0.1:9000 with data in
`.data/s3`), and `webhook`, `dispatcher`, `s3`, `sse` and `dashboard`,
each as `go run ./cmd/octoflow <service>`. The `s3` entry creates the
bucket first.

## Scripts

| Script | Does |
|---|---|
| `scripts/test-webhook.sh` | posts a signed sample `pull_request` delivery (repo 111, org 222, PR 7) to `WEBHOOK_URL` (default `http://localhost:8080/webhook`); needs `WEBHOOK_SECRET` |
| `scripts/test-sse.sh` | reads `/events`, `/stats` and a filtered route from the local SSE server |
| `scripts/test-system.sh` | starts hivemind and checks the health endpoints |
| `scripts/create-bucket.sh` | creates `$S3_BUCKET` on the local rustfs with `curl --aws-sigv4` |
| `scripts/clear-streams.sh` | deletes every NATS stream after a confirmation |
| `scripts/wait-for.sh PORT CMD...` | runs `CMD` once `PORT` accepts connections |
| `scripts/start-local.sh` | what `make run` calls |
