{ pkgs, flake, ... }:
pkgs.testers.runNixOSTest {
  name = "octoflow";

  nodes.machine =
    { pkgs, ... }:
    {
      imports = [ flake.nixosModules.default ];
      virtualisation.memorySize = 2048;
      environment.systemPackages = [
        pkgs.curl
        pkgs.openssl
        pkgs.natscli
        pkgs.jq
        flake.packages.${pkgs.stdenv.hostPlatform.system}.default
      ];

      systemd.services.rustfs = {
        wantedBy = [ "multi-user.target" ];
        environment = {
          RUSTFS_ACCESS_KEY = "testkey";
          RUSTFS_SECRET_KEY = "testsecret";
        };
        serviceConfig = {
          ExecStart = "${pkgs.rustfs}/bin/rustfs server --address 127.0.0.1:9000 /var/lib/rustfs";
          StateDirectory = "rustfs";
        };
      };

      services.octoflow = {
        enable = true;
        webhook.environmentFile = pkgs.writeText "webhook.env" "WEBHOOK_SECRET=s3cret";
        s3 = {
          bucket = "octoflow";
          endpoint = "http://127.0.0.1:9000";
          region = "us-east-1";
          environmentFile = pkgs.writeText "s3.env" ''
            AWS_ACCESS_KEY_ID=testkey
            AWS_SECRET_ACCESS_KEY=testsecret
          '';
        };
      };
    };

  testScript = ''
    import json

    payload = json.dumps({
      "action": "opened",
      "pull_request": {"number": 7, "user": {"id": 2}},
      "repository": {"id": 111, "full_name": "numtide/x", "owner": {"login": "numtide", "id": 222}},
      "organization": {"login": "numtide", "id": 222},
      "sender": {"login": "zimbatm", "id": 2},
    })

    machine.wait_for_unit("nats.service")
    machine.wait_for_open_port(9000)
    machine.succeed(
      "curl -sf -X PUT --aws-sigv4 aws:amz:us-east-1:s3 --user testkey:testsecret http://127.0.0.1:9000/octoflow"
    )
    for unit in ["octoflow-webhook", "octoflow-dispatcher", "octoflow-sse", "octoflow-s3"]:
      machine.wait_for_unit(unit + ".service")
    machine.wait_for_open_port(8180)
    machine.wait_for_open_port(8181)

    machine.succeed(f"printf '%s' '{payload}' > /tmp/payload.json")
    sig = machine.succeed(
      "openssl dgst -sha256 -hmac s3cret /tmp/payload.json | awk '{print $2}'"
    ).strip()

    def deliver(signature):
      return machine.succeed(
        "curl -s -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:8180/webhook "
        "-H 'Content-Type: application/json' -H 'X-GitHub-Event: pull_request' "
        f"-H 'X-GitHub-Delivery: vm-1' -H 'X-Hub-Signature-256: {signature}' "
        "--data-binary @/tmp/payload.json"
      ).strip()

    with subtest("signed delivery is accepted, a forged one is not"):
      assert deliver("sha256=00") == "401"
      assert deliver(f"sha256={sig}") == "200"
      assert deliver(f"sha256={sig}") == "200"

    with subtest("the event leaves through SSE on its org, repo and pr routes, once"):
      for route in ["org/222", "repo/111", "pr/111/7"]:
        out = machine.wait_until_succeeds(
          f"timeout 3 curl -sN 'http://127.0.0.1:8181/events/{route}?last=10' | grep -c '^event: pull_request' || true",
          timeout=30,
        ).strip()
        assert out == "1", f"{route}: {out} events"

    with subtest("every service exposes metrics; the dispatcher counted the event"):
      machine.wait_until_succeeds(
        "curl -sf http://127.0.0.1:9181/metrics | grep -c 'octoflow_dispatched_events_total{status=\"ok\"} 1'",
        timeout=30,
      )
      machine.succeed("curl -sf http://127.0.0.1:9180/metrics | grep -c 'octoflow_webhook_requests_total{event_type=\"pull_request\",organization=\"numtide\",status=\"success\"} 2'")
      machine.succeed("curl -sf http://127.0.0.1:9182/metrics | grep -c 'octoflow_sse_events_sent_total{route=\"org\"} 1'")
      machine.succeed("curl -sf http://127.0.0.1:9183/healthz")
      machine.wait_until_succeeds(
        "curl -sf http://127.0.0.1:9181/metrics | grep -c '^octoflow_stream_messages [1-9]'",
        timeout=60,
      )

    with subtest("the s3-worker writes the batch when it stops"):
      machine.succeed("systemctl stop octoflow-s3")
      machine.succeed("journalctl -u octoflow-s3 | grep 'msg=\"Uploaded batch to S3\" event_count=1'")

    with subtest("the archive restores a lost stream"):
      machine.succeed("nats --server nats://127.0.0.1:4222 stream rm GITHUB_EVENTS_RAW -f")
      machine.succeed("systemctl restart octoflow-webhook octoflow-dispatcher octoflow-sse")
      for unit in ["octoflow-webhook", "octoflow-dispatcher", "octoflow-sse"]:
        machine.wait_for_unit(unit + ".service")
      machine.wait_for_open_port(8181)
      out = machine.succeed(
        "S3_BUCKET=octoflow S3_ENDPOINT=http://127.0.0.1:9000 AWS_REGION=us-east-1 "
        "AWS_ACCESS_KEY_ID=testkey AWS_SECRET_ACCESS_KEY=testsecret "
        "octoflow restore --from $(date -u +%F)"
      )
      assert "published=1 duplicates=0" in out, out
      for route in ["org/222", "repo/111", "pr/111/7"]:
        out = machine.wait_until_succeeds(
          f"timeout 3 curl -sN 'http://127.0.0.1:8181/events/{route}?last=10' | grep -c '^event: pull_request' || true",
          timeout=30,
        ).strip()
        assert out == "1", f"{route} after restore: {out} events"

    with subtest("a 2 MiB delivery, above the nats-server default max_payload, is stored"):
      machine.succeed(
        "head -c 2097152 /dev/zero | tr '\\0' x > /tmp/pad.txt && "
        "jq -c --rawfile pad /tmp/pad.txt '. + {padding: $pad}' /tmp/payload.json > /tmp/big.json"
      )
      bigsig = machine.succeed("openssl dgst -sha256 -hmac s3cret /tmp/big.json | awk '{print $2}'").strip()
      code = machine.succeed(
        "curl -s -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:8180/webhook "
        "-H 'Content-Type: application/json' -H 'X-GitHub-Event: push' "
        f"-H 'X-GitHub-Delivery: vm-big' -H 'X-Hub-Signature-256: sha256={bigsig}' "
        "--data-binary @/tmp/big.json"
      ).strip()
      assert code == "200", f"big delivery: {code}"
      out = machine.wait_until_succeeds(
        "timeout 3 curl -sN 'http://127.0.0.1:8181/events/type/push?last=1' | grep -c '^event: push' || true",
        timeout=30,
      ).strip()
      assert out == "1", f"type/push after the 2 MiB delivery: {out} events"
  '';
}
