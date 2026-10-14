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

    with subtest("the s3-worker writes the batch when it stops"):
      machine.succeed("systemctl stop octoflow-s3")
      machine.succeed("journalctl -u octoflow-s3 | grep 'Uploaded batch to S3 event_count=1'")
  '';
}
