# Configure the GitHub webhook

This guide connects a GitHub organization (or one repository) to a
running receiver. You need admin rights on the organization and a URL
where GitHub can reach the receiver over HTTPS.

## 1. Choose a secret

Generate one and give it to the receiver:

```bash
openssl rand -hex 32
```

Set it as `WEBHOOK_SECRET` where the receiver runs. On NixOS it goes into
`services.octoflow.webhook.environmentFile`. The receiver refuses to
start without a secret unless `WEBHOOK_ALLOW_UNSIGNED=true` says you
mean it; do not do that on the internet.

## 2. Expose the receiver

GitHub must reach `/webhook` over HTTPS. The NixOS module does this with
nginx and ACME when `services.octoflow.domain` is set. For a laptop, a
tunnel works:

```bash
cloudflared tunnel --url http://localhost:8080
```

## 3. Add the webhook in GitHub

Organization settings, Webhooks, Add webhook:

| Field | Value |
|---|---|
| Payload URL | `https://your-domain/webhook` |
| Content type | `application/json` (the receiver rejects form encoding) |
| Secret | the value from step 1 |
| SSL verification | enabled |
| Events | the types you need; see below before choosing "Send me everything" |

GitHub sends a `ping` at once. The receiver answers 200 without storing
it; the delivery shows a green check in "Recent Deliveries".

"Send me everything" includes event types whose payloads are not public
even on a public repository: `repository_advisory`,
`secret_scanning_alert` and its `_location` and `_scan` events,
`code_scanning_alert`, `dependabot_alert`,
`repository_vulnerability_alert`, `security_and_analysis`, the
`exemption_request_*` and `dismissal_request_*` events, `deploy_key`
and `custom_property_values`. The stream and the archive expose
everything they hold, so leave those unchecked unless every reader is
trusted with them. An event that slipped through is removed with
[`octoflow wipe`](wipe-events.md).

## 4. Restrict the organizations, optionally

A receiver on a public URL accepts any delivery with a valid signature.
To accept only your organizations:

```bash
WEBHOOK_ALLOWED_ORGS=numtide,nix-community
```

Deliveries from other organizations get a 403.

## 5. Check a real event

Create a label or open an issue in a repository of the organization,
then:

```bash
nats sub "github.events.org.>"
```

or look at the delivery in GitHub: a 200 means stored; a 401 means the
secrets differ; a 503 means NATS did not acknowledge, and you can press
"Redeliver" once it is back. See [Troubleshoot](troubleshoot.md) for the
rest.
