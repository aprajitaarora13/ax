# AX on Managed Agent Substrate (prototype)

Not for merge. This runs `ax-server` in a customer project against a Managed
Agent Substrate Instance (`agentsubstrate.googleapis.com`), reached only over
Private Service Connect (PSC), and drives it with the `ax` CLI from a laptop.

## What the code changes do

| Area | Change |
| :--- | :--- |
| Substrate client (`internal/substrate`) | Treats an external hostname as a managed Instance: TLS name = the host, Instance CA from `SUBSTRATE_CA_PEM`/`SUBSTRATE_CA_FILE`, and a Google ID token for that audience from the metadata server (no token file). |
| ActorTemplates | If the sandbox class rejects `system_info` volumes (ACE microVMs), retries without them and with MEMORY snapshots, because restarting a VOLUMES snapshot from disk fails with `Required field 'spec.bootImage' not specified`. |
| Readiness (`internal/controller`) | Probes `/readyz` through the router edge (`:8443`, ID token + Instance CA) when the actor has no worker pod IP, which is always the case on ACE. |
| `ax ssh` (`internal/server`, `internal/metadata`) | The managed router reaches microVM actors over HTTP/1.1 only, so `ax-server` carries guest gRPC calls to the runner as framed HTTP/1.1 through the router's CONNECT endpoint (`:8444`). The runner replays them over loopback h2c. |
| Actor ports | `GET /v1alpha1/tasks/{atespace}/{task}/port/{port}/...` on `ax-server` tunnels to any actor port through `:8444`. |
| `ax` CLI | `AX_SERVER=https://...` uses TLS, `AX_AUTH_TOKEN` is sent as a bearer token (Cloud Run IAM), and `ax ssh` goes through `ax-server` when there is no cluster. |

## Setup used (autopush)

1. Lease a consumer project from PRS pool `celery_autopush_default` and create
   an Instance (`CreateInstance` over CCFE stubby).
2. In the consumer project: a PSC endpoint to
   `Instance.private_service_connect_config.service_attachment`, a private
   Cloud DNS zone mapping `Instance.endpoint`'s host to the PSC IP, Cloud NAT,
   and Postgres reachable from the VPC.
3. Grant `roles/artifactregistry.reader` on the consumer image repository to
   the Instance's tenant service accounts (names derived from the celery
   blueprint: `<tenant-project-number>-compute@developer.gserviceaccount.com`,
   `ate-api-<sha256(instance id)[:8]>@<tenant-project>.iam.gserviceaccount.com`,
   `substrate-nodes-<same suffix>@...`).
4. Deploy `ax-server` to Cloud Run with Direct VPC egress, as a consumer
   project service account (allowlisted by the Instance), with
   `ATENET_ROUTER_ADDR=https://<host>:8443`, `SUBSTRATE_CA_PEM`, `AX_STORE`, and
   `--substrate-endpoint=<host>:443`.
5. From a laptop:
   `AX_SERVER=https://<cloud-run-url> AX_AUTH_TOKEN=$(gcloud auth print-identity-token) ax apply -f ...`

`demo_agent.py` is the agent the demo Task runs: it reads recent commits from
GitHub and asks Gemini to write release notes into `/workspace/report.md`.

## Known gaps

* ACE sandbox egress goes through Google Cloud Agent Gateway TLS inspection,
  not Substrate's egress gateway, so AX Gateway allowlists are not enforced and
  the sandbox has no copy of the inspection CA. The demo agent trusts the CA
  from the presented chain (trust on first use), which is prototype-only.
* Credentials reach the sandbox as task env (the Gemini API key), because
  egress credential injection does not apply on ACE.
* MEMORY suspend on ACE keeps the microVM running (idle mode); a real snapshot
  restore (VOLUMES) is blocked by the `spec.bootImage` bug above.
* `ax-server` relies on Cloud Run IAM for caller authentication and has no
  per-task authorization of its own.
