# Managed AX API prototype

A prototype of **Managed AX exposed as its own Google Cloud style API**, built
the same way `agentsubstrate.googleapis.com` exposes Agent Substrate:

*   **Control API** (`cmd/ax-managed-api`, `internal/managedapi`):
    `projects/{project}/locations/us-central1/axInstances` with create / get /
    list / delete and long-running operations. Callers authenticate with a
    Google access token; the server checks their IAM permissions on the parent
    (customer) project.
*   **Work API**: AX's own `ax.v1alpha1` API, served at the instance's
    `axEndpoint` over Private Service Connect. An Envoy edge accepts only Google
    ID tokens whose audience is the endpoint and whose email belongs to a
    service account of the customer project, and presents a per-instance CA
    (`caCertificate`).

Each AxInstance is a namespace in a provider GKE cluster (Postgres, ax-server,
Envoy edge, internal LoadBalancer, PSC ServiceAttachment). Sandboxes run on a
Managed Agent Substrate Instance (ATE on ACE microVMs) owned by the provider.

```
customer project                         provider project
----------------                         -------------------------------------
REST: create AxInstance  --------------> ax-managed-api (control API)
                                            | provisions                 
ax CLI --PSC + ID token----------------> Envoy edge -> ax-server -> Postgres
                                                          |
                                                          v  PSC + ID token
                                         Managed Agent Substrate Instance
                                         (ATE on ACE microVMs)
```

Customer flow:

```bash
# 1. Create (control API).
curl -X POST -H "Authorization: Bearer $(gcloud auth application-default print-access-token)" \
  "$API/v1alpha/projects/$PROJECT/locations/us-central1/axInstances?axInstanceId=demo"
# 2. Connect: PSC endpoint to privateServiceConnectConfig.serviceAttachment and
#    a private DNS record for axEndpoint's host.
# 3. Use AX right away, as a service account of $PROJECT.
export AX_SERVER=https://<axEndpoint> SSL_CERT_FILE=ca.pem
export AX_AUTH_TOKEN=<ID token, audience https://<axEndpoint host>>
ax apply -f task.yaml && ax resume task <name> && ax ssh <name>
```

Prototype only: the provider environment is hardcoded in
`cmd/ax-managed-api/main.go`, and the control API runs next to `kubectl` and
`gcloud`.
