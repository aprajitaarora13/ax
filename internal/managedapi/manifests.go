// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package managedapi

import (
	"bytes"
	"strings"
	"text/template"
)

// ManifestParams is everything needed to render one AxInstance's resources in
// the provider cluster.
type ManifestParams struct {
	Namespace       string
	Host            string // AX endpoint hostname (TLS name and JWT audience)
	CustomerProject string // parent project; PSC consumer and allowed principals
	Certs           *EdgeCerts
	PostgresPass    string

	EngineHost string // Managed Agent Substrate Instance hostname
	EngineIP   string // provider-side PSC IP for EngineHost
	EngineCA   string // Instance.ca_certificate

	CallerGSA     string // provider SA allowlisted on the engine Instance
	ServerImage   string
	RunnerImage   string
	EnvoyImage    string
	PostgresImage string
	PSCNATSubnet  string
}

// AllowedPrincipalSuffix is the caller email suffix allowed on the edge.
func (p ManifestParams) AllowedPrincipalSuffix() string {
	return "@" + p.CustomerProject + ".iam.gserviceaccount.com"
}

func indent(n int, s string) string {
	pad := strings.Repeat(" ", n)
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i := range lines {
		lines[i] = pad + lines[i]
	}
	return strings.Join(lines, "\n")
}

var manifestTmpl = template.Must(template.New("m").Funcs(template.FuncMap{"indent": indent}).Parse(manifestYAML))

// RenderManifests returns the multi-document YAML for one AxInstance.
func RenderManifests(p ManifestParams) (string, error) {
	var b bytes.Buffer
	data := struct {
		ManifestParams
		Suffix string
	}{p, p.AllowedPrincipalSuffix()}
	if err := manifestTmpl.Execute(&b, data); err != nil {
		return "", err
	}
	return b.String(), nil
}

const manifestYAML = `apiVersion: v1
kind: Namespace
metadata:
  name: {{.Namespace}}
  labels: {managed-ax/instance: "true"}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ax-server
  namespace: {{.Namespace}}
  annotations:
    iam.gke.io/gcp-service-account: {{.CallerGSA}}
---
apiVersion: v1
kind: Secret
metadata: {name: edge-tls, namespace: {{.Namespace}}}
type: kubernetes.io/tls
stringData:
  tls.crt: |
{{indent 4 .Certs.CertPEM}}
  tls.key: |
{{indent 4 .Certs.KeyPEM}}
---
apiVersion: v1
kind: Secret
metadata: {name: ax-store, namespace: {{.Namespace}}}
stringData:
  password: "{{.PostgresPass}}"
  url: "postgres://ax:{{.PostgresPass}}@postgres:5432/ax?sslmode=disable"
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: postgres, namespace: {{.Namespace}}}
spec:
  replicas: 1
  selector: {matchLabels: {app: postgres}}
  template:
    metadata: {labels: {app: postgres}}
    spec:
      containers:
      - name: postgres
        image: {{.PostgresImage}}
        env:
        - {name: POSTGRES_USER, value: ax}
        - {name: POSTGRES_DB, value: ax}
        - name: POSTGRES_PASSWORD
          valueFrom: {secretKeyRef: {name: ax-store, key: password}}
        - {name: PGDATA, value: /var/lib/postgresql/data/pgdata}
        ports: [{containerPort: 5432}]
        readinessProbe:
          exec: {command: [pg_isready, -U, ax]}
        resources: {requests: {cpu: 250m, memory: 512Mi}}
        volumeMounts: [{name: data, mountPath: /var/lib/postgresql/data}]
      volumes: [{name: data, emptyDir: {}}]
---
apiVersion: v1
kind: Service
metadata: {name: postgres, namespace: {{.Namespace}}}
spec:
  selector: {app: postgres}
  ports: [{port: 5432}]
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: ax-server, namespace: {{.Namespace}}}
spec:
  replicas: 1
  selector: {matchLabels: {app: ax-server}}
  template:
    metadata: {labels: {app: ax-server}}
    spec:
      serviceAccountName: ax-server
      hostAliases:
      - ip: {{.EngineIP}}
        hostnames: [{{.EngineHost}}]
      containers:
      - name: ax-server
        image: {{.ServerImage}}
        args: [--addr=:8080, --substrate-endpoint={{.EngineHost}}:443]
        env:
        - name: AX_STORE
          valueFrom: {secretKeyRef: {name: ax-store, key: url}}
        - {name: ATENET_ROUTER_ADDR, value: "https://{{.EngineHost}}:8443"}
        - {name: AX_DEFAULT_TASK_IMAGE, value: "{{.RunnerImage}}"}
        - name: SUBSTRATE_CA_PEM
          value: |
{{indent 12 .EngineCA}}
        ports: [{containerPort: 8080, name: h2c}]
        resources: {requests: {cpu: 500m, memory: 512Mi}}
---
apiVersion: v1
kind: Service
metadata: {name: ax-server, namespace: {{.Namespace}}}
spec:
  selector: {app: ax-server}
  ports: [{port: 8080, name: h2c, appProtocol: kubernetes.io/h2c}]
---
apiVersion: v1
kind: ConfigMap
metadata: {name: edge-envoy, namespace: {{.Namespace}}}
data:
  envoy.yaml: |
    static_resources:
      listeners:
      - name: ax_https
        address: {socket_address: {address: 0.0.0.0, port_value: 8443}}
        filter_chains:
        - transport_socket:
            name: envoy.transport_sockets.tls
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext
              common_tls_context:
                alpn_protocols: [h2, http/1.1]
                tls_certificates:
                - certificate_chain: {filename: /etc/edge-tls/tls.crt}
                  private_key: {filename: /etc/edge-tls/tls.key}
          filters:
          - name: envoy.filters.network.http_connection_manager
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
              stat_prefix: ax
              codec_type: AUTO
              stream_idle_timeout: 3600s
              http2_protocol_options: {allow_connect: true}
              upgrade_configs: [{upgrade_type: websocket}, {upgrade_type: CONNECT}]
              route_config:
                virtual_hosts:
                - name: ax
                  domains: ["*"]
                  routes:
                  - match: {prefix: /}
                    route: {cluster: ax_server, timeout: 0s, idle_timeout: 3600s}
              http_filters:
              - name: envoy.filters.http.jwt_authn
                typed_config:
                  "@type": type.googleapis.com/envoy.extensions.filters.http.jwt_authn.v3.JwtAuthentication
                  providers:
                    google:
                      issuer: https://accounts.google.com
                      audiences: ["https://{{.Host}}"]
                      forward: true
                      payload_in_metadata: jwt_payload
                      remote_jwks:
                        http_uri: {uri: "https://www.googleapis.com/oauth2/v3/certs", cluster: google_jwks, timeout: 5s}
                        cache_duration: 300s
                  rules:
                  - match: {prefix: /}
                    requires: {provider_name: google}
              - name: envoy.filters.http.rbac
                typed_config:
                  "@type": type.googleapis.com/envoy.extensions.filters.http.rbac.v3.RBAC
                  rules:
                    action: ALLOW
                    policies:
                      customer-service-accounts:
                        permissions: [{any: true}]
                        principals:
                        - metadata:
                            filter: envoy.filters.http.jwt_authn
                            path: [{key: jwt_payload}, {key: email}]
                            value: {string_match: {suffix: "{{.Suffix}}"}}
              - name: envoy.filters.http.router
                typed_config:
                  "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
      clusters:
      - name: ax_server
        type: STRICT_DNS
        connect_timeout: 5s
        typed_extension_protocol_options:
          envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
            "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
            explicit_http_config: {http2_protocol_options: {allow_connect: true}}
        load_assignment:
          cluster_name: ax_server
          endpoints:
          - lb_endpoints:
            - endpoint: {address: {socket_address: {address: ax-server.{{.Namespace}}.svc.cluster.local, port_value: 8080}}}
      - name: google_jwks
        type: LOGICAL_DNS
        dns_lookup_family: V4_ONLY
        connect_timeout: 5s
        load_assignment:
          cluster_name: google_jwks
          endpoints:
          - lb_endpoints:
            - endpoint: {address: {socket_address: {address: www.googleapis.com, port_value: 443}}}
        transport_socket:
          name: envoy.transport_sockets.tls
          typed_config:
            "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext
            sni: www.googleapis.com
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: edge, namespace: {{.Namespace}}}
spec:
  replicas: 1
  selector: {matchLabels: {app: edge}}
  template:
    metadata: {labels: {app: edge}}
    spec:
      containers:
      - name: envoy
        image: {{.EnvoyImage}}
        args: [-c, /etc/envoy/envoy.yaml, --log-level, info]
        ports: [{containerPort: 8443}]
        readinessProbe:
          tcpSocket: {port: 8443}
        resources: {requests: {cpu: 250m, memory: 256Mi}}
        volumeMounts:
        - {name: config, mountPath: /etc/envoy}
        - {name: tls, mountPath: /etc/edge-tls}
      volumes:
      - name: config
        configMap: {name: edge-envoy}
      - name: tls
        secret: {secretName: edge-tls}
---
apiVersion: v1
kind: Service
metadata:
  name: edge
  namespace: {{.Namespace}}
  annotations:
    networking.gke.io/load-balancer-type: "Internal"
spec:
  type: LoadBalancer
  selector: {app: edge}
  ports: [{name: https, port: 443, targetPort: 8443}]
---
apiVersion: networking.gke.io/v1
kind: ServiceAttachment
metadata: {name: edge, namespace: {{.Namespace}}}
spec:
  connectionPreference: ACCEPT_MANUAL
  natSubnets: [{{.PSCNATSubnet}}]
  proxyProtocol: false
  resourceRef: {kind: Service, name: edge}
  consumerAllowList:
  - {project: {{.CustomerProject}}, connectionLimit: 10}
`
