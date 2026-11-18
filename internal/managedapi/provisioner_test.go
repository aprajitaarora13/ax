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
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func testParams(t *testing.T) ManifestParams {
	t.Helper()
	certs, err := NewEdgeCerts("demo", "demo-abc.us-central1.managed-ax.internal", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return ManifestParams{
		Namespace: "axi-demo-abc", Host: "demo-abc.us-central1.managed-ax.internal", CustomerProject: "cust",
		Certs: certs, PostgresPass: "pw", EngineHost: "as-1.us-south1.staging.agentsubstrate.goog",
		EngineIP: "10.0.0.5", EngineCA: "-----BEGIN CERTIFICATE-----\nX\n-----END CERTIFICATE-----\n",
		CallerGSA: "caller@prov.iam.gserviceaccount.com", ServerImage: "srv", RunnerImage: "run",
		EnvoyImage: "envoy", PostgresImage: "pg", PSCNATSubnet: "nat",
	}
}

func TestEdgeCertsChain(t *testing.T) {
	c, err := NewEdgeCerts("demo", "h.example", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM([]byte(c.CAPEM)) {
		t.Fatal("bad CA PEM")
	}
	b, _ := pem.Decode([]byte(c.CertPEM))
	leaf, err := x509.ParseCertificate(b.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{DNSName: "h.example", Roots: pool}); err != nil {
		t.Errorf("leaf does not verify against CA: %v", err)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{DNSName: "other.example", Roots: pool}); err == nil {
		t.Error("leaf verified for the wrong host")
	}
}

func TestRenderManifests(t *testing.T) {
	p := testParams(t)
	out, err := RenderManifests(p)
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]map[string]any{}
	dec := yaml.NewDecoder(strings.NewReader(out))
	for {
		var doc map[string]any
		if err := dec.Decode(&doc); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatalf("invalid YAML: %v\n%s", err, out)
		}
		name := doc["metadata"].(map[string]any)["name"].(string)
		kinds[fmt.Sprintf("%s/%s", doc["kind"], name)] = doc
	}
	for _, k := range []string{"Namespace/axi-demo-abc", "Deployment/ax-server", "Deployment/edge", "Deployment/postgres",
		"Service/edge", "ServiceAttachment/edge", "ConfigMap/edge-envoy", "Secret/edge-tls", "ServiceAccount/ax-server"} {
		if kinds[k] == nil {
			t.Errorf("missing %s", k)
		}
	}

	// The PSC attachment only admits the customer project.
	sa := kinds["ServiceAttachment/edge"]["spec"].(map[string]any)
	if got := sa["consumerAllowList"].([]any)[0].(map[string]any)["project"]; got != "cust" {
		t.Errorf("consumerAllowList project = %v", got)
	}

	// Envoy config parses and pins audience + allowed principal suffix.
	envoyCfg := kinds["ConfigMap/edge-envoy"]["data"].(map[string]any)["envoy.yaml"].(string)
	var envoy map[string]any
	if err := yaml.Unmarshal([]byte(envoyCfg), &envoy); err != nil {
		t.Fatalf("envoy config is not valid YAML: %v", err)
	}
	for _, want := range []string{`audiences: ["https://demo-abc.us-central1.managed-ax.internal"]`, `suffix: "@cust.iam.gserviceaccount.com"`, "ax-server.axi-demo-abc.svc.cluster.local"} {
		if !strings.Contains(envoyCfg, want) {
			t.Errorf("envoy config missing %q", want)
		}
	}

	// ax-server targets the engine through its PSC IP and trusts its CA.
	if !strings.Contains(out, "--substrate-endpoint=as-1.us-south1.staging.agentsubstrate.goog:443") ||
		!strings.Contains(out, "- ip: 10.0.0.5") || !strings.Contains(out, "            -----BEGIN CERTIFICATE-----") {
		t.Error("ax-server is not wired to the engine Instance")
	}
}

func TestKubeProvisionerProvisionAndDeprovision(t *testing.T) {
	var calls []string
	run := func(_ context.Context, stdin, name string, args ...string) (string, error) {
		line := name + " " + strings.Join(args, " ")
		calls = append(calls, line)
		if strings.Contains(line, "jsonpath={.status.serviceAttachmentURL}") {
			return "https://www.googleapis.com/compute/v1/projects/prov/regions/us-central1/serviceAttachments/sa1", nil
		}
		if strings.Contains(line, "subnets describe") {
			return "", errors.New("not found")
		}
		if strings.Contains(line, "subnets list") {
			return "10.128.0.0/20\n192.168.0.0/26\n", nil
		}
		if strings.Contains(line, "apply -f -") && !strings.Contains(stdin, "kind: ServiceAttachment") {
			return "", errors.New("apply got no manifests")
		}
		return "", nil
	}
	p := &KubeProvisioner{Cfg: ProviderConfig{ProviderProject: "prov", Kubeconfig: "kc", CallerGSA: "caller@prov.iam.gserviceaccount.com",
		SubstrateInstance: "engine", EndpointDomain: "managed-ax.internal", Network: "default", Region: "us-central1",
		NATSupernet: "192.168.0.0/16"}, Run: run, Poll: time.Millisecond, clock: time.Now}
	req := ProvisionRequest{Project: "cust", Location: "us-central1", ID: "demo"}
	res, err := p.Provision(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	ns, host := p.Names(req)
	if res.AxEndpoint != host+":443" || res.ServiceAttachment != "projects/prov/regions/us-central1/serviceAttachments/sa1" || res.SubstrateInstance != "engine" {
		t.Errorf("result = %+v", res)
	}
	if !strings.HasPrefix(res.CaCertificate, "-----BEGIN CERTIFICATE-----") {
		t.Error("missing CA certificate")
	}
	joined := strings.Join(calls, "\n")
	for _, want := range []string{"roles/iam.workloadIdentityUser --member serviceAccount:prov.svc.id.goog[" + ns + "/ax-server]",
		"subnets create psc-nat-" + ns + " --project prov --network default --region us-central1 --range 192.168.0.64/26 --purpose PRIVATE_SERVICE_CONNECT",
		"kubectl --kubeconfig kc apply -f -", "rollout status deploy/ax-server"} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing call containing %q in:\n%s", want, joined)
		}
	}

	calls = nil
	if err := p.Deprovision(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(calls[0], "delete namespace "+ns) || !strings.Contains(calls[1], "remove-iam-policy-binding") ||
		!strings.Contains(calls[2], "subnets delete psc-nat-"+ns) {
		t.Errorf("deprovision calls = %v", calls)
	}
}

func TestPickFreeRange(t *testing.T) {
	cases := []struct {
		used []string
		want string
	}{
		{nil, "192.168.0.0/26"},
		{[]string{"192.168.0.0/26", "192.168.0.64/26"}, "192.168.0.128/26"},
		{[]string{"192.168.0.0/24"}, "192.168.1.0/26"},
		{[]string{"192.168.0.32/28"}, "192.168.0.64/26"},
		{[]string{"10.128.0.0/9", "garbage"}, "192.168.0.0/26"},
	}
	for _, c := range cases {
		got, err := PickFreeRange("192.168.0.0/16", c.used)
		if err != nil || got != c.want {
			t.Errorf("PickFreeRange(%v) = %q, %v; want %q", c.used, got, err, c.want)
		}
	}
	if _, err := PickFreeRange("192.168.0.0/26", []string{"192.168.0.0/26"}); err == nil {
		t.Error("expected exhaustion error")
	}
}

func TestNamesAreDistinctPerProject(t *testing.T) {
	p := &KubeProvisioner{Cfg: ProviderConfig{EndpointDomain: "d"}}
	a, _ := p.Names(ProvisionRequest{Project: "a", ID: "x"})
	b, _ := p.Names(ProvisionRequest{Project: "b", ID: "x"})
	if a == b {
		t.Errorf("same namespace %q for different projects", a)
	}
}

func TestIAMAuthorizer(t *testing.T) {
	var gotAuth, gotBody string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		if !strings.HasSuffix(r.URL.Path, "/v1/projects/cust:testIamPermissions") {
			w.WriteHeader(404)
			return
		}
		if bytes.Contains(b, []byte(ReadPermission)) {
			fmt.Fprintf(w, `{"permissions":[%q]}`, ReadPermission)
			return
		}
		fmt.Fprint(w, `{}`)
	}))
	defer ts.Close()
	a := &IAMAuthorizer{BaseURL: ts.URL}
	if err := a.Authorize(context.Background(), "tok", "cust", false); err != nil {
		t.Errorf("read: %v", err)
	}
	if gotAuth != "Bearer tok" || !strings.Contains(gotBody, ReadPermission) {
		t.Errorf("request auth=%q body=%q", gotAuth, gotBody)
	}
	if err := a.Authorize(context.Background(), "tok", "cust", true); !errors.Is(err, ErrPermissionDenied) {
		t.Errorf("mutate without permission: err = %v", err)
	}
	if err := a.Authorize(context.Background(), "tok", "other", false); !errors.Is(err, ErrPermissionDenied) {
		t.Errorf("unknown project: err = %v", err)
	}
}
