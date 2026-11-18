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
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"time"
)

// Runner executes a command with optional stdin and returns its stdout.
type Runner func(ctx context.Context, stdin string, name string, args ...string) (string, error)

// ExecRunner runs commands on the host.
func ExecRunner(ctx context.Context, stdin, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(errb.String()))
	}
	return out.String(), nil
}

// ProviderConfig is the fixed provider-side environment.
type ProviderConfig struct {
	ProviderProject   string
	Kubeconfig        string
	SubstrateInstance string // full name of the engine Instance
	EngineHost        string
	EngineIP          string
	EngineCA          string
	CallerGSA         string
	ServerImage       string
	RunnerImage       string
	EnvoyImage        string
	PostgresImage     string
	PSCNATSubnet      string
	EndpointDomain    string // AX endpoints are <id>-<hash>.<EndpointDomain>
}

// KubeProvisioner creates each AxInstance as a namespace in the provider GKE
// cluster: Postgres, ax-server (backed by the engine Instance), an Envoy edge
// that checks Google ID tokens, and a PSC ServiceAttachment.
type KubeProvisioner struct {
	Cfg   ProviderConfig
	Run   Runner
	Poll  time.Duration
	clock func() time.Time
}

// NewKubeProvisioner returns a provisioner using the host's kubectl and gcloud.
func NewKubeProvisioner(cfg ProviderConfig) *KubeProvisioner {
	return &KubeProvisioner{Cfg: cfg, Run: ExecRunner, Poll: 10 * time.Second, clock: time.Now}
}

// Names returns the namespace and endpoint host for an instance. A short hash
// of the project keeps instances with the same ID in different projects apart.
func (p *KubeProvisioner) Names(req ProvisionRequest) (ns, host string) {
	sum := sha256.Sum256([]byte(req.Project + "/" + req.ID))
	h := hex.EncodeToString(sum[:])[:6]
	return "axi-" + req.ID + "-" + h, req.ID + "-" + h + "." + p.Cfg.EndpointDomain
}

func (p *KubeProvisioner) kubectl(ctx context.Context, stdin string, args ...string) (string, error) {
	return p.Run(ctx, stdin, "kubectl", append([]string{"--kubeconfig", p.Cfg.Kubeconfig}, args...)...)
}

func (p *KubeProvisioner) wiMember(ns string) string {
	return fmt.Sprintf("serviceAccount:%s.svc.id.goog[%s/ax-server]", p.Cfg.ProviderProject, ns)
}

// Provision implements Provisioner.
func (p *KubeProvisioner) Provision(ctx context.Context, req ProvisionRequest) (*ProvisionResult, error) {
	ns, host := p.Names(req)
	certs, err := NewEdgeCerts(req.ID, host, p.clock())
	if err != nil {
		return nil, err
	}
	pw := make([]byte, 12)
	if _, err := rand.Read(pw); err != nil {
		return nil, err
	}
	yaml, err := RenderManifests(ManifestParams{
		Namespace: ns, Host: host, CustomerProject: req.Project, Certs: certs,
		PostgresPass: hex.EncodeToString(pw),
		EngineHost:   p.Cfg.EngineHost, EngineIP: p.Cfg.EngineIP, EngineCA: p.Cfg.EngineCA,
		CallerGSA: p.Cfg.CallerGSA, ServerImage: p.Cfg.ServerImage, RunnerImage: p.Cfg.RunnerImage,
		EnvoyImage: p.Cfg.EnvoyImage, PostgresImage: p.Cfg.PostgresImage, PSCNATSubnet: p.Cfg.PSCNATSubnet,
	})
	if err != nil {
		return nil, fmt.Errorf("render manifests: %w", err)
	}
	// Let this instance's ax-server act as the engine caller SA (allowlisted on
	// the Substrate Instance because it belongs to the provider project).
	if _, err := p.Run(ctx, "", "gcloud", "iam", "service-accounts", "add-iam-policy-binding", p.Cfg.CallerGSA,
		"--project", p.Cfg.ProviderProject, "--role", "roles/iam.workloadIdentityUser",
		"--member", p.wiMember(ns), "--condition", "None", "--quiet"); err != nil {
		return nil, fmt.Errorf("workload identity binding: %w", err)
	}
	if _, err := p.kubectl(ctx, yaml, "apply", "-f", "-"); err != nil {
		return nil, fmt.Errorf("apply manifests: %w", err)
	}
	for _, d := range []string{"postgres", "ax-server", "edge"} {
		if _, err := p.kubectl(ctx, "", "-n", ns, "rollout", "status", "deploy/"+d, "--timeout=15m"); err != nil {
			return nil, fmt.Errorf("wait for %s: %w", d, err)
		}
	}
	sa, err := p.waitServiceAttachment(ctx, ns)
	if err != nil {
		return nil, err
	}
	return &ProvisionResult{
		AxEndpoint:        host + ":443",
		ServiceAttachment: sa,
		CaCertificate:     certs.CAPEM,
		SubstrateInstance: p.Cfg.SubstrateInstance,
	}, nil
}

func (p *KubeProvisioner) waitServiceAttachment(ctx context.Context, ns string) (string, error) {
	for {
		out, err := p.kubectl(ctx, "", "-n", ns, "get", "serviceattachment", "edge", "-o", "jsonpath={.status.serviceAttachmentURL}")
		if err == nil {
			if url := strings.TrimSpace(out); url != "" {
				// Return the resource path like Instance.private_service_connect_config.
				if i := strings.Index(url, "projects/"); i >= 0 {
					return url[i:], nil
				}
				return url, nil
			}
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("wait for service attachment: %w", ctx.Err())
		case <-time.After(p.Poll):
		}
	}
}

// Deprovision implements Provisioner.
func (p *KubeProvisioner) Deprovision(ctx context.Context, req ProvisionRequest) error {
	ns, _ := p.Names(req)
	if _, err := p.kubectl(ctx, "", "delete", "namespace", ns, "--ignore-not-found", "--wait=true", "--timeout=10m"); err != nil {
		return fmt.Errorf("delete namespace: %w", err)
	}
	if _, err := p.Run(ctx, "", "gcloud", "iam", "service-accounts", "remove-iam-policy-binding", p.Cfg.CallerGSA,
		"--project", p.Cfg.ProviderProject, "--role", "roles/iam.workloadIdentityUser",
		"--member", p.wiMember(ns), "--condition", "None", "--quiet"); err != nil &&
		!strings.Contains(err.Error(), "not found") && !strings.Contains(err.Error(), "does not exist") {
		return fmt.Errorf("remove workload identity binding: %w", err)
	}
	return nil
}

// IAMAuthorizer asks Cloud Resource Manager, with the caller's own access
// token, whether the caller holds the needed permission on the project.
// That stands in for agentsubstrate-style IAM permissions on AxInstances.
type IAMAuthorizer struct {
	Client  *http.Client
	BaseURL string // default https://cloudresourcemanager.googleapis.com
}

// Permissions checked for reads and for mutations.
const (
	ReadPermission   = "resourcemanager.projects.get"
	MutatePermission = "compute.forwardingRules.create"
)

// Authorize implements Authorizer.
func (a *IAMAuthorizer) Authorize(ctx context.Context, bearer, project string, mutate bool) error {
	perm := ReadPermission
	if mutate {
		perm = MutatePermission
	}
	base := a.BaseURL
	if base == "" {
		base = "https://cloudresourcemanager.googleapis.com"
	}
	body, _ := json.Marshal(map[string][]string{"permissions": {perm}})
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/projects/"+project+":testIamPermissions", bytes.NewReader(body))
	if err != nil {
		return err
	}
	r.Header.Set("Authorization", "Bearer "+bearer)
	r.Header.Set("Content-Type", "application/json")
	c := a.Client
	if c == nil {
		c = &http.Client{Timeout: 15 * time.Second}
	}
	resp, err := c.Do(r)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: testIamPermissions returned %s", ErrPermissionDenied, resp.Status)
	}
	var out struct {
		Permissions []string `json:"permissions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return err
	}
	for _, p := range out.Permissions {
		if p == perm {
			return nil
		}
	}
	return fmt.Errorf("%w: missing %s", ErrPermissionDenied, perm)
}
