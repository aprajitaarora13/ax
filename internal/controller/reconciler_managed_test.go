// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"context"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/ax/internal/substrate/substratetest"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// TestResumeViaManagedRouterWithoutWorkerIP verifies that Resume marks
// WorkspaceReady=True when the Substrate actor has no worker pod IP (as on
// microVM-backed Managed Agent Substrate Instances) by probing the external
// router edge over TLS with the Instance CA and OIDC bearer token.
func TestResumeViaManagedRouterWithoutWorkerIP(t *testing.T) {
	edge := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" || r.URL.Query().Get("check") != "workspace" {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		if r.Header.Get("Authorization") != "Bearer edge-id-token" {
			http.Error(w, "Jwt is missing", http.StatusUnauthorized)
			return
		}
		if r.Header.Get("ate-target-actor") != "default/managed-task" {
			http.Error(w, "bad target actor", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer edge.Close()

	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: edge.Certificate().Raw})
	if err := os.WriteFile(caFile, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	tokenFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenFile, []byte("edge-id-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SUBSTRATE_CA_FILE", caFile)
	t.Setenv("SUBSTRATE_CA_PEM", "")
	t.Setenv("SUBSTRATE_TOKEN_FILE", tokenFile)
	// httptest TLS certificates carry SAN "example.com", which IsManagedTarget treats as external.
	t.Setenv("ATENET_ROUTER_ADDR", "https://example.com:8443")

	sub := substratetest.NewServer(t) // workerIP is empty, matching microVM actors.
	r := NewTaskReconciler(sub.Client(), "default-template", v1alpha1.DefaultSystemNamespace, "runner@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	r.WorkspaceReadyTimeout = 2 * time.Second
	edgeAddr := edge.Listener.Addr().String()
	var dialer net.Dialer
	r.managedRouterDialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return dialer.DialContext(ctx, network, edgeAddr)
	}

	task := &v1alpha1.Task{
		ApiVersion: v1alpha1.APIVersion,
		Kind:       v1alpha1.KindTask,
		Metadata:   &v1alpha1.ObjectMeta{Name: "managed-task", Atespace: "default"},
		Spec:       &v1alpha1.TaskSpec{},
	}
	if err := r.Resume(context.Background(), task, nil, nil); err != nil {
		t.Fatalf("Resume() failed: %v", err)
	}
	if !conditionTrue(task, v1alpha1.ConditionWorkspaceReady) {
		t.Fatalf("WorkspaceReady = False after managed router probe, conditions = %v", task.GetStatus().GetConditions())
	}

	// Without the Instance CA, TLS verification against the edge fails closed.
	t.Setenv("SUBSTRATE_CA_FILE", "")
	taskUntrusted := &v1alpha1.Task{
		ApiVersion: v1alpha1.APIVersion,
		Kind:       v1alpha1.KindTask,
		Metadata:   &v1alpha1.ObjectMeta{Name: "untrusted-task", Atespace: "default"},
		Spec:       &v1alpha1.TaskSpec{},
	}
	r.WorkspaceReadyTimeout = 150 * time.Millisecond
	if err := r.Resume(context.Background(), taskUntrusted, nil, nil); err != nil {
		t.Fatalf("Resume() with untrusted router failed unexpectedly: %v", err)
	}
	if conditionTrue(taskUntrusted, v1alpha1.ConditionWorkspaceReady) {
		t.Fatal("WorkspaceReady = True without trusting Instance CA, want False")
	}
}
