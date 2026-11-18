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
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

type fakeProv struct {
	res        *ProvisionResult
	err        error
	provisions []ProvisionRequest
	deletes    []ProvisionRequest
}

func (f *fakeProv) Provision(_ context.Context, r ProvisionRequest) (*ProvisionResult, error) {
	f.provisions = append(f.provisions, r)
	return f.res, f.err
}

func (f *fakeProv) Deprovision(_ context.Context, r ProvisionRequest) error {
	f.deletes = append(f.deletes, r)
	return nil
}

type fakeAuthz struct{ allowProject string }

func (a fakeAuthz) Authorize(_ context.Context, bearer, project string, _ bool) error {
	if bearer == "good" && project == a.allowProject {
		return nil
	}
	return ErrPermissionDenied
}

func newTestServer(t *testing.T, prov Provisioner, state string) *Server {
	t.Helper()
	s := NewServer(prov, fakeAuthz{allowProject: "cust"}, state)
	s.async = func(f func()) { f() }
	return s
}

func do(t *testing.T, h http.Handler, method, path, token string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return rec, body
}

const base = "/v1alpha/projects/cust/locations/us-central1"

func TestCreateGetListDelete(t *testing.T) {
	prov := &fakeProv{res: &ProvisionResult{
		AxEndpoint: "demo-abc.example:443", ServiceAttachment: "projects/p/regions/us-central1/serviceAttachments/sa",
		CaCertificate: "PEM", SubstrateInstance: "projects/p/locations/us-south1/instances/engine",
	}}
	state := filepath.Join(t.TempDir(), "state.json")
	s := newTestServer(t, prov, state)
	h := s.Handler()

	rec, op := do(t, h, "POST", base+"/axInstances?axInstanceId=demo", "good")
	if rec.Code != 200 {
		t.Fatalf("create: %d %s", rec.Code, rec.Body)
	}
	if op["done"] != false || !strings.Contains(op["name"].(string), "/operations/") {
		t.Fatalf("create returned %v, want a pending operation", op)
	}
	if len(prov.provisions) != 1 || prov.provisions[0].Project != "cust" || prov.provisions[0].ID != "demo" {
		t.Fatalf("provision calls = %+v", prov.provisions)
	}

	opPath := "/v1alpha/" + op["name"].(string)
	_, done := do(t, h, "GET", opPath, "good")
	if done["done"] != true || done["response"].(map[string]any)["state"] != StateActive {
		t.Fatalf("operation after provisioning = %v", done)
	}

	rec, inst := do(t, h, "GET", base+"/axInstances/demo", "good")
	if rec.Code != 200 || inst["state"] != StateActive || inst["axEndpoint"] != "demo-abc.example:443" {
		t.Fatalf("get = %d %v", rec.Code, inst)
	}
	if inst["allowedPrincipalSuffix"] != "@cust.iam.gserviceaccount.com" {
		t.Errorf("allowedPrincipalSuffix = %v", inst["allowedPrincipalSuffix"])
	}
	if inst["privateServiceConnectConfig"].(map[string]any)["serviceAttachment"] == "" {
		t.Errorf("missing service attachment: %v", inst)
	}

	_, list := do(t, h, "GET", base+"/axInstances", "good")
	if n := len(list["axInstances"].([]any)); n != 1 {
		t.Errorf("list returned %d instances, want 1", n)
	}

	// State survives a restart.
	s2 := newTestServer(t, prov, state)
	if rec, _ := do(t, s2.Handler(), "GET", base+"/axInstances/demo", "good"); rec.Code != 200 {
		t.Errorf("get after reload = %d", rec.Code)
	}

	if rec, _ := do(t, h, "DELETE", base+"/axInstances/demo", "good"); rec.Code != 200 {
		t.Fatalf("delete = %d %s", rec.Code, rec.Body)
	}
	if len(prov.deletes) != 1 {
		t.Errorf("deprovision calls = %d, want 1", len(prov.deletes))
	}
	if rec, _ := do(t, h, "GET", base+"/axInstances/demo", "good"); rec.Code != 404 {
		t.Errorf("get after delete = %d, want 404", rec.Code)
	}
}

func TestCreateFailureMarksFailed(t *testing.T) {
	s := newTestServer(t, &fakeProv{err: errors.New("boom")}, "")
	h := s.Handler()
	_, op := do(t, h, "POST", base+"/axInstances?axInstanceId=demo", "good")
	_, done := do(t, h, "GET", "/v1alpha/"+op["name"].(string), "good")
	if done["done"] != true || done["error"] == nil {
		t.Fatalf("operation = %v, want done with error", done)
	}
	_, inst := do(t, h, "GET", base+"/axInstances/demo", "good")
	if inst["state"] != StateFailed || !strings.Contains(inst["errorMessage"].(string), "boom") {
		t.Errorf("instance = %v", inst)
	}
}

func TestRequestValidation(t *testing.T) {
	s := newTestServer(t, &fakeProv{res: &ProvisionResult{}}, "")
	h := s.Handler()
	cases := []struct {
		name, method, path, token string
		want                      int
	}{
		{"no token", "GET", base + "/axInstances", "", 401},
		{"bad token", "GET", base + "/axInstances", "bad", 403},
		{"other project", "GET", "/v1alpha/projects/other/locations/us-central1/axInstances", "good", 403},
		{"bad location", "GET", "/v1alpha/projects/cust/locations/europe-west1/axInstances", "good", 400},
		{"bad id", "POST", base + "/axInstances?axInstanceId=Bad_ID", "good", 400},
		{"missing id", "POST", base + "/axInstances", "good", 400},
		{"unknown instance", "GET", base + "/axInstances/nope", "good", 404},
		{"unknown op", "GET", base + "/operations/nope", "good", 404},
	}
	for _, c := range cases {
		if rec, _ := do(t, h, c.method, c.path, c.token); rec.Code != c.want {
			t.Errorf("%s: got %d, want %d (%s)", c.name, rec.Code, c.want, rec.Body)
		}
	}
	do(t, h, "POST", base+"/axInstances?axInstanceId=dup", "good")
	if rec, _ := do(t, h, "POST", base+"/axInstances?axInstanceId=dup", "good"); rec.Code != 409 {
		t.Errorf("duplicate create = %d, want 409", rec.Code)
	}
}
