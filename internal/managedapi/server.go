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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Provisioner builds and tears down the data plane for one AxInstance.
type Provisioner interface {
	Provision(ctx context.Context, req ProvisionRequest) (*ProvisionResult, error)
	Deprovision(ctx context.Context, req ProvisionRequest) error
}

// Authorizer checks that the caller (bearer access token) may act on project.
type Authorizer interface {
	Authorize(ctx context.Context, bearer, project string, mutate bool) error
}

// ErrPermissionDenied is returned by Authorizers to deny a request.
var ErrPermissionDenied = errors.New("permission denied")

var idRE = regexp.MustCompile(`^[a-z]([a-z0-9-]{0,18}[a-z0-9])?$`)

// SupportedLocation is the only location this prototype serves.
const SupportedLocation = "us-central1"

// Server serves the AxInstance API (v1alpha).
type Server struct {
	prov      Provisioner
	authz     Authorizer
	statePath string
	now       func() time.Time
	// async runs provisioning work; tests replace it to run inline.
	async func(func())

	mu         sync.Mutex
	instances  map[string]*AxInstance // by name
	operations map[string]*Operation  // by name
}

// NewServer creates a Server. statePath, when non-empty, persists state as JSON.
func NewServer(prov Provisioner, authz Authorizer, statePath string) *Server {
	s := &Server{
		prov:       prov,
		authz:      authz,
		statePath:  statePath,
		now:        time.Now,
		async:      func(f func()) { go f() },
		instances:  map[string]*AxInstance{},
		operations: map[string]*Operation{},
	}
	s.load()
	return s
}

type persisted struct {
	Instances  map[string]*AxInstance `json:"instances"`
	Operations map[string]*Operation  `json:"operations"`
}

func (s *Server) load() {
	if s.statePath == "" {
		return
	}
	b, err := os.ReadFile(s.statePath)
	if err != nil {
		return
	}
	var p persisted
	if json.Unmarshal(b, &p) == nil {
		if p.Instances != nil {
			s.instances = p.Instances
		}
		if p.Operations != nil {
			s.operations = p.Operations
		}
	}
}

// saveLocked writes state; s.mu must be held.
func (s *Server) saveLocked() {
	if s.statePath == "" {
		return
	}
	b, _ := json.MarshalIndent(persisted{s.instances, s.operations}, "", "  ")
	if err := os.WriteFile(s.statePath, b, 0o600); err != nil {
		log.Printf("save state: %v", err)
	}
}

// Handler returns the HTTP handler for the API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	base := "/v1alpha/projects/{project}/locations/{location}"
	mux.HandleFunc("POST "+base+"/axInstances", s.create)
	mux.HandleFunc("GET "+base+"/axInstances", s.list)
	mux.HandleFunc("GET "+base+"/axInstances/{id}", s.get)
	mux.HandleFunc("DELETE "+base+"/axInstances/{id}", s.delete)
	mux.HandleFunc("GET "+base+"/operations/{op}", s.getOperation)
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, code int, status, msg string) {
	writeJSON(w, code, map[string]Status{"error": {Code: code, Message: msg, Status: status}})
}

// authorize validates path params and the caller. It returns false after
// writing an error response.
func (s *Server) authorize(w http.ResponseWriter, r *http.Request, mutate bool) (project, location string, ok bool) {
	project, location = r.PathValue("project"), r.PathValue("location")
	if location != SupportedLocation {
		writeErr(w, http.StatusBadRequest, "INVALID_ARGUMENT", fmt.Sprintf("location %q is not supported; use %s", location, SupportedLocation))
		return "", "", false
	}
	bearer, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !found || strings.TrimSpace(bearer) == "" {
		writeErr(w, http.StatusUnauthorized, "UNAUTHENTICATED", "missing bearer access token")
		return "", "", false
	}
	if err := s.authz.Authorize(r.Context(), strings.TrimSpace(bearer), project, mutate); err != nil {
		writeErr(w, http.StatusForbidden, "PERMISSION_DENIED", fmt.Sprintf("caller may not use project %q: %v", project, err))
		return "", "", false
	}
	return project, location, true
}

func instanceName(project, location, id string) string {
	return fmt.Sprintf("projects/%s/locations/%s/axInstances/%s", project, location, id)
}

func newUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *Server) newOperationLocked(project, location, target, verb string) *Operation {
	op := &Operation{
		Name: fmt.Sprintf("projects/%s/locations/%s/operations/operation-%d-%s", project, location, s.now().UnixMilli(), newUID()[:8]),
		Metadata: OperationMetadata{
			Type:       "type.googleapis.com/managedax.v1alpha.OperationMetadata",
			CreateTime: s.now().UTC(),
			Target:     target,
			Verb:       verb,
		},
	}
	s.operations[op.Name] = op
	return op
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	project, location, ok := s.authorize(w, r, true)
	if !ok {
		return
	}
	id := r.URL.Query().Get("axInstanceId")
	if !idRE.MatchString(id) {
		writeErr(w, http.StatusBadRequest, "INVALID_ARGUMENT", "axInstanceId must match "+idRE.String())
		return
	}
	name := instanceName(project, location, id)

	s.mu.Lock()
	if _, exists := s.instances[name]; exists {
		s.mu.Unlock()
		writeErr(w, http.StatusConflict, "ALREADY_EXISTS", name+" already exists")
		return
	}
	now := s.now().UTC()
	inst := &AxInstance{Name: name, UID: newUID(), State: StateCreating, CreateTime: now, UpdateTime: now}
	s.instances[name] = inst
	op := s.newOperationLocked(project, location, name, "create")
	s.saveLocked()
	resp := *op
	req := ProvisionRequest{Project: project, Location: location, ID: id, UID: inst.UID}
	s.mu.Unlock()

	s.async(func() { s.runCreate(op.Name, req) })
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) runCreate(opName string, req ProvisionRequest) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	res, err := s.prov.Provision(ctx, req)

	s.mu.Lock()
	defer s.mu.Unlock()
	name := instanceName(req.Project, req.Location, req.ID)
	inst := s.instances[name]
	op := s.operations[opName]
	end := s.now().UTC()
	op.Done, op.Metadata.EndTime = true, &end
	if inst == nil {
		op.Error = &Status{Code: 10, Message: "instance deleted during create", Status: "ABORTED"}
		s.saveLocked()
		return
	}
	inst.UpdateTime = end
	if err != nil {
		inst.State, inst.ErrorMessage = StateFailed, err.Error()
		op.Error = &Status{Code: 13, Message: err.Error(), Status: "INTERNAL"}
		log.Printf("create %s failed: %v", name, err)
	} else {
		inst.State = StateActive
		inst.AxEndpoint = res.AxEndpoint
		inst.PrivateServiceConnectConfig = &PSCConfig{ServiceAttachment: res.ServiceAttachment}
		inst.CaCertificate = res.CaCertificate
		inst.SubstrateInstance = res.SubstrateInstance
		inst.AllowedPrincipalSuffix = "@" + req.Project + ".iam.gserviceaccount.com"
		cp := *inst
		op.Response = &cp
		log.Printf("create %s done: endpoint %s", name, res.AxEndpoint)
	}
	s.saveLocked()
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	project, location, ok := s.authorize(w, r, false)
	if !ok {
		return
	}
	name := instanceName(project, location, r.PathValue("id"))
	s.mu.Lock()
	inst, found := s.instances[name]
	var cp AxInstance
	if found {
		cp = *inst
	}
	s.mu.Unlock()
	if !found {
		writeErr(w, http.StatusNotFound, "NOT_FOUND", name+" not found")
		return
	}
	writeJSON(w, http.StatusOK, cp)
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	project, location, ok := s.authorize(w, r, false)
	if !ok {
		return
	}
	prefix := fmt.Sprintf("projects/%s/locations/%s/axInstances/", project, location)
	s.mu.Lock()
	out := []AxInstance{}
	for name, inst := range s.instances {
		if strings.HasPrefix(name, prefix) {
			out = append(out, *inst)
		}
	}
	s.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	writeJSON(w, http.StatusOK, map[string]any{"axInstances": out})
}

func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	project, location, ok := s.authorize(w, r, true)
	if !ok {
		return
	}
	id := r.PathValue("id")
	name := instanceName(project, location, id)
	s.mu.Lock()
	inst, found := s.instances[name]
	if !found {
		s.mu.Unlock()
		writeErr(w, http.StatusNotFound, "NOT_FOUND", name+" not found")
		return
	}
	if inst.State == StateCreating || inst.State == StateDeleting {
		s.mu.Unlock()
		writeErr(w, http.StatusConflict, "FAILED_PRECONDITION", name+" is "+inst.State)
		return
	}
	inst.State, inst.UpdateTime = StateDeleting, s.now().UTC()
	op := s.newOperationLocked(project, location, name, "delete")
	s.saveLocked()
	resp := *op
	req := ProvisionRequest{Project: project, Location: location, ID: id, UID: inst.UID}
	s.mu.Unlock()

	s.async(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer cancel()
		err := s.prov.Deprovision(ctx, req)
		s.mu.Lock()
		defer s.mu.Unlock()
		o := s.operations[op.Name]
		end := s.now().UTC()
		o.Done, o.Metadata.EndTime = true, &end
		if err != nil {
			o.Error = &Status{Code: 13, Message: err.Error(), Status: "INTERNAL"}
			if i := s.instances[name]; i != nil {
				i.State, i.ErrorMessage = StateFailed, err.Error()
			}
		} else {
			delete(s.instances, name)
		}
		s.saveLocked()
	})
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) getOperation(w http.ResponseWriter, r *http.Request) {
	project, location, ok := s.authorize(w, r, false)
	if !ok {
		return
	}
	name := fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, r.PathValue("op"))
	s.mu.Lock()
	op, found := s.operations[name]
	var cp Operation
	if found {
		cp = *op
	}
	s.mu.Unlock()
	if !found {
		writeErr(w, http.StatusNotFound, "NOT_FOUND", name+" not found")
		return
	}
	writeJSON(w, http.StatusOK, cp)
}
