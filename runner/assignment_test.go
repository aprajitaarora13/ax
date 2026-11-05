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

package runner_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/google/ax/runner"
	"gopkg.in/yaml.v3"
)

// assignmentRunner starts a runner that waits for its assignment, with a
// harness script that records its goal, config, and env to out.
type assignmentRunner struct {
	cfg      runner.Config
	out      string
	wsPath   string
	finished chan error
	cancel   context.CancelFunc
}

func newAssignmentRunner(t *testing.T, stateDir string) *assignmentRunner {
	t.Helper()
	dir := t.TempDir()
	r := &assignmentRunner{out: filepath.Join(dir, "harness.out"), wsPath: filepath.Join(dir, "workspace")}
	harness := filepath.Join(dir, "ax-harness")
	script := fmt.Sprintf("#!/bin/sh\n{ cat \"$AX_GOAL_FILE\"; echo; cat \"$AX_CONFIG_FILE\"; echo; echo \"token=$TOKEN\"; } > %s\n", r.out)
	if err := os.WriteFile(harness, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	r.cfg = runner.Config{
		Port:            freePort(t),
		MetadataPort:    freePort(t),
		AwaitAssignment: true,
		StateDir:        stateDir,
		RunDir:          filepath.Join(dir, "run"),
		HarnessPath:     harness,
	}
	return r
}

func (r *assignmentRunner) start(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.finished = make(chan error, 1)
	go func() { r.finished <- runner.Run(ctx, r.cfg) }()
	t.Cleanup(r.stop)
	waitFor(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", r.cfg.Port))
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
}

func (r *assignmentRunner) stop() {
	if r.cancel != nil {
		r.cancel()
		<-r.finished
		r.cancel = nil
	}
}

func (r *assignmentRunner) url(path string) string {
	return fmt.Sprintf("http://127.0.0.1:%d%s", r.cfg.Port, path)
}

func (r *assignmentRunner) assign(t *testing.T, task *v1alpha1.Task) int {
	t.Helper()
	taskYAML, err := yaml.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(v1alpha1.Assignment{TaskYAML: string(taskYAML)})
	resp, err := http.Post(r.url("/v1alpha1/assignment"), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST assignment: %v", err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func (r *assignmentRunner) readyz(t *testing.T, query string) int {
	t.Helper()
	resp, err := http.Get(r.url("/readyz" + query))
	if err != nil {
		t.Fatalf("GET readyz: %v", err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func harnessTask(t *testing.T, name, wsPath, prompt string) *v1alpha1.Task {
	t.Helper()
	var task v1alpha1.Task
	src := fmt.Sprintf(`
metadata: {name: %s, atespace: default}
spec:
  workspace: {name: ws, path: %q}
  env: [{name: TOKEN, value: from-assignment}]
  config: {prompt: %q, model: some-model}
`, name, wsPath, prompt)
	if err := runner.DecodeSpec([]byte(src), &task); err != nil {
		t.Fatal(err)
	}
	return &task
}

func TestRun_AwaitsAssignmentAndRunsHarness(t *testing.T) {
	r := newAssignmentRunner(t, t.TempDir())
	r.start(t)

	// Waiting for the assignment: alive for Substrate's wakeup probe, but the
	// workspace is not ready.
	if got := r.readyz(t, ""); got != http.StatusOK {
		t.Errorf("readyz while awaiting = %d, want 200", got)
	}
	if got := r.readyz(t, "?check=workspace"); got != http.StatusServiceUnavailable {
		t.Errorf("readyz?check=workspace while awaiting = %d, want 503", got)
	}

	task := harnessTask(t, "fix-tests", r.wsPath, "run the tests and fix them")
	if got := r.assign(t, task); got != http.StatusOK {
		t.Fatalf("assignment status = %d, want 200", got)
	}
	// Delivering the same task again, as every resume does, is a no-op.
	if got := r.assign(t, task); got != http.StatusOK {
		t.Errorf("repeat assignment status = %d, want 200", got)
	}
	// A sandbox runs exactly one task.
	if got := r.assign(t, harnessTask(t, "other", r.wsPath, "x")); got != http.StatusConflict {
		t.Errorf("different assignment status = %d, want 409", got)
	}

	waitFor(t, func() bool {
		_, err := os.Stat(r.out)
		return err == nil && r.readyz(t, "?check=workspace") == http.StatusOK
	})
	waitFor(t, func() bool { b, _ := os.ReadFile(r.out); return strings.Contains(string(b), "token=") })
	got, _ := os.ReadFile(r.out)
	for _, want := range []string{"run the tests and fix them", `"model":"some-model"`, "token=from-assignment"} {
		if !strings.Contains(string(got), want) {
			t.Errorf("harness output %q missing %q", got, want)
		}
	}
}

func TestRun_ResumesFromPersistedAssignment(t *testing.T) {
	stateDir := t.TempDir()
	first := newAssignmentRunner(t, stateDir)
	first.start(t)
	task := harnessTask(t, "fix-tests", first.wsPath, "keep going")
	if got := first.assign(t, task); got != http.StatusOK {
		t.Fatalf("assignment status = %d, want 200", got)
	}
	waitFor(t, func() bool { b, _ := os.ReadFile(first.out); return strings.Contains(string(b), "token=") })
	first.stop()

	info, err := os.Stat(filepath.Join(stateDir, "assignment.json"))
	if err != nil {
		t.Fatalf("assignment not persisted: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("persisted assignment mode = %o, want 600", perm)
	}

	// A resumed container starts a new runner on the same durable volume: it
	// picks the task up without a new delivery.
	second := newAssignmentRunner(t, stateDir)
	second.start(t)
	waitFor(t, func() bool { b, _ := os.ReadFile(second.out); return strings.Contains(string(b), "keep going") })
}
