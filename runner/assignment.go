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

package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/ax/pkg/apis/v1alpha1"
)

// assignmentWaiter holds the task assignment a runner receives at runtime and
// persists it on the durable volume so a resumed container finds it again.
type assignmentWaiter struct {
	path string
	once sync.Once
	ch   chan v1alpha1.Assignment

	mu      sync.Mutex
	current *v1alpha1.Assignment
}

func newAssignmentWaiter(stateDir string) *assignmentWaiter {
	return &assignmentWaiter{
		path: filepath.Join(stateDir, assignmentFile),
		ch:   make(chan v1alpha1.Assignment, 1),
	}
}

// deliver accepts an assignment. Delivering the same task again is a no-op,
// so the control plane can deliver on every resume; a different task is
// rejected because a sandbox runs exactly one task.
func (w *assignmentWaiter) deliver(a v1alpha1.Assignment) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.current != nil {
		if w.current.TaskYAML != a.TaskYAML {
			return errors.New("runner already has a different task assignment")
		}
		return nil
	}
	if err := w.persist(a); err != nil {
		return err
	}
	w.current = &a
	w.once.Do(func() { w.ch <- a })
	return nil
}

// persist writes the assignment atomically, readable only by the runner: it
// can carry spec.env values.
func (w *assignmentWaiter) persist(a v1alpha1.Assignment) error {
	data, err := json.Marshal(a)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(w.path), 0o700); err != nil {
		return fmt.Errorf("creating runner state dir: %w", err)
	}
	tmp := w.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("persisting assignment: %w", err)
	}
	return os.Rename(tmp, w.path)
}

// wait returns the persisted assignment when there is one, otherwise blocks
// until the control plane delivers it or ctx is done.
func (w *assignmentWaiter) wait(ctx context.Context) (*v1alpha1.Task, *v1alpha1.Workspace, error) {
	if data, err := os.ReadFile(w.path); err == nil {
		var a v1alpha1.Assignment
		if err := json.Unmarshal(data, &a); err != nil {
			return nil, nil, fmt.Errorf("reading persisted assignment: %w", err)
		}
		w.mu.Lock()
		if w.current == nil {
			w.current = &a
		}
		w.mu.Unlock()
		return decodeAssignment(a)
	}
	select {
	case a := <-w.ch:
		return decodeAssignment(a)
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func decodeAssignment(a v1alpha1.Assignment) (*v1alpha1.Task, *v1alpha1.Workspace, error) {
	task := &v1alpha1.Task{}
	if err := DecodeSpec([]byte(a.TaskYAML), task); err != nil {
		return nil, nil, err
	}
	if a.WorkspaceYAML == "" {
		return task, nil, nil
	}
	ws := &v1alpha1.Workspace{}
	if err := DecodeSpec([]byte(a.WorkspaceYAML), ws); err != nil {
		return nil, nil, err
	}
	return task, ws, nil
}
