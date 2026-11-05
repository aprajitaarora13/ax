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

package v1alpha1

// Assignment is what the control plane hands the runner at runtime: the Task
// launch configuration and its bound Workspace. It travels in the body of
// POST /v1alpha1/assignment on the runner's probe port instead of in the
// container environment, so the ActorTemplate carries no per-task data and
// can be shared, together with its warm snapshots, across tasks.
type Assignment struct {
	// TaskYAML is the Task, without status, as YAML.
	TaskYAML string `json:"taskYaml"`
	// WorkspaceYAML is the bound Workspace as YAML; empty when the task binds none.
	WorkspaceYAML string `json:"workspaceYaml,omitempty"`
}
