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
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/encoding/protojson"
)

// Harness contract: the runner turns the task into files, never into
// container environment, and points the harness at them.
const (
	// GoalFileEnv names the file holding spec.config.prompt, the task's goal.
	GoalFileEnv = "AX_GOAL_FILE"
	// ConfigFileEnv names the file holding spec.config as JSON.
	ConfigFileEnv = "AX_CONFIG_FILE"

	goalFileName   = "goal.md"
	configFileName = "config.json"
)

// writeHarnessFiles writes the task's goal and config under dir and returns
// the environment entries that point the harness at them.
func writeHarnessFiles(dir string, task *v1alpha1.Task) ([]string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating %s: %w", dir, err)
	}
	config := task.GetSpec().GetConfig()
	configJSON := []byte("{}")
	if config != nil {
		b, err := protojson.Marshal(config)
		if err != nil {
			return nil, fmt.Errorf("encoding spec.config: %w", err)
		}
		configJSON = b
	}
	goal := config.GetFields()["prompt"].GetStringValue()

	goalPath := filepath.Join(dir, goalFileName)
	configPath := filepath.Join(dir, configFileName)
	if err := os.WriteFile(goalPath, []byte(goal), 0o644); err != nil {
		return nil, fmt.Errorf("writing goal: %w", err)
	}
	if err := os.WriteFile(configPath, configJSON, 0o644); err != nil {
		return nil, fmt.Errorf("writing config: %w", err)
	}
	return []string{GoalFileEnv + "=" + goalPath, ConfigFileEnv + "=" + configPath}, nil
}
