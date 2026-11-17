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

package main

import "testing"

func TestResolveEndpointDial(t *testing.T) {
	t.Setenv("AX_AUTH_TOKEN", "bearer-tok")

	target, tlsCfg, extra := resolveEndpointDial("https://ax-server.example.run.app")
	if target != "ax-server.example.run.app:443" {
		t.Errorf("HTTPS target = %q, want ax-server.example.run.app:443", target)
	}
	if tlsCfg == nil || tlsCfg.ServerName != "ax-server.example.run.app" {
		t.Errorf("HTTPS tlsCfg = %+v, want ServerName ax-server.example.run.app", tlsCfg)
	}
	if len(extra) != 1 {
		t.Errorf("HTTPS extra opts = %d, want 1 auth option", len(extra))
	}

	target, tlsCfg, _ = resolveEndpointDial("http://localhost:8080")
	if target != "localhost:8080" || tlsCfg != nil {
		t.Errorf("HTTP target = %q tlsCfg = %v, want localhost:8080 nil", target, tlsCfg)
	}
}
