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

// Command ax-managed-api is a prototype Managed AX control plane. It serves a
// Google Cloud style AxInstance API and provisions each instance in a provider
// GKE cluster, backed by a Managed Agent Substrate Instance (ATE on ACE).
//
// This is prototype code: the provider environment is hardcoded below.
package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/google/ax/internal/managedapi"
)

const (
	listenAddr      = ":8090"
	providerProject = "aprajitaarora-gke-dev-2"
	registry        = "us-central1-docker.pkg.dev/" + providerProject + "/managed-ax"
	engineInstance  = "projects/" + providerProject + "/locations/us-south1/instances/managed-ax-engine"
	engineHost      = "as-32ca5d631cfc-837016535793.us-south1.staging.agentsubstrate.goog"
	engineIP        = "10.128.0.122"
	stateDir        = ".ax-managed-api"
)

func main() {
	home, err := os.UserHomeDir()
	if err != nil {
		log.Fatal(err)
	}
	dir := filepath.Join(home, stateDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		log.Fatal(err)
	}
	ca, err := os.ReadFile(filepath.Join(dir, "engine-ca.pem"))
	if err != nil {
		log.Fatalf("read engine CA (Instance.ca_certificate): %v", err)
	}
	prov := managedapi.NewKubeProvisioner(managedapi.ProviderConfig{
		ProviderProject:   providerProject,
		Kubeconfig:        filepath.Join(dir, "kubeconfig"),
		SubstrateInstance: engineInstance,
		EngineHost:        engineHost,
		EngineIP:          engineIP,
		EngineCA:          string(ca),
		CallerGSA:         "ax-engine-caller@" + providerProject + ".iam.gserviceaccount.com",
		ServerImage:       registry + "/ax-server@sha256:71f2c1d9786fa6983b6c80d9a754fc21dbe101ea414b7d70bffc0dffae7a2bd8",
		RunnerImage:       registry + "/ax-task-runner@sha256:60849a7ce9d150af6d7a7402938f01fb42aea8398505e1a1790aa6e3b36b2214",
		EnvoyImage:        registry + "/envoy@sha256:6ed0d4f28b8122df896062c425b34f18b8287e8c71c6badb3b84ca2e2f47c519",
		PostgresImage:     registry + "/postgres@sha256:a2f7e20374cd8fd8ed4a588b79549e10589bea7b1686e77922f5ac741b80b8a1",
		Network:           "default",
		Region:            "us-central1",
		NATSupernet:       "192.168.0.0/16",
		EndpointDomain:    "us-central1.managed-ax.internal",
	})
	srv := managedapi.NewServer(prov, &managedapi.IAMAuthorizer{}, filepath.Join(dir, "state.json"))
	log.Printf("Managed AX API (v1alpha) listening on %s", listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, srv.Handler()))
}
