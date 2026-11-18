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

// Package managedapi is a prototype control plane for "Managed AX": a Google
// Cloud style resource API (AxInstance) that provisions a dedicated AX server
// for a customer project, backed by a Managed Agent Substrate Instance, and
// exposes it to the customer over Private Service Connect.
//
// The API mirrors how agentsubstrate.googleapis.com exposes Substrate: the
// control API only creates the resource and returns an endpoint; the work API
// (AX's own ax.v1alpha1 gRPC API) is served at that endpoint.
package managedapi

import "time"

// AxInstance states.
const (
	StateCreating = "CREATING"
	StateActive   = "ACTIVE"
	StateDeleting = "DELETING"
	StateFailed   = "FAILED"
)

// AxInstance is the customer-facing resource:
// projects/{project}/locations/{location}/axInstances/{axInstance}.
type AxInstance struct {
	Name       string    `json:"name"`
	UID        string    `json:"uid"`
	State      string    `json:"state"`
	CreateTime time.Time `json:"createTime"`
	UpdateTime time.Time `json:"updateTime"`
	// AxEndpoint is host:port of the AX API (ax.v1alpha1) for this instance.
	AxEndpoint string `json:"axEndpoint,omitempty"`
	// PrivateServiceConnectConfig is where the customer connects a PSC
	// endpoint to reach AxEndpoint.
	PrivateServiceConnectConfig *PSCConfig `json:"privateServiceConnectConfig,omitempty"`
	// CaCertificate is the PEM CA that signs the AxEndpoint TLS certificate.
	CaCertificate string `json:"caCertificate,omitempty"`
	// AllowedPrincipalSuffix is the email suffix of callers allowed on
	// AxEndpoint (service accounts of the parent project).
	AllowedPrincipalSuffix string `json:"allowedPrincipalSuffix,omitempty"`
	// SubstrateInstance is the Managed Agent Substrate Instance that runs this
	// AxInstance's sandboxes on ACE microVMs.
	SubstrateInstance string `json:"substrateInstance,omitempty"`
	ErrorMessage      string `json:"errorMessage,omitempty"`
}

// PSCConfig describes the producer side of Private Service Connect.
type PSCConfig struct {
	ServiceAttachment string `json:"serviceAttachment"`
}

// Operation is a google.longrunning.Operation-shaped LRO.
type Operation struct {
	Name     string            `json:"name"`
	Metadata OperationMetadata `json:"metadata"`
	Done     bool              `json:"done"`
	Error    *Status           `json:"error,omitempty"`
	Response *AxInstance       `json:"response,omitempty"`
}

// OperationMetadata mirrors common OperationMetadata fields.
type OperationMetadata struct {
	Type       string     `json:"@type"`
	CreateTime time.Time  `json:"createTime"`
	EndTime    *time.Time `json:"endTime,omitempty"`
	Target     string     `json:"target"`
	Verb       string     `json:"verb"`
}

// Status is a google.rpc.Status-shaped error.
type Status struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Status  string `json:"status,omitempty"`
}

// ProvisionRequest is what the server asks the Provisioner to build.
type ProvisionRequest struct {
	Project  string // customer (parent) project ID
	Location string
	ID       string // axInstance ID
	UID      string
}

// ProvisionResult is what the Provisioner returns once the instance serves.
type ProvisionResult struct {
	AxEndpoint        string
	ServiceAttachment string
	CaCertificate     string
	SubstrateInstance string
}
