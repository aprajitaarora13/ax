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

package substrate

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
)

func TestScopeMessageRewritesAllAtespaceReferences(t *testing.T) {
	const p = "axi-demo-abc-"
	get := &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "default", Name: "a"}}
	scopeRequest(p, get)
	if got := get.GetActor().GetAtespace(); got != p+"default" {
		t.Errorf("GetActor atespace = %q", got)
	}
	scopeRequest(p, get) // idempotent
	if got := get.GetActor().GetAtespace(); got != p+"default" {
		t.Errorf("rescoped atespace = %q", got)
	}
	list := &ateapipb.ListActorsRequest{Atespace: "team"}
	scopeRequest(p, list)
	if list.GetAtespace() != p+"team" {
		t.Errorf("ListActors atespace = %q", list.GetAtespace())
	}

	create := &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "default"}}}
	scopeRequest(p, create)
	if got := create.GetAtespace().GetMetadata().GetName(); got != p+"default" {
		t.Errorf("CreateAtespace name = %q", got)
	}

	actor := &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "default", Name: "task"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "ax-system", Name: "tmpl"},
		SourceTag:     &ateapipb.ObjectRef{Atespace: "default", Name: "tag"},
	}}
	scopeRequest(p, actor)
	a := actor.GetActor()
	if a.GetMetadata().GetAtespace() != p+"default" || a.GetActorTemplate().GetAtespace() != p+"ax-system" ||
		a.GetSourceTag().GetAtespace() != p+"default" {
		t.Errorf("nested refs not scoped: %v", a)
	}
	if a.GetMetadata().GetName() != "task" {
		t.Errorf("actor name changed: %q", a.GetMetadata().GetName())
	}
}

func TestScopeNoPrefixIsNoop(t *testing.T) {
	req := &ateapipb.ListActorsRequest{Atespace: "default"}
	scopeRequest("", req)
	if req.GetAtespace() != "default" {
		t.Errorf("atespace = %q", req.GetAtespace())
	}
	if atespaceScopeDialOptions("") != nil {
		t.Error("expected no dial options without a prefix")
	}
}

func TestUnaryInterceptorScopesBeforeSend(t *testing.T) {
	var sent string
	invoker := func(_ context.Context, _ string, req, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		sent = req.(*ateapipb.ListActorsRequest).GetAtespace()
		return nil
	}
	err := atespaceScopeUnary("ns-")(context.Background(), "/m", &ateapipb.ListActorsRequest{Atespace: "default"}, nil, nil, invoker)
	if err != nil || sent != "ns-default" {
		t.Errorf("sent %q, err %v", sent, err)
	}
}

func TestScopeManagedTargetActorFromPodNamespace(t *testing.T) {
	f := filepath.Join(t.TempDir(), "namespace")
	if err := os.WriteFile(f, []byte("axi-demo-abc\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	oldFile := podNamespaceFile
	podNamespaceFile, scopeOnce, scopePrefix = f, sync.Once{}, ""
	t.Cleanup(func() { podNamespaceFile, scopeOnce, scopePrefix = oldFile, sync.Once{}, "" })

	if got := ScopeManagedTargetActor("default/demo-agent"); got != "axi-demo-abc-default/demo-agent" {
		t.Errorf("got %q", got)
	}
	if got := ScopeManagedTargetActor("no-slash"); got != "no-slash" {
		t.Errorf("got %q", got)
	}
}
