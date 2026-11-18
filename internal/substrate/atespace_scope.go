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
	"strings"
	"sync"

	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// When several AX servers share one managed Substrate Instance (Managed AX),
// each must keep its sandboxes apart from the others. The AX server scopes
// every atespace it sends to a managed Substrate with its own Kubernetes
// namespace, which is unique per Managed AX instance:
// user atespace "default" -> Substrate atespace "<namespace>-default".

var (
	podNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	scopeOnce        sync.Once
	scopePrefix      string
)

// managedAtespacePrefix returns "<namespace>-" when running in a pod, else "".
func managedAtespacePrefix() string {
	scopeOnce.Do(func() {
		if b, err := os.ReadFile(podNamespaceFile); err == nil {
			if ns := strings.TrimSpace(string(b)); ns != "" {
				scopePrefix = ns + "-"
			}
		}
	})
	return scopePrefix
}

func scopeName(prefix, atespace string) string {
	if prefix == "" || atespace == "" || strings.HasPrefix(atespace, prefix) {
		return atespace
	}
	return prefix + atespace
}

// ScopeManagedTargetActor scopes an "atespace/actor" router target for a
// managed Substrate. It is a no-op outside a pod.
func ScopeManagedTargetActor(target string) string {
	atespace, actor, ok := strings.Cut(target, "/")
	if !ok {
		return target
	}
	return scopeName(managedAtespacePrefix(), atespace) + "/" + actor
}

// scopeMessage rewrites, in place, every atespace reference in m: string
// fields named "atespace", and the name of Atespace resources.
func scopeMessage(prefix string, m protoreflect.Message) {
	if prefix == "" || !m.IsValid() {
		return
	}
	isAtespace := m.Descriptor().FullName() == (&ateapipb.Atespace{}).ProtoReflect().Descriptor().FullName()
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.Kind() == protoreflect.StringKind && !fd.IsList() && !fd.IsMap() && fd.Name() == "atespace":
			m.Set(fd, protoreflect.ValueOfString(scopeName(prefix, v.String())))
		case fd.Kind() == protoreflect.MessageKind && fd.IsList():
			l := v.List()
			for i := 0; i < l.Len(); i++ {
				scopeMessage(prefix, l.Get(i).Message())
			}
		case fd.Kind() == protoreflect.MessageKind && !fd.IsMap():
			sub := v.Message()
			if isAtespace && fd.Name() == "metadata" {
				if md, ok := sub.Interface().(*ateapipb.ResourceMetadata); ok {
					md.Name = scopeName(prefix, md.GetName())
				}
			}
			scopeMessage(prefix, sub)
		}
		return true
	})
}

func scopeRequest(prefix string, req any) {
	if pm, ok := req.(proto.Message); ok {
		scopeMessage(prefix, pm.ProtoReflect())
	}
}

func atespaceScopeUnary(prefix string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		scopeRequest(prefix, req)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

type scopedStream struct {
	grpc.ClientStream
	prefix string
}

func (s *scopedStream) SendMsg(m any) error {
	scopeRequest(s.prefix, m)
	return s.ClientStream.SendMsg(m)
}

func atespaceScopeStream(prefix string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		cs, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}
		return &scopedStream{ClientStream: cs, prefix: prefix}, nil
	}
}

// atespaceScopeDialOptions returns interceptors that scope atespaces, or nil
// when there is nothing to scope.
func atespaceScopeDialOptions(prefix string) []grpc.DialOption {
	if prefix == "" {
		return nil
	}
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(atespaceScopeUnary(prefix)),
		grpc.WithChainStreamInterceptor(atespaceScopeStream(prefix)),
	}
}
