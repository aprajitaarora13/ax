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

package substrate

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsManagedTarget(t *testing.T) {
	tests := []struct {
		target string
		want   bool
	}{
		{target: "as-0123456789ab-123456789012.us-central1.agentsubstrate.goog:443", want: true},
		{target: "https://as-0123456789ab-123456789012.us-central1.agentsubstrate.goog:8443", want: true},
		{target: "substrate.example.com:443", want: true},
		{target: "api.ate-system.svc.cluster.local:443", want: false},
		{target: "api.ate-system.svc", want: false},
		{target: "atenet-router.ate-system.svc", want: false},
		{target: "10.128.0.24:443", want: false},
		{target: "localhost:8080", want: false},
		{target: "", want: false},
	}
	for _, tt := range tests {
		if got := IsManagedTarget(tt.target); got != tt.want {
			t.Errorf("IsManagedTarget(%q) = %v, want %v", tt.target, got, tt.want)
		}
	}
}

func TestLoadManagedCAPool(t *testing.T) {
	caPEM := selfSignedCAPEM(t)

	t.Setenv("SUBSTRATE_CA_PEM", "")
	t.Setenv("SUBSTRATE_CA_FILE", "")
	pool, err := LoadManagedCAPool()
	if err != nil || pool != nil {
		t.Fatalf("LoadManagedCAPool() empty = (%v, %v), want (nil, nil)", pool, err)
	}

	t.Setenv("SUBSTRATE_CA_PEM", string(caPEM))
	if pool, err := LoadManagedCAPool(); err != nil || pool == nil {
		t.Fatalf("LoadManagedCAPool() from SUBSTRATE_CA_PEM = (%v, %v), want non-nil pool", pool, err)
	}

	t.Setenv("SUBSTRATE_CA_PEM", "")
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SUBSTRATE_CA_FILE", caPath)
	if pool, err := LoadManagedCAPool(); err != nil || pool == nil {
		t.Fatalf("LoadManagedCAPool() from SUBSTRATE_CA_FILE = (%v, %v), want non-nil pool", pool, err)
	}
}

func TestIDTokenSourceCachesAndRefreshes(t *testing.T) {
	var calls atomic.Int32
	meta := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			http.Error(w, "missing Metadata-Flavor", http.StatusForbidden)
			return
		}
		if r.URL.Query().Get("audience") != "substrate.example.com" || r.URL.Query().Get("format") != "full" {
			http.Error(w, "bad query", http.StatusBadRequest)
			return
		}
		n := calls.Add(1)
		exp := time.Now().Add(time.Hour).Unix()
		if n == 1 {
			// Near expiry so the second call refreshes immediately.
			exp = time.Now().Add(time.Minute).Unix()
		}
		_, _ = fmt.Fprint(w, fakeJWT(t, fmt.Sprintf("tok-%d", n), exp))
	}))
	defer meta.Close()

	prev := metadataIdentityURL
	metadataIdentityURL = meta.URL
	t.Cleanup(func() { metadataIdentityURL = prev })
	t.Setenv("SUBSTRATE_TOKEN_FILE", "")

	src := NewMetadataIDTokenSource("substrate.example.com")
	tok1, err := src.Token(context.Background())
	if err != nil {
		t.Fatalf("first Token() error: %v", err)
	}
	tok2, err := src.Token(context.Background())
	if err != nil {
		t.Fatalf("second Token() error: %v", err)
	}
	if tok1 == tok2 {
		t.Errorf("near-expiry token was not refreshed: both calls returned %q", tok1)
	}
	tok3, err := src.Token(context.Background())
	if err != nil {
		t.Fatalf("third Token() error: %v", err)
	}
	if tok3 != tok2 || calls.Load() != 2 {
		t.Errorf("fresh token was not cached: calls=%d, tok2=%q, tok3=%q", calls.Load(), tok2, tok3)
	}
}

func TestManagedBearerTokenPrefersTokenFile(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("rotated-1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SUBSTRATE_TOKEN_FILE", tokenFile)
	got, err := ManagedBearerToken(context.Background(), "substrate.example.com")
	if err != nil || got != "rotated-1" {
		t.Fatalf("ManagedBearerToken() = (%q, %v), want (%q, nil)", got, err, "rotated-1")
	}
	if err := os.WriteFile(tokenFile, []byte("rotated-2"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err = ManagedBearerToken(context.Background(), "substrate.example.com")
	if err != nil || got != "rotated-2" {
		t.Fatalf("ManagedBearerToken() after rotation = (%q, %v), want (%q, nil)", got, err, "rotated-2")
	}
}

func TestUnsupportedSystemInfoVolume(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "microvm sandbox rejection",
			err:  status.Error(codes.InvalidArgument, `actor_template.volumes[1].system_info: Forbidden: system_info volumes are not supported for sandbox_class "ace"`),
			want: true,
		},
		{name: "other invalid argument", err: status.Error(codes.InvalidArgument, "bad image"), want: false},
		{name: "other code", err: status.Error(codes.Unavailable, "system_info volumes are not supported"), want: false},
		{name: "nil", err: nil, want: false},
		{name: "non-status", err: errors.New("system_info volumes are not supported"), want: false},
	}
	for _, tt := range tests {
		if got := unsupportedSystemInfoVolume(tt.err); got != tt.want {
			t.Errorf("%s: unsupportedSystemInfoVolume() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestAdaptTemplateForManagedSandbox(t *testing.T) {
	tmpl := BuildActorTemplate("ns", "tmpl", "img@sha256:abc", nil, nil, "", nil)

	got := adaptTemplateForManagedSandbox(tmpl)
	for _, v := range got.GetVolumes() {
		if v.GetSystemInfo() != nil {
			t.Errorf("adaptTemplateForManagedSandbox() kept system_info volume %q", v.GetName())
		}
	}
	for _, m := range got.GetContainers()[0].GetVolumeMounts() {
		if m.GetName() == "egress-trust-bundle" {
			t.Errorf("adaptTemplateForManagedSandbox() kept mount for dropped volume %q", m.GetName())
		}
	}
	if got.GetSnapshotConfig().GetPreferredFidelity() != ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY {
		t.Errorf("PreferredFidelity = %v, want SNAPSHOT_FIDELITY_MEMORY", got.GetSnapshotConfig().GetPreferredFidelity())
	}
	if tmpl.GetSnapshotConfig().GetPreferredFidelity() != ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES {
		t.Error("adaptTemplateForManagedSandbox() mutated input template")
	}

}

func fakeJWT(t *testing.T, id string, exp int64) string {
	t.Helper()
	hdr := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`))
	body, err := json.Marshal(map[string]any{"jti": id, "exp": exp})
	if err != nil {
		t.Fatal(err)
	}
	return hdr + "." + base64.RawURLEncoding.EncodeToString(body) + ".sig"
}

func selfSignedCAPEM(t *testing.T) []byte {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
