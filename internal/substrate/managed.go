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
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	defaultGCEMetadataIdentityURL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity"
	idTokenRefreshMargin          = 5 * time.Minute
	idTokenDefaultTTL             = 45 * time.Minute
)

// IsManagedTarget reports whether target (a host or host:port) addresses an
// external Substrate control plane or router endpoint rather than an in-cluster
// Kubernetes service, loopback address, or raw IP literal.
func IsManagedTarget(target string) bool {
	host := HostOnly(target)
	if host == "" || host == "localhost" || net.ParseIP(host) != nil {
		return false
	}
	if host == "api.ate-system.svc" || host == "atenet-router.ate-system.svc" {
		return false
	}
	return !strings.HasSuffix(host, ".svc") && !strings.HasSuffix(host, ".svc.cluster.local")
}

// HostOnly strips any scheme and port from target and returns the hostname.
func HostOnly(target string) string {
	s := strings.TrimSpace(target)
	if strings.Contains(s, "://") {
		if u, err := url.Parse(s); err == nil {
			return u.Hostname()
		}
	}
	if h, _, err := net.SplitHostPort(s); err == nil {
		return h
	}
	return s
}

// LoadManagedCAPool loads a PEM certificate bundle from SUBSTRATE_CA_PEM or
// SUBSTRATE_CA_FILE. It returns (nil, nil) when neither is set so callers fall
// back to system roots.
func LoadManagedCAPool() (*x509.CertPool, error) {
	if pemData := strings.TrimSpace(os.Getenv("SUBSTRATE_CA_PEM")); pemData != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(pemData)) {
			return nil, fmt.Errorf("failed to parse CA certificate from SUBSTRATE_CA_PEM")
		}
		return pool, nil
	}
	if caFile := strings.TrimSpace(os.Getenv("SUBSTRATE_CA_FILE")); caFile != "" {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("reading CA file %q: %w", caFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate from %q", caFile)
		}
		return pool, nil
	}
	return nil, nil
}

// IDTokenSource fetches and caches OIDC ID tokens from the GCE / Cloud Run
// metadata server for a single audience, refreshing before expiry.
type IDTokenSource struct {
	audience    string
	identityURL string
	httpClient  *http.Client

	mu     sync.Mutex
	token  string
	expiry time.Time
}

// metadataIdentityURL is the compute metadata server's ID token endpoint.
// Tests point it at a fake server.
var metadataIdentityURL = defaultGCEMetadataIdentityURL

// NewMetadataIDTokenSource creates an IDTokenSource for audience backed by the
// compute metadata server.
func NewMetadataIDTokenSource(audience string) *IDTokenSource {
	return &IDTokenSource{
		audience:    audience,
		identityURL: metadataIdentityURL,
		httpClient:  &http.Client{Timeout: 5 * time.Second},
	}
}

// Token returns a valid ID token, fetching a fresh one when missing or near expiry.
func (s *IDTokenSource) Token(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.token != "" && time.Now().Add(idTokenRefreshMargin).Before(s.expiry) {
		return s.token, nil
	}
	u, err := url.Parse(s.identityURL)
	if err != nil {
		return "", fmt.Errorf("parsing metadata identity URL: %w", err)
	}
	q := u.Query()
	q.Set("audience", s.audience)
	q.Set("format", "full")
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetching metadata ID token for %q: %w", s.audience, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return "", fmt.Errorf("reading metadata ID token: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metadata ID token status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	tok := strings.TrimSpace(string(body))
	if tok == "" {
		return "", fmt.Errorf("metadata server returned an empty ID token")
	}
	s.token = tok
	s.expiry = jwtExpiry(tok, time.Now().Add(idTokenDefaultTTL))
	return tok, nil
}

func jwtExpiry(tok string, fallback time.Time) time.Time {
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		return fallback
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return fallback
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Exp <= 0 {
		return fallback
	}
	return time.Unix(claims.Exp, 0)
}

var (
	sharedSourcesMu sync.Mutex
	sharedSources   = map[string]*IDTokenSource{}
)

// ManagedBearerToken resolves a bearer token for a managed Substrate endpoint:
// it reads SUBSTRATE_TOKEN_FILE when set (re-reading on every call so rotated
// files take effect immediately), and otherwise mints/caches an OIDC ID token
// from the compute metadata server for audience.
func ManagedBearerToken(ctx context.Context, audience string) (string, error) {
	if tokenFile := strings.TrimSpace(os.Getenv("SUBSTRATE_TOKEN_FILE")); tokenFile != "" {
		b, err := os.ReadFile(tokenFile)
		if err != nil {
			return "", fmt.Errorf("reading token file %q: %w", tokenFile, err)
		}
		return strings.TrimSpace(string(b)), nil
	}
	aud := HostOnly(audience)
	if aud == "" {
		return "", fmt.Errorf("empty audience for managed bearer token")
	}
	key := metadataIdentityURL + "|" + aud
	sharedSourcesMu.Lock()
	src, ok := sharedSources[key]
	if !ok {
		src = NewMetadataIDTokenSource(aud)
		sharedSources[key] = src
	}
	sharedSourcesMu.Unlock()
	return src.Token(ctx)
}

type metadataPerRPCAuth struct {
	source *IDTokenSource
}

func (m metadataPerRPCAuth) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	tok, err := m.source.Token(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]string{"authorization": "Bearer " + tok}, nil
}

func (m metadataPerRPCAuth) RequireTransportSecurity() bool {
	return true
}

// unsupportedSystemInfoVolume reports whether err is Substrate rejecting a
// template because its sandbox class does not support system_info volumes.
func unsupportedSystemInfoVolume(err error) bool {
	return status.Code(err) == codes.InvalidArgument &&
		strings.Contains(status.Convert(err).Message(), "system_info volumes are not supported")
}

// adaptTemplateForManagedSandbox returns a copy of tmpl for a sandbox class
// without system_info volume support (ACE microVMs on Managed Agent
// Substrate): it drops system_info volumes and their mounts, and uses
// SNAPSHOT_FIDELITY_MEMORY because restarting a VOLUMES snapshot from disk
// fails there ("Required field 'spec.bootImage' not specified").
func adaptTemplateForManagedSandbox(tmpl *ateapipb.ActorTemplate) *ateapipb.ActorTemplate {
	out := proto.Clone(tmpl).(*ateapipb.ActorTemplate)
	dropped := map[string]bool{}
	var volumes []*ateapipb.Volume
	for _, v := range out.GetVolumes() {
		if v.GetSystemInfo() != nil {
			dropped[v.GetName()] = true
			continue
		}
		volumes = append(volumes, v)
	}
	out.Volumes = volumes
	for _, c := range out.GetContainers() {
		var mounts []*ateapipb.VolumeMount
		for _, m := range c.GetVolumeMounts() {
			if !dropped[m.GetName()] {
				mounts = append(mounts, m)
			}
		}
		c.VolumeMounts = mounts
	}
	if out.GetSnapshotConfig() != nil {
		out.SnapshotConfig.PreferredFidelity = ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	}
	return out
}
