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

package server

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"github.com/google/ax/internal/controller"
	axmetadata "github.com/google/ax/internal/metadata"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate/substratetest"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// TestManagedGuestProxyRunsProcessOverHTTP1 runs a real runner probe server
// (guest services on) behind a stand-in for the router's :8444 CONNECT
// endpoint, and drives StartProcess + StreamProcessOutput through ax-server's
// guest proxy, which must carry them as framed HTTP/1.1 because the managed
// router does not reach microVM actors over HTTP/2.
func TestManagedGuestProxyRunsProcessOverHTTP1(t *testing.T) {
	cert, caPEM := generateTestCert(t, "router.example.com")
	t.Setenv("SUBSTRATE_CA_PEM", string(caPEM))
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("edge-jwt"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SUBSTRATE_TOKEN_FILE", tokenFile)
	t.Setenv("ATENET_ROUTER_ADDR", "https://router.example.com:8443")

	runnerTask := &v1alpha1.Task{
		ApiVersion: v1alpha1.APIVersion,
		Kind:       v1alpha1.KindTask,
		Metadata:   &v1alpha1.ObjectMeta{Name: "task-1", Atespace: "default"},
		Spec:       &v1alpha1.TaskSpec{Debug: true},
	}
	runner := axmetadata.NewServer(0, runnerTask, nil, axmetadata.ServerOptions{WorkspacePath: t.TempDir(), LogDir: t.TempDir()})
	if err := runner.Start(); err != nil {
		t.Fatalf("runner Start() failed: %v", err)
	}
	t.Cleanup(func() { _ = runner.Stop(context.Background()) })

	connectLis, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	defer connectLis.Close()
	var (
		hdrMu    sync.Mutex
		gotAuth  string
		gotActor string
		gotHost  string
	)
	go func() {
		for {
			conn, err := connectLis.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				br := bufio.NewReader(c)
				req, err := http.ReadRequest(br)
				if err != nil || req.Method != http.MethodConnect {
					return
				}
				hdrMu.Lock()
				gotAuth = req.Header.Get("Proxy-Authorization")
				gotActor = req.Header.Get("ate-target-actor")
				gotHost = req.Host
				hdrMu.Unlock()
				up, err := net.Dial("tcp", runner.ProbeAddr())
				if err != nil {
					return
				}
				defer up.Close()
				_, _ = io.WriteString(c, "HTTP/1.1 200 Connection Established\r\n\r\n")
				done := make(chan struct{}, 2)
				go func() { _, _ = io.Copy(up, br); done <- struct{}{} }()
				go func() { _, _ = io.Copy(c, up); done <- struct{}{} }()
				<-done
			}(conn)
		}
	}()

	sub := substratetest.NewServer(t)
	reconciler := controller.NewTaskReconciler(sub.Client(), "", "", "")
	srv := NewServer(memory.NewStore(), reconciler)
	var d net.Dialer
	srv.guestProxy.dialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return d.DialContext(ctx, network, connectLis.Addr().String())
	}
	axLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer axLis.Close()
	go func() { _ = srv.GRPCServer().Serve(axLis) }()
	defer srv.GRPCServer().Stop()

	conn, err := grpc.NewClient(axLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := metadata.AppendToOutgoingContext(context.Background(), "ate-target-actor", "default/task-1")
	procClient := ateenvv1alpha.NewProcessServiceClient(conn)

	proc, err := procClient.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{Command: []string{"echo", "hello from microvm"}})
	if err != nil {
		t.Fatalf("StartProcess() error: %v", err)
	}
	hdrMu.Lock()
	auth, actor, host := gotAuth, gotActor, gotHost
	hdrMu.Unlock()
	if auth != "Bearer edge-jwt" || actor != "default/task-1" || host != "task-1:80" {
		t.Fatalf("CONNECT headers auth=%q actor=%q host=%q", auth, actor, host)
	}

	stream, err := procClient.StreamProcessOutput(ctx, &ateenvv1alpha.StreamProcessOutputRequest{ProcessId: proc.GetProcessId(), Follow: true})
	if err != nil {
		t.Fatalf("StreamProcessOutput() error: %v", err)
	}
	var out strings.Builder
	exitCode := int32(-1)
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv() error: %v", err)
		}
		out.Write(msg.GetStdout())
		if e := msg.GetExit(); e != nil {
			exitCode = e.GetExitCode()
		}
	}
	if out.String() != "hello from microvm\n" || exitCode != 0 {
		t.Fatalf("output %q exit %d, want %q exit 0", out.String(), exitCode, "hello from microvm\n")
	}
}

func TestManagedGuestProxyPortConnectTunnel(t *testing.T) {
	cert, caPEM := generateTestCert(t, "router.example.com")
	t.Setenv("SUBSTRATE_CA_PEM", string(caPEM))
	t.Setenv("ATENET_ROUTER_ADDR", "https://router.example.com:8443")

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("connect-jwt"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SUBSTRATE_TOKEN_FILE", tokenFile)

	connectLis, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	defer connectLis.Close()
	go func() {
		conn, err := connectLis.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		br := bufio.NewReader(conn)
		req, err := http.ReadRequest(br)
		if err != nil {
			return
		}
		if req.Method != http.MethodConnect || req.Host != "task-1:8085" ||
			req.Header.Get("Proxy-Authorization") != "Bearer connect-jwt" ||
			req.Header.Get("ate-target-actor") != "default/task-1" {
			_, _ = io.WriteString(conn, "HTTP/1.1 403 Forbidden\r\n\r\n")
			return
		}
		_, _ = io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n")

		innerReq, err := http.ReadRequest(br)
		if err != nil {
			return
		}
		body := fmt.Sprintf("actor-8085:%s?%s", innerReq.URL.Path, innerReq.URL.RawQuery)
		_, _ = fmt.Fprintf(conn, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(body), body)
	}()

	sub := substratetest.NewServer(t)
	reconciler := controller.NewTaskReconciler(sub.Client(), "", "", "")
	srv := NewServer(memory.NewStore(), reconciler)
	var d net.Dialer
	srv.guestProxy.dialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return d.DialContext(ctx, network, connectLis.Addr().String())
	}

	axHTTP := httptest.NewServer(srv.Handler())
	defer axHTTP.Close()

	resp, err := http.Get(axHTTP.URL + "/v1alpha1/tasks/default/task-1/port/8085/status?turn=1")
	if err != nil {
		t.Fatalf("GET port connect bridge failed: %v", err)
	}
	defer resp.Body.Close()
	got, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || string(got) != "actor-8085:/status?turn=1" {
		t.Fatalf("status=%d body=%q, want 200 %q", resp.StatusCode, string(got), "actor-8085:/status?turn=1")
	}
}

func generateTestCert(t *testing.T, dnsName string) (tls.Certificate, []byte) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: dnsName},
		DNSNames:              []string{dnsName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyBytes, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})
	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return tlsCert, certPEM
}
