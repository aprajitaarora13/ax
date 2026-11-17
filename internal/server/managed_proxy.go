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
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	axmetadata "github.com/google/ax/internal/metadata"
	"github.com/google/ax/internal/substrate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// managedGuestProxy forwards guest gRPC calls (ax ssh) and
// non-default actor HTTP ports through a Managed Agent Substrate router's TLS
// HTTP CONNECT endpoint (:8444).
type managedGuestProxy struct {
	ateenvv1alpha.UnimplementedProcessServiceServer
	ateenvv1alpha.UnimplementedFileSystemServiceServer

	// dialContext overrides TCP dialing in unit tests.
	dialContext func(ctx context.Context, network, addr string) (net.Conn, error)
}

// dialActorPort opens a TLS connection to the Managed Substrate router's :8444
// listener and negotiates HTTP CONNECT to <atespace>/<actor>:<port>, returning
// a cleartext stream suitable for HTTP/1.1 or HTTP/2 (h2c / gRPC).
func (p *managedGuestProxy) dialActorPort(ctx context.Context, targetActor string, port int) (net.Conn, error) {
	connectAddr, sni, ok := resolveManagedConnectEndpoint()
	if !ok {
		return nil, errors.New("managed router CONNECT endpoint is not configured")
	}
	tok, err := substrate.ManagedBearerToken(ctx, sni)
	if err != nil {
		return nil, fmt.Errorf("obtaining router token: %w", err)
	}
	pool, err := substrate.LoadManagedCAPool()
	if err != nil {
		return nil, fmt.Errorf("loading router CA: %w", err)
	}

	actorName := targetActor
	if idx := strings.LastIndex(targetActor, "/"); idx >= 0 {
		actorName = targetActor[idx+1:]
	}
	authority := net.JoinHostPort(actorName, strconv.Itoa(port))

	dial := p.dialContext
	if dial == nil {
		var d net.Dialer
		d.Timeout = 30 * time.Second
		dial = d.DialContext
	}
	rawConn, err := dial(ctx, "tcp", connectAddr)
	if err != nil {
		return nil, err
	}
	tlsConn := tls.Client(rawConn, &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: sni,
		RootCAs:    pool,
	})
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		_ = rawConn.Close()
		return nil, err
	}
	connectReq := fmt.Sprintf(
		"CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Authorization: Bearer %s\r\nate-target-actor: %s\r\n\r\n",
		authority, authority, tok, targetActor,
	)
	if _, err := io.WriteString(tlsConn, connectReq); err != nil {
		_ = tlsConn.Close()
		return nil, err
	}
	br := bufio.NewReader(tlsConn)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
	if err != nil {
		_ = tlsConn.Close()
		return nil, err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_ = tlsConn.Close()
		return nil, fmt.Errorf("router CONNECT returned %s", resp.Status)
	}
	return &bufferedConn{Conn: tlsConn, r: br}, nil
}

// The managed router reaches microVM actors over HTTP/1.1 only, so guest
// gRPC calls are carried to the runner as framed HTTP/1.1 requests
// (metadata.HTTP1GRPCContentType) through the router's CONNECT endpoint.

func targetActorFrom(ctx context.Context) (string, error) {
	if _, _, ok := resolveManagedConnectEndpoint(); !ok {
		return "", status.Error(codes.Unimplemented, "managed router is not configured")
	}
	md, _ := metadata.FromIncomingContext(ctx)
	targets := md.Get("ate-target-actor")
	if len(targets) == 0 || strings.TrimSpace(targets[0]) == "" {
		return "", status.Error(codes.InvalidArgument, "missing ate-target-actor metadata")
	}
	return strings.TrimSpace(targets[0]), nil
}

// callFramed POSTs body to the runner's probe port for method and returns the
// streaming response body of framed messages.
func (p *managedGuestProxy) callFramed(ctx context.Context, method string, body io.Reader) (io.ReadCloser, error) {
	targetActor, err := targetActorFrom(ctx)
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return p.dialActorPort(ctx, targetActor, 80)
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://actor"+method, body)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	req.Header.Set("Content-Type", axmetadata.HTTP1GRPCContentType)
	resp, err := transport.RoundTrip(req)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "calling %s via managed router: %v", method, err)
	}
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		return nil, status.Errorf(codes.Unavailable, "%s via managed router: HTTP %d: %s", method, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return resp.Body, nil
}

// readFramed reads framed messages, calling onMsg for each data frame, until
// the status frame, which it returns as an error (nil for OK).
func readFramed(r io.Reader, onMsg func([]byte) error) error {
	for {
		kind, data, err := axmetadata.ReadGRPCFrame(r)
		if err != nil {
			return status.Errorf(codes.Unavailable, "reading guest response: %v", err)
		}
		if kind == 1 {
			return axmetadata.DecodeStatusFrame(data)
		}
		if err := onMsg(data); err != nil {
			return err
		}
	}
}

func singleFrame(m proto.Message) (io.Reader, error) {
	b, err := proto.Marshal(m)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	var buf bytes.Buffer
	if err := axmetadata.WriteGRPCFrame(&buf, 0, b); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &buf, nil
}

func unaryFramed[Resp proto.Message](p *managedGuestProxy, ctx context.Context, method string, req proto.Message, resp Resp) (Resp, error) {
	body, err := singleFrame(req)
	if err != nil {
		return resp, err
	}
	rc, err := p.callFramed(ctx, method, body)
	if err != nil {
		return resp, err
	}
	defer rc.Close()
	err = readFramed(rc, func(b []byte) error { return proto.Unmarshal(b, resp) })
	return resp, err
}

func serverStreamFramed[Resp any, PResp interface {
	*Resp
	proto.Message
}](p *managedGuestProxy, ctx context.Context, method string, req proto.Message, send func(PResp) error) error {
	body, err := singleFrame(req)
	if err != nil {
		return err
	}
	rc, err := p.callFramed(ctx, method, body)
	if err != nil {
		return err
	}
	defer rc.Close()
	return readFramed(rc, func(b []byte) error {
		msg := PResp(new(Resp))
		if err := proto.Unmarshal(b, msg); err != nil {
			return err
		}
		return send(msg)
	})
}

func clientStreamFramed[Req any, PReq interface {
	*Req
	proto.Message
}](p *managedGuestProxy, ctx context.Context, method string, recv func() (PReq, error), resp proto.Message) error {
	pr, pw := io.Pipe()
	go func() {
		for {
			msg, err := recv()
			if errors.Is(err, io.EOF) {
				_ = pw.Close()
				return
			}
			if err != nil {
				_ = pw.CloseWithError(err)
				return
			}
			b, err := proto.Marshal(msg)
			if err == nil {
				err = axmetadata.WriteGRPCFrame(pw, 0, b)
			}
			if err != nil {
				_ = pw.CloseWithError(err)
				return
			}
		}
	}()
	rc, err := p.callFramed(ctx, method, pr)
	if err != nil {
		_ = pr.CloseWithError(err)
		return err
	}
	defer rc.Close()
	return readFramed(rc, func(b []byte) error { return proto.Unmarshal(b, resp) })
}

func (p *managedGuestProxy) StartProcess(ctx context.Context, req *ateenvv1alpha.StartProcessRequest) (*ateenvv1alpha.Process, error) {
	return unaryFramed(p, ctx, ateenvv1alpha.ProcessService_StartProcess_FullMethodName, req, &ateenvv1alpha.Process{})
}

func (p *managedGuestProxy) GetProcess(ctx context.Context, req *ateenvv1alpha.GetProcessRequest) (*ateenvv1alpha.Process, error) {
	return unaryFramed(p, ctx, ateenvv1alpha.ProcessService_GetProcess_FullMethodName, req, &ateenvv1alpha.Process{})
}

func (p *managedGuestProxy) SignalProcess(ctx context.Context, req *ateenvv1alpha.SignalProcessRequest) (*ateenvv1alpha.Process, error) {
	return unaryFramed(p, ctx, ateenvv1alpha.ProcessService_SignalProcess_FullMethodName, req, &ateenvv1alpha.Process{})
}

func (p *managedGuestProxy) StreamProcessOutput(req *ateenvv1alpha.StreamProcessOutputRequest, srv grpc.ServerStreamingServer[ateenvv1alpha.ProcessOutput]) error {
	return serverStreamFramed[ateenvv1alpha.ProcessOutput](p, srv.Context(), ateenvv1alpha.ProcessService_StreamProcessOutput_FullMethodName, req, srv.Send)
}

func (p *managedGuestProxy) WriteProcessInput(srv grpc.ClientStreamingServer[ateenvv1alpha.WriteProcessInputRequest, ateenvv1alpha.WriteProcessInputResponse]) error {
	resp := &ateenvv1alpha.WriteProcessInputResponse{}
	if err := clientStreamFramed[ateenvv1alpha.WriteProcessInputRequest](p, srv.Context(), ateenvv1alpha.ProcessService_WriteProcessInput_FullMethodName, srv.Recv, resp); err != nil {
		return err
	}
	return srv.SendAndClose(resp)
}

func (p *managedGuestProxy) ReadFile(req *ateenvv1alpha.ReadFileRequest, srv grpc.ServerStreamingServer[ateenvv1alpha.ReadFileResponse]) error {
	return serverStreamFramed[ateenvv1alpha.ReadFileResponse](p, srv.Context(), ateenvv1alpha.FileSystemService_ReadFile_FullMethodName, req, srv.Send)
}

func (p *managedGuestProxy) WriteFile(srv grpc.ClientStreamingServer[ateenvv1alpha.WriteFileRequest, ateenvv1alpha.WriteFileResponse]) error {
	resp := &ateenvv1alpha.WriteFileResponse{}
	if err := clientStreamFramed[ateenvv1alpha.WriteFileRequest](p, srv.Context(), ateenvv1alpha.FileSystemService_WriteFile_FullMethodName, srv.Recv, resp); err != nil {
		return err
	}
	return srv.SendAndClose(resp)
}

// serveTaskPortConnect handles /v1alpha1/tasks/{atespace}/{name}/port/{port}[/{subpath...}]
// by opening a TLS HTTP CONNECT tunnel to the Managed Substrate router's :8444
// endpoint with Proxy-Authorization and ate-target-actor headers.
func (p *managedGuestProxy) serveTaskPortConnect(w http.ResponseWriter, r *http.Request) bool {
	const prefix = "/v1alpha1/tasks/"
	if !strings.HasPrefix(r.URL.Path, prefix) {
		return false
	}
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, prefix), "/", 5)
	if len(parts) < 4 || parts[2] != "port" || parts[0] == "" || parts[1] == "" {
		return false
	}
	atespace, actor := parts[0], parts[1]
	portNum, err := strconv.Atoi(parts[3])
	if err != nil || portNum < 1 || portNum > 65535 {
		http.Error(w, "invalid actor port", http.StatusBadRequest)
		return true
	}
	subpath := "/"
	if len(parts) == 5 && parts[4] != "" {
		subpath = "/" + parts[4]
	}
	if _, _, ok := resolveManagedConnectEndpoint(); !ok {
		http.Error(w, "managed router CONNECT endpoint is not configured", http.StatusNotImplemented)
		return true
	}

	authority := net.JoinHostPort(actor, strconv.Itoa(portNum))
	targetActor := atespace + "/" + actor
	transport := &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return p.dialActorPort(ctx, targetActor, portNum)
		},
	}

	rp := &httputil.ReverseProxy{
		Transport: transport,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(&url.URL{Scheme: "http", Host: authority})
			pr.Out.URL.Path = subpath
			pr.Out.URL.RawPath = ""
			pr.Out.URL.RawQuery = r.URL.RawQuery
			pr.Out.Host = authority
			pr.Out.Header.Del("Authorization")
		},
	}
	rp.ServeHTTP(w, r)
	return true
}

type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufferedConn) Read(b []byte) (int, error) {
	return c.r.Read(b)
}

func resolveManagedConnectEndpoint() (addr, sni string, ok bool) {
	router := strings.TrimSpace(os.Getenv("ATENET_ROUTER_ADDR"))
	if !substrate.IsManagedTarget(router) {
		return "", "", false
	}
	sni = substrate.HostOnly(router)
	return net.JoinHostPort(sni, "8444"), sni, true
}
