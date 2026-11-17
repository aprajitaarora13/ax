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

package metadata

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// HTTP1GRPCContentType identifies framed guest gRPC calls carried over HTTP/1.1
// when an upstream ingress proxy (for example microVM session ingress) speaks
// HTTP/1.1 rather than HTTP/2 to the actor's probe port.
const HTTP1GRPCContentType = "application/x-ax-grpc-http1"

const (
	frameData   byte = 0
	frameStatus byte = 1
	maxFrameLen      = 16 << 20
)

// WriteGRPCFrame writes one framed payload (kind 0 = message, kind 1 = status).
func WriteGRPCFrame(w io.Writer, kind byte, payload []byte) error {
	var hdr [5]byte
	hdr[0] = kind
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	return nil
}

// ReadGRPCFrame reads the next framed payload, returning io.EOF at clean end-of-stream.
func ReadGRPCFrame(r io.Reader) (byte, []byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[1:])
	if n > maxFrameLen {
		return 0, nil, fmt.Errorf("frame size %d exceeds limit %d", n, maxFrameLen)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, nil, err
	}
	return hdr[0], buf, nil
}

func writeStatusFrame(w io.Writer, err error) {
	if err == nil {
		_ = WriteGRPCFrame(w, frameStatus, nil)
		return
	}
	b, mErr := proto.Marshal(status.Convert(err).Proto())
	if mErr != nil {
		b, _ = proto.Marshal(status.New(codes.Internal, err.Error()).Proto())
	}
	_ = WriteGRPCFrame(w, frameStatus, b)
}

// DecodeStatusFrame converts a frameStatus payload back into a gRPC error.
func DecodeStatusFrame(payload []byte) error {
	if len(payload) == 0 {
		return nil
	}
	var st spb.Status
	if err := proto.Unmarshal(payload, &st); err != nil {
		return status.Errorf(codes.Internal, "decoding status frame: %v", err)
	}
	return status.ErrorProto(&st)
}

func (s *Server) ensureLoopbackGRPCClient() (*grpc.ClientConn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.grpcLoopbackConn != nil {
		return s.grpcLoopbackConn, nil
	}
	if s.grpcLoopbackAddr == "" {
		return nil, status.Error(codes.Unavailable, "guest gRPC loopback listener is not running")
	}
	conn, err := grpc.NewClient(s.grpcLoopbackAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	s.grpcLoopbackConn = conn
	return conn, nil
}

func (s *Server) handleHTTP1GRPC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || !strings.HasPrefix(r.URL.Path, "/ateenv.v1alpha.") {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", HTTP1GRPCContentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)

	conn, err := s.ensureLoopbackGRPCClient()
	if err != nil {
		writeStatusFrame(w, err)
		return
	}
	ctx := r.Context()
	proc := ateenvv1alpha.NewProcessServiceClient(conn)
	fs := ateenvv1alpha.NewFileSystemServiceClient(conn)

	switch r.URL.Path {
	case ateenvv1alpha.ProcessService_StartProcess_FullMethodName:
		serveUnary(w, r.Body, &ateenvv1alpha.StartProcessRequest{}, func(req *ateenvv1alpha.StartProcessRequest) (proto.Message, error) {
			return proc.StartProcess(ctx, req)
		})
	case ateenvv1alpha.ProcessService_GetProcess_FullMethodName:
		serveUnary(w, r.Body, &ateenvv1alpha.GetProcessRequest{}, func(req *ateenvv1alpha.GetProcessRequest) (proto.Message, error) {
			return proc.GetProcess(ctx, req)
		})
	case ateenvv1alpha.ProcessService_SignalProcess_FullMethodName:
		serveUnary(w, r.Body, &ateenvv1alpha.SignalProcessRequest{}, func(req *ateenvv1alpha.SignalProcessRequest) (proto.Message, error) {
			return proc.SignalProcess(ctx, req)
		})
	case ateenvv1alpha.ProcessService_StreamProcessOutput_FullMethodName:
		var req ateenvv1alpha.StreamProcessOutputRequest
		if err := readSingleMessage(r.Body, &req); err != nil {
			writeStatusFrame(w, err)
			return
		}
		stream, err := proc.StreamProcessOutput(ctx, &req)
		if err != nil {
			writeStatusFrame(w, err)
			return
		}
		for {
			msg, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				writeStatusFrame(w, nil)
				return
			}
			if err != nil {
				writeStatusFrame(w, err)
				return
			}
			b, err := proto.Marshal(msg)
			if err != nil {
				writeStatusFrame(w, err)
				return
			}
			if err := WriteGRPCFrame(w, frameData, b); err != nil {
				return
			}
		}
	case ateenvv1alpha.ProcessService_WriteProcessInput_FullMethodName:
		stream, err := proc.WriteProcessInput(ctx)
		if err != nil {
			writeStatusFrame(w, err)
			return
		}
		for {
			kind, data, err := ReadGRPCFrame(r.Body)
			if errors.Is(err, io.EOF) {
				resp, err := stream.CloseAndRecv()
				if err != nil {
					writeStatusFrame(w, err)
					return
				}
				b, _ := proto.Marshal(resp)
				_ = WriteGRPCFrame(w, frameData, b)
				writeStatusFrame(w, nil)
				return
			}
			if err != nil {
				writeStatusFrame(w, status.Errorf(codes.InvalidArgument, "reading input frame: %v", err))
				return
			}
			if kind != frameData {
				continue
			}
			var msg ateenvv1alpha.WriteProcessInputRequest
			if err := proto.Unmarshal(data, &msg); err != nil {
				writeStatusFrame(w, status.Errorf(codes.InvalidArgument, "decoding input frame: %v", err))
				return
			}
			if err := stream.Send(&msg); err != nil {
				writeStatusFrame(w, err)
				return
			}
		}
	case ateenvv1alpha.FileSystemService_ReadFile_FullMethodName:
		var req ateenvv1alpha.ReadFileRequest
		if err := readSingleMessage(r.Body, &req); err != nil {
			writeStatusFrame(w, err)
			return
		}
		stream, err := fs.ReadFile(ctx, &req)
		if err != nil {
			writeStatusFrame(w, err)
			return
		}
		for {
			msg, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				writeStatusFrame(w, nil)
				return
			}
			if err != nil {
				writeStatusFrame(w, err)
				return
			}
			b, err := proto.Marshal(msg)
			if err != nil {
				writeStatusFrame(w, err)
				return
			}
			if err := WriteGRPCFrame(w, frameData, b); err != nil {
				return
			}
		}
	case ateenvv1alpha.FileSystemService_WriteFile_FullMethodName:
		stream, err := fs.WriteFile(ctx)
		if err != nil {
			writeStatusFrame(w, err)
			return
		}
		for {
			kind, data, err := ReadGRPCFrame(r.Body)
			if errors.Is(err, io.EOF) {
				resp, err := stream.CloseAndRecv()
				if err != nil {
					writeStatusFrame(w, err)
					return
				}
				b, _ := proto.Marshal(resp)
				_ = WriteGRPCFrame(w, frameData, b)
				writeStatusFrame(w, nil)
				return
			}
			if err != nil {
				writeStatusFrame(w, status.Errorf(codes.InvalidArgument, "reading file frame: %v", err))
				return
			}
			if kind != frameData {
				continue
			}
			var msg ateenvv1alpha.WriteFileRequest
			if err := proto.Unmarshal(data, &msg); err != nil {
				writeStatusFrame(w, status.Errorf(codes.InvalidArgument, "decoding file frame: %v", err))
				return
			}
			if err := stream.Send(&msg); err != nil {
				writeStatusFrame(w, err)
				return
			}
		}
	default:
		writeStatusFrame(w, status.Errorf(codes.Unimplemented, "unknown method %q", r.URL.Path))
	}
}

func readSingleMessage(r io.Reader, out proto.Message) error {
	kind, data, err := ReadGRPCFrame(r)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "reading request frame: %v", err)
	}
	if kind != frameData {
		return status.Errorf(codes.InvalidArgument, "unexpected frame kind %d", kind)
	}
	if err := proto.Unmarshal(data, out); err != nil {
		return status.Errorf(codes.InvalidArgument, "decoding request frame: %v", err)
	}
	return nil
}

func serveUnary[Req proto.Message](w io.Writer, r io.Reader, req Req, fn func(Req) (proto.Message, error)) {
	if err := readSingleMessage(r, req); err != nil {
		writeStatusFrame(w, err)
		return
	}
	resp, err := fn(req)
	if err != nil {
		writeStatusFrame(w, err)
		return
	}
	b, err := proto.Marshal(resp)
	if err != nil {
		writeStatusFrame(w, err)
		return
	}
	_ = WriteGRPCFrame(w, frameData, b)
	writeStatusFrame(w, nil)
}
