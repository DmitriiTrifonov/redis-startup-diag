// Package grpcdiag exposes pkg/redisdiag over gRPC, for services that only
// open gRPC ports. No code generation is needed: the service is registered
// by hand and every request/response is a google.protobuf.Struct.
//
// The matching service definition is in redisdiag.proto next to this file,
// so it can be called with grpcurl without server reflection:
//
//	grpcurl -plaintext -import-path grpcdiag -proto redisdiag.proto \
//	    -d '{}' <host:port> redisdiag.v1.RedisDiag/Snapshot
//	grpcurl ... -d '{"last": 50}'      <host:port> redisdiag.v1.RedisDiag/Events
//	grpcurl ... -d '{"timeout": "2s"}' <host:port> redisdiag.v1.RedisDiag/Probe
//
// Register it on an internal server/port, not on the public one.
package grpcdiag

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/DmitriiTrifonov/redis-startup-diag/pkg/redisdiag"
)

const serviceName = "redisdiag.v1.RedisDiag"

// Register adds the RedisDiag service to s.
func Register(s grpc.ServiceRegistrar, in *redisdiag.Instrument) {
	s.RegisterService(&serviceDesc, &server{in: in})
}

type server struct{ in *redisdiag.Instrument }

// diagServer is what the ServiceDesc HandlerType points at.
type diagServer interface {
	snapshot(context.Context, *structpb.Struct) (*structpb.Struct, error)
	events(context.Context, *structpb.Struct) (*structpb.Struct, error)
	probe(context.Context, *structpb.Struct) (*structpb.Struct, error)
}

func (s *server) snapshot(_ context.Context, _ *structpb.Struct) (*structpb.Struct, error) {
	return toStruct(s.in.Snapshot())
}

func (s *server) events(_ context.Context, req *structpb.Struct) (*structpb.Struct, error) {
	ev := s.in.Events()
	f := req.GetFields()
	if v, ok := f["since"]; ok {
		since, err := time.Parse(time.RFC3339, v.GetStringValue())
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "since must be RFC3339")
		}
		i := 0
		for i < len(ev) && ev[i].Time.Before(since) {
			i++
		}
		ev = ev[i:]
	}
	if v, ok := f["last"]; ok {
		if n := int(v.GetNumberValue()); n >= 0 && n < len(ev) {
			ev = ev[len(ev)-n:]
		}
	}
	return toStruct(map[string]any{"events": ev})
}

func (s *server) probe(ctx context.Context, req *structpb.Struct) (*structpb.Struct, error) {
	timeout := 2 * time.Second
	if v, ok := req.GetFields()["timeout"]; ok {
		d, err := time.ParseDuration(v.GetStringValue())
		if err != nil || d <= 0 || d > 30*time.Second {
			return nil, status.Error(codes.InvalidArgument, "timeout must be a duration in (0, 30s]")
		}
		timeout = d
	}
	res, err := s.in.Probe(ctx, timeout)
	if errors.Is(err, redisdiag.ErrProbeRunning) {
		return nil, status.Error(codes.ResourceExhausted, err.Error())
	}
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return toStruct(res)
}

// toStruct goes through JSON so the field names match the HTTP endpoints.
func toStruct(v any) (*structpb.Struct, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	st, err := structpb.NewStruct(m)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return st, nil
}

func unary(name string, call func(diagServer, context.Context, *structpb.Struct) (*structpb.Struct, error)) grpc.MethodDesc {
	return grpc.MethodDesc{
		MethodName: name,
		Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			req := new(structpb.Struct)
			if err := dec(req); err != nil {
				return nil, err
			}
			if interceptor == nil {
				return call(srv.(diagServer), ctx, req)
			}
			info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/" + serviceName + "/" + name}
			return interceptor(ctx, req, info, func(ctx context.Context, r any) (any, error) {
				return call(srv.(diagServer), ctx, r.(*structpb.Struct))
			})
		},
	}
}

var serviceDesc = grpc.ServiceDesc{
	ServiceName: serviceName,
	HandlerType: (*diagServer)(nil),
	Methods: []grpc.MethodDesc{
		unary("Snapshot", diagServer.snapshot),
		unary("Events", diagServer.events),
		unary("Probe", diagServer.probe),
	},
	Metadata: "redisdiag.proto",
}
