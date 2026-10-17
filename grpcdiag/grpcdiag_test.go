package grpcdiag_test

import (
	"context"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/DmitriiTrifonov/redis-startup-diag/grpcdiag"
	"github.com/DmitriiTrifonov/redis-startup-diag/pkg/redisdiag"
)

// Integration test against a real cluster:
//
//	REDIS_ADDRS=127.0.0.1:7000 REDIS_PASSWORD=secret go test ./...
func TestGRPC(t *testing.T) {
	addrs := os.Getenv("REDIS_ADDRS")
	if addrs == "" {
		t.Skip("REDIS_ADDRS not set")
	}
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    strings.Split(addrs, ","),
		Password: os.Getenv("REDIS_PASSWORD"),
		PoolSize: 20,
	})
	defer rdb.Close()
	in := redisdiag.InstrumentCluster(rdb, redisdiag.Options{
		Logger:             slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		DisableInternalLog: true,
	})
	defer in.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < 50; i++ { // make the client talk to every master
		_ = rdb.Get(ctx, "redisdiag-test:"+string(rune('a'+i%26))+string(rune('0'+i/26))).Err()
	}

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	grpcdiag.Register(srv, in)
	go srv.Serve(lis)
	defer srv.Stop()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	call := func(method string, req map[string]any) *structpb.Struct {
		t.Helper()
		in, _ := structpb.NewStruct(req)
		out := new(structpb.Struct)
		if err := conn.Invoke(ctx, "/redisdiag.v1.RedisDiag/"+method, in, out); err != nil {
			t.Fatalf("%s: %v", method, err)
		}
		return out
	}

	snap := call("Snapshot", map[string]any{})
	nodes := snap.Fields["nodes"].GetListValue().GetValues()
	if len(nodes) == 0 {
		t.Fatalf("snapshot has no nodes: %v", snap)
	}
	t.Logf("snapshot: %d nodes, pool_size_per_node=%v", len(nodes),
		snap.Fields["client_options"].GetStructValue().Fields["pool_size_per_node"].GetNumberValue())

	ev := call("Events", map[string]any{"last": 5})
	t.Logf("events: %d", len(ev.Fields["events"].GetListValue().GetValues()))

	pr := call("Probe", map[string]any{"timeout": "1s"})
	if !pr.Fields["topology_ok"].GetBoolValue() {
		t.Fatalf("probe topology failed: %v", pr)
	}
	for _, n := range pr.Fields["nodes"].GetListValue().GetValues() {
		f := n.GetStructValue().Fields
		t.Logf("probe %s %s tcp=%s ping=%s %s", f["addr"].GetStringValue(), f["role"].GetStringValue(),
			f["tcp_dial"].GetStringValue(), f["pool_ping"].GetStringValue(), f["pool_ping_error"].GetStringValue())
	}

	// bad argument -> InvalidArgument
	bad, _ := structpb.NewStruct(map[string]any{"timeout": "nope"})
	if err := conn.Invoke(ctx, "/redisdiag.v1.RedisDiag/Probe", bad, new(structpb.Struct)); err == nil {
		t.Fatal("expected error for bad timeout")
	}
}
