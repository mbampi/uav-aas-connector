package grpc

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"uav-aas-connector/pkg/config"
	"uav-aas-connector/pkg/core"
	pb "uav-aas-connector/pkg/exporters/grpc/proto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testIngestServer is a minimal AASIngest server that forwards every received
// update onto a channel so the test can assert delivery.
type testIngestServer struct {
	pb.UnimplementedAASIngestServer
	received chan *pb.UpdateRequest
}

func (s *testIngestServer) StreamUpdates(stream grpc.ClientStreamingServer[pb.UpdateRequest, pb.IngestResponse]) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.IngestResponse{})
		}
		if err != nil {
			return err
		}
		s.received <- req
	}
}

// TestExporter_StreamsToRealServerWithKeepalive exercises the full dial path -
// including the keepalive dial options - against a real gRPC server on an
// ephemeral port, and confirms a healthy stream still delivers updates. It
// guards against the keepalive params being rejected by gRPC or breaking normal
// streaming. The server's enforcement policy mirrors the cadence documented on
// the client keepalive constants, so the two are validated together.
//
// Note: this does not exercise half-open-link *detection* (that needs a network
// fault injector and would take keepaliveTime+keepaliveTimeout to fire); it
// verifies the change is wired correctly and non-breaking.
func TestExporter_StreamsToRealServerWithKeepalive(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             keepaliveTime, // accept the client's ping cadence
		PermitWithoutStream: true,          // ...even between stream reconnects
	}))
	received := make(chan *pb.UpdateRequest, 16)
	pb.RegisterAASIngestServer(srv, &testIngestServer{received: received})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	exp, err := NewExporter(config.ExporterConfig{Host: lis.Addr().String()})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan *core.AASUpdateEvent, 4)
	go func() { _ = exp.Start(ctx, in) }()

	in <- &core.AASUpdateEvent{
		SubmodelID: "Nav", PropertyID: "Pos.Lat", Value: float64(12.34), Timestamp: time.Now(),
	}

	select {
	case got := <-received:
		assert.Equal(t, "Nav", got.SubmodelId)
		assert.Equal(t, "Pos.Lat", got.PropertyPath)
		assert.InDelta(t, 12.34, got.GetDoubleVal(), 1e-9)
	case <-time.After(5 * time.Second):
		t.Fatal("server did not receive the streamed update")
	}
}

// TestExporter_StopWaitsForSendLoop guards against a race between Stop
// closing the stream/connection and sendLoop still calling Send on it from
// another goroutine: gRPC's clientStream explicitly forbids calling CloseSend
// concurrently with SendMsg. Stop must block until sendLoop has fully
// released the stream before touching it. Run with -race to catch a
// regression here.
func TestExporter_StopWaitsForSendLoop(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	received := make(chan *pb.UpdateRequest, 64)
	pb.RegisterAASIngestServer(srv, &testIngestServer{received: received})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	exp, err := NewExporter(config.ExporterConfig{Host: lis.Addr().String()})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan *core.AASUpdateEvent, 64)
	startDone := make(chan struct{})
	go func() {
		defer close(startDone)
		_ = exp.Start(ctx, in)
	}()

	// Keep sendLoop actively calling Send while Stop runs concurrently, as
	// Start's own shutdown path (ctx cancellation) would otherwise make
	// sendLoop exit before Stop gets a chance to race it.
	stopDone := make(chan struct{})
	go func() {
		defer close(stopDone)
		time.Sleep(20 * time.Millisecond)
		cancel()
		_ = exp.Stop()
	}()

	for i := range 100 {
		select {
		case in <- &core.AASUpdateEvent{SubmodelID: "Nav", PropertyID: "Pos.Lat", Value: float64(i), Timestamp: time.Now()}:
		case <-stopDone:
		}
	}

	select {
	case <-stopDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return")
	}
	select {
	case <-startDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after context cancellation")
	}
}
