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
