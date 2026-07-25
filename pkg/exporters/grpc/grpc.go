package grpc

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"uav-aas-connector/pkg/config"
	"uav-aas-connector/pkg/core"
	pb "uav-aas-connector/pkg/exporters/grpc/proto"
	"uav-aas-connector/pkg/instrumentation"
)

type Exporter struct {
	conn     *grpc.ClientConn
	client   pb.AASIngestClient
	stream   pb.AASIngest_StreamUpdatesClient
	host     string
	security config.SecurityConfig
	drops    *instrumentation.Counter // events dropped because sendChan was full
}

func NewExporter(cfg config.ExporterConfig) (*Exporter, error) {
	host := cfg.Host
	if host == "" {
		host = fmt.Sprintf("localhost:%d", cfg.Port)
	}

	return &Exporter{
		host:     host,
		security: cfg.Security,
		drops:    instrumentation.DropCounter("exporter.grpc"),
	}, nil
}

func (e *Exporter) Start(ctx context.Context, inChan <-chan *core.AASUpdateEvent) error {
	slog.Info("gRPC connecting", "host", e.host, "tls", e.security.TLSEnabled)

	// 1. Dial the Server
	dialOpts, err := e.dialOptions()
	if err != nil {
		return fmt.Errorf("invalid gRPC security config: %w", err)
	}
	conn, err := grpc.NewClient(e.host, dialOpts...)
	if err != nil {
		return fmt.Errorf("failed to create gRPC client: %v", err)
	}
	e.conn = conn
	e.client = pb.NewAASIngestClient(conn)

	// Decouple ingestion from the network: a dedicated goroutine owns the
	// stream and performs the (potentially blocking) Send, while this loop
	// only drains inChan into a buffered queue. A slow or stuck network can no
	// longer stall inChan and make the core drop updates for this exporter.
	sendChan := make(chan *core.AASUpdateEvent, sendQueueSize)
	go e.sendLoop(ctx, sendChan)

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-inChan:
			if !ok {
				return nil
			}
			select {
			case sendChan <- event:
			default:
				// Network can't keep up and the buffer is full; count the loss
				// (instrumentation.ReportDrops surfaces it periodically) instead
				// of logging per-event.
				e.drops.Inc()
			}
		}
	}
}

// sendQueueSize bounds the buffered send queue that absorbs network blips
// before updates start being dropped.
const sendQueueSize = 1000

// Keepalive parameters. Without these, a half-open connection (peer dies
// without sending a TCP RST - power loss, cable pull, NAT/firewall dropping the
// flow) leaves stream.Send blocked until the OS TCP retransmit timeout, which
// defaults to ~15 minutes on Linux. With client keepalive, gRPC pings an idle
// transport and tears it down within keepaliveTime+keepaliveTimeout, so the
// sender goroutine's Send fails fast and the reconnect loop takes over in
// seconds instead of minutes.
//
// NOTE: keepaliveTime is below the gRPC server's default enforcement MinTime
// (5m), so the ingest server must permit this cadence (keepalive.EnforcementPolicy
// with MinTime <= keepaliveTime and PermitWithoutStream: true) or it will send
// GOAWAY "too_many_pings". Both sides are part of this integration, so they are
// configured together.
const (
	keepaliveTime    = 20 * time.Second
	keepaliveTimeout = 5 * time.Second
)

// sendLoop owns the gRPC stream and reconnects with a fixed backoff. It runs on
// its own goroutine so the blocking Send never touches the ingest loop. A gRPC
// stream must only be sent on from a single goroutine, which this satisfies.
func (e *Exporter) sendLoop(ctx context.Context, sendChan <-chan *core.AASUpdateEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := e.streamAndSend(ctx, sendChan); err != nil {
				slog.Error("gRPC stream error, retrying", "error", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(1 * time.Second):
				}
			}
		}
	}
}

// streamAndSend opens a stream and forwards queued events until the stream
// fails (returning the error to trigger a reconnect) or the context ends.
func (e *Exporter) streamAndSend(ctx context.Context, sendChan <-chan *core.AASUpdateEvent) error {
	stream, err := e.client.StreamUpdates(ctx)
	if err != nil {
		return err
	}
	e.stream = stream
	slog.Info("gRPC stream established")

	for {
		select {
		case <-ctx.Done():
			return stream.CloseSend()

		case event, ok := <-sendChan:
			if !ok {
				return stream.CloseSend()
			}

			req := buildRequest(event)

			instrumentation.Record("GRPC_SENDING", event.PropertyID, event.Timestamp)

			// Send to stream (blocking if network is backed up, but this runs
			// on the sender goroutine, so ingestion is unaffected).
			if err := stream.Send(req); err != nil {
				return err // Return error to trigger reconnect loop
			}

			instrumentation.Record("GRPC_SENT", event.PropertyID, event.Timestamp)
		}
	}
}

// buildRequest converts a core event into the protobuf message, populating the
// 'oneof' value field from the event's concrete type.
func buildRequest(event *core.AASUpdateEvent) *pb.UpdateRequest {
	req := &pb.UpdateRequest{
		SubmodelId:   event.SubmodelID,
		PropertyPath: event.PropertyID,
		TimestampNs:  event.Timestamp.UnixNano(),
	}

	switch v := event.Value.(type) {
	case float64:
		req.Value = &pb.UpdateRequest_DoubleVal{DoubleVal: v}
	case float32:
		req.Value = &pb.UpdateRequest_DoubleVal{DoubleVal: float64(v)}
	case int:
		req.Value = &pb.UpdateRequest_IntVal{IntVal: int32(v)}
	case int32:
		req.Value = &pb.UpdateRequest_IntVal{IntVal: v}
	case string:
		req.Value = &pb.UpdateRequest_StringVal{StringVal: v}
	case bool:
		req.Value = &pb.UpdateRequest_BoolVal{BoolVal: v}
	default:
		// Fallback to string for unknown types
		req.Value = &pb.UpdateRequest_StringVal{StringVal: fmt.Sprintf("%v", v)}
	}
	return req
}

func (e *Exporter) Stop() error {
	if e.stream != nil {
		e.stream.CloseSend()
	}
	if e.conn != nil {
		return e.conn.Close()
	}
	return nil
}

// dialOptions builds gRPC dial options from the shared security config: TLS
// transport credentials when enabled (insecure otherwise, matching the
// previous behavior), plus per-RPC basic auth metadata when a username is
// set, for remote servers that authenticate gRPC calls that way.
func (e *Exporter) dialOptions() ([]grpc.DialOption, error) {
	var transportCreds credentials.TransportCredentials
	if e.security.TLSEnabled {
		tlsConfig, err := e.security.BuildTLSConfig()
		if err != nil {
			return nil, err
		}
		transportCreds = credentials.NewTLS(tlsConfig)
	} else {
		transportCreds = insecure.NewCredentials()
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                keepaliveTime,
			Timeout:             keepaliveTimeout,
			PermitWithoutStream: true, // keep the long-lived conn probed even between stream reconnects
		}),
	}

	if e.security.Username != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(basicAuthCreds{
			username:   e.security.Username,
			password:   e.security.Password,
			requireTLS: e.security.TLSEnabled,
		}))
	}

	return opts, nil
}

// basicAuthCreds attaches HTTP Basic-style credentials to every RPC via the
// "authorization" metadata key.
type basicAuthCreds struct {
	username, password string
	requireTLS         bool
}

func (c basicAuthCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	token := base64.StdEncoding.EncodeToString([]byte(c.username + ":" + c.password))
	return map[string]string{"authorization": "Basic " + token}, nil
}

func (c basicAuthCreds) RequireTransportSecurity() bool {
	return c.requireTLS
}
