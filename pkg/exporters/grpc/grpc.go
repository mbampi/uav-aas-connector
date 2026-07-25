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
}

func NewExporter(cfg config.ExporterConfig) (*Exporter, error) {
	host := cfg.Host
	if host == "" {
		host = fmt.Sprintf("localhost:%d", cfg.Port)
	}

	return &Exporter{host: host, security: cfg.Security}, nil
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

	// 2. Main Loop: Reconnect logic + Stream handling
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err := e.runStream(ctx, inChan); err != nil {
				slog.Error("gRPC stream error, retrying", "error", err)
				time.Sleep(1 * time.Second)
			}
		}
	}
}

func (e *Exporter) runStream(ctx context.Context, inChan <-chan *core.AASUpdateEvent) error {
	// Open the stream
	stream, err := e.client.StreamUpdates(ctx)
	if err != nil {
		return err
	}
	e.stream = stream
	slog.Info("gRPC stream established")

	for {
		select {
		case <-ctx.Done():
			return e.stream.CloseSend()

		case event, ok := <-inChan:
			if !ok {
				return e.stream.CloseSend()
			}

			// Convert Core Event -> Proto Message
			req := &pb.UpdateRequest{
				SubmodelId:   event.SubmodelID,
				PropertyPath: event.PropertyID,
				TimestampNs:  event.Timestamp.UnixNano(),
			}

			// Type Switch to populate the 'oneof' field
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

			instrumentation.Record("GRPC_SENDING", event.PropertyID, event.Timestamp)

			// Send to stream (Blocking if network is backed up)
			if err := e.stream.Send(req); err != nil {
				return err // Return error to trigger reconnect loop
			}

			instrumentation.Record("GRPC_SENT", event.PropertyID, event.Timestamp)
		}
	}
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

	opts := []grpc.DialOption{grpc.WithTransportCredentials(transportCreds)}

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
