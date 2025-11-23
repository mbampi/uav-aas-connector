package grpc

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"uav-aas-connector/pkg/config"
	"uav-aas-connector/pkg/core"
	pb "uav-aas-connector/pkg/exporters/grpc/proto"
	"uav-aas-connector/pkg/instrumentation"
)

type Exporter struct {
	conn   *grpc.ClientConn
	client pb.AASIngestClient
	stream pb.AASIngest_StreamUpdatesClient
	host   string
}

func NewExporter(cfg config.ExporterConfig) (*Exporter, error) {
	host := cfg.Host
	if host == "" {
		host = fmt.Sprintf("localhost:%d", cfg.Port)
	}

	return &Exporter{host: host}, nil
}

func (e *Exporter) Start(ctx context.Context, inChan <-chan *core.AASUpdateEvent) error {
	log.Printf("[gRPC] Connecting to %s...", e.host)

	// 1. Dial the Server
	// Note: For production, use credentials.NewClientTLSFromFile(...)
	conn, err := grpc.NewClient(e.host, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
				log.Printf("[gRPC] Stream error: %v. Retrying in 1s...", err)
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
	log.Println("[gRPC] Stream established")

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
