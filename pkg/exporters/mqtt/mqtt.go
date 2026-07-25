package mqtt

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"uav-aas-connector/pkg/config"
	"uav-aas-connector/pkg/core"
	"uav-aas-connector/pkg/instrumentation"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Exporter sends AAS updates to an MQTT broker.
// It follows a topic structure compatible with AAS ValueOnly access.
type Exporter struct {
	client      mqtt.Client
	topicPrefix string
	shards      []chan *core.AASUpdateEvent // per-worker queues; one worker per shard
	wg          sync.WaitGroup             // WaitGroup for graceful shutdown
}

const (
	// numWorkers is the number of publish workers (and shards). Updates for a
	// given property always route to the same shard, so a property's messages
	// are published in order while different properties publish concurrently.
	numWorkers = 50
	// shardBufferSize bounds each shard's queue; when full, updates for that
	// shard are dropped rather than blocking the ingest loop.
	shardBufferSize = 256
)

func NewExporter(cfg config.ExporterConfig) (*Exporter, error) {
	opts := mqtt.NewClientOptions()

	// Default to local broker if Host not specified
	broker := cfg.Host
	if broker == "" {
		broker = fmt.Sprintf("tcp://localhost:%d", cfg.Port)
	}

	if cfg.Security.TLSEnabled {
		if !hasTLSScheme(broker) {
			return nil, fmt.Errorf("mqtt: host must use a TLS scheme (e.g. ssl://) when security.tlsEnabled is true, got %q", broker)
		}
		tlsConfig, err := cfg.Security.BuildTLSConfig()
		if err != nil {
			return nil, fmt.Errorf("mqtt: %w", err)
		}
		opts.SetTLSConfig(tlsConfig)
	}
	opts.AddBroker(broker)

	if cfg.Security.Username != "" {
		opts.SetUsername(cfg.Security.Username)
		opts.SetPassword(cfg.Security.Password)
	}

	if cfg.ClientID != "" {
		opts.SetClientID(cfg.ClientID)
	} else {
		opts.SetClientID("aas-connector-" + fmt.Sprint(time.Now().Unix()))
	}

	// Auto-reconnect ensures reliability during network blips
	opts.SetAutoReconnect(true)
	opts.SetOnConnectHandler(func(c mqtt.Client) {
		slog.Info("MQTT connected", "broker", broker)
	})
	opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		slog.Error("MQTT connection lost", "error", err)
	})

	client := mqtt.NewClient(opts)

	// Set default prefix if missing
	prefix := cfg.TopicPrefix
	if prefix == "" {
		prefix = "aas/events"
	}
	// Remove trailing slash to be clean
	prefix = strings.TrimSuffix(prefix, "/")

	shards := make([]chan *core.AASUpdateEvent, numWorkers)
	for i := range shards {
		shards[i] = make(chan *core.AASUpdateEvent, shardBufferSize)
	}

	return &Exporter{
		client:      client,
		topicPrefix: prefix,
		shards:      shards,
	}, nil
}

// hasTLSScheme reports whether broker uses one of the schemes paho.mqtt
// dials over TLS. It catches the common mistake of enabling
// security.tlsEnabled without switching the broker URL away from tcp://.
func hasTLSScheme(broker string) bool {
	for _, scheme := range []string{"ssl://", "tls://", "mqtts://", "mqtt+ssl://", "tcps://"} {
		if strings.HasPrefix(broker, scheme) {
			return true
		}
	}
	return false
}

func (e *Exporter) Start(ctx context.Context, inChan <-chan *core.AASUpdateEvent) error {
	slog.Info("MQTT connecting")
	if token := e.client.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to connect to MQTT broker: %w", token.Error())
	}

	for i := range e.shards {
		e.wg.Add(1)
		go e.worker(e.shards[i])
	}

	// Ingest loop: route each event to its property's shard without blocking.
	// The Marshal and (potentially blocking) Paho Publish now run in the
	// workers, so a slow broker can never stall ingestion.
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-inChan:
			if !ok {
				return nil
			}
			shard := e.shards[shardFor(event, len(e.shards))]
			select {
			case shard <- event:
			default:
				slog.Warn("MQTT worker pool saturated, dropping update", "property", event.PropertyID)
			}
		}
	}
}

// Stop gracefully shuts down the exporter, waiting for all pending messages to be sent.
func (e *Exporter) Stop() error {
	// Close the shard channels to signal workers to drain and stop.
	for _, shard := range e.shards {
		close(shard)
	}

	// Wait for all workers to finish publishing pending messages.
	e.wg.Wait()

	if e.client != nil && e.client.IsConnected() {
		// Quiesce for 250ms to allow pending messages to send
		e.client.Disconnect(250)
		slog.Info("MQTT disconnected")
	}
	return nil
}

// worker publishes events from a single shard. Because each property maps to
// exactly one shard, a property's updates are published in order.
func (e *Exporter) worker(shard <-chan *core.AASUpdateEvent) {
	defer e.wg.Done()
	for event := range shard {
		e.publish(event)
	}
}

// shardFor maps an event to a worker shard by hashing its property identity
// (FNV-1a), so all updates for one property are handled by the same worker.
func shardFor(event *core.AASUpdateEvent, n int) int {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(event.SubmodelID); i++ {
		h = (h ^ uint32(event.SubmodelID[i])) * prime32
	}
	for i := 0; i < len(event.PropertyID); i++ {
		h = (h ^ uint32(event.PropertyID[i])) * prime32
	}
	return int(h % uint32(n))
}

// publish marshals and sends one event, then waits (bounded) for the broker
// acknowledgment. It runs on a worker goroutine, so its cost and any Paho
// backpressure never reach the ingest loop.
func (e *Exporter) publish(event *core.AASUpdateEvent) {
	// AAS TOPIC STANDARD (Common Pattern):
	// <Prefix>/submodels/<Base64_SubmodelID>/elements/<IdShortPath>
	smIDEncoded := base64.URLEncoding.EncodeToString([]byte(event.SubmodelID))
	// PropertyID is usually an idShort path (e.g. "Position.Latitude"), which is safe for topics.
	// We replace dots with slashes to make it a true MQTT hierarchy.
	path := strings.ReplaceAll(event.PropertyID, ".", "/")
	topic := fmt.Sprintf("%s/submodels/%s/elements/%s", e.topicPrefix, smIDEncoded, path)

	data := encodePayload(event.Value, event.Timestamp)

	instrumentation.Record("MQTT_SENDING", event.PropertyID, event.Timestamp)
	token := e.client.Publish(topic, 0, false, data)

	// WaitTimeout prevents indefinite blocking if the broker hangs.
	if token.WaitTimeout(2 * time.Second) {
		if err := token.Error(); err != nil {
			slog.Error("MQTT publish failed", "error", err, "property", event.PropertyID)
		} else {
			instrumentation.Record("MQTT_SENT", event.PropertyID, event.Timestamp)
		}
	}
}

// encodePayload builds the telemetry JSON payload
// {"value":<v>,"timestamp":"<RFC3339Nano>"} directly, avoiding the reflection
// cost of encoding/json on the hot path. The value is encoded per its concrete
// type; unexpected types fall back to encoding/json.
func encodePayload(value any, ts time.Time) []byte {
	buf := make([]byte, 0, 96)
	buf = append(buf, `{"value":`...)
	buf = appendJSONValue(buf, value)
	buf = append(buf, `,"timestamp":"`...)
	// RFC3339Nano emits only JSON-safe characters, so it needs no escaping.
	buf = ts.UTC().AppendFormat(buf, time.RFC3339Nano)
	buf = append(buf, '"', '}')
	return buf
}

// appendJSONValue appends value as a JSON literal. Integer kinds arrive from
// the pipeline already standardized to int64/uint64 (see the MAVLink adapter).
func appendJSONValue(buf []byte, value any) []byte {
	switch v := value.(type) {
	case string:
		return appendJSONString(buf, v)
	case bool:
		return strconv.AppendBool(buf, v)
	case float64:
		return appendJSONFloat(buf, v, 64)
	case float32:
		return appendJSONFloat(buf, float64(v), 32)
	case int:
		return strconv.AppendInt(buf, int64(v), 10)
	case int32:
		return strconv.AppendInt(buf, int64(v), 10)
	case int64:
		return strconv.AppendInt(buf, v, 10)
	case uint32:
		return strconv.AppendUint(buf, uint64(v), 10)
	case uint64:
		return strconv.AppendUint(buf, v, 10)
	default:
		// Safety net for unexpected types.
		if b, err := json.Marshal(v); err == nil {
			return append(buf, b...)
		}
		return append(buf, "null"...)
	}
}

// appendJSONFloat appends f, emitting null for NaN/Inf (which are not valid
// JSON) so the payload always parses.
func appendJSONFloat(buf []byte, f float64, bitSize int) []byte {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return append(buf, "null"...)
	}
	return strconv.AppendFloat(buf, f, 'f', -1, bitSize)
}

// appendJSONString appends s as a quoted, escaped JSON string.
func appendJSONString(buf []byte, s string) []byte {
	const hex = "0123456789abcdef"
	buf = append(buf, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			buf = append(buf, '\\', '"')
		case c == '\\':
			buf = append(buf, '\\', '\\')
		case c == '\n':
			buf = append(buf, '\\', 'n')
		case c == '\r':
			buf = append(buf, '\\', 'r')
		case c == '\t':
			buf = append(buf, '\\', 't')
		case c < 0x20:
			buf = append(buf, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xF])
		default:
			buf = append(buf, c)
		}
	}
	return append(buf, '"')
}
