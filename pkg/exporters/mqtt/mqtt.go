package mqtt

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
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
}

// Payload represents a standard "Time-stamped Value" for telemetry.
type Payload struct {
	Value     any    `json:"value"`
	Timestamp string `json:"timestamp"` // ISO 8601
}

func NewExporter(cfg config.ExporterConfig) (*Exporter, error) {
	opts := mqtt.NewClientOptions()

	// Default to local broker if Host not specified
	broker := cfg.Host
	if broker == "" {
		broker = fmt.Sprintf("tcp://localhost:%d", cfg.Port)
	}
	opts.AddBroker(broker)

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

	return &Exporter{
		client:      client,
		topicPrefix: prefix,
	}, nil
}

func (e *Exporter) Start(ctx context.Context, inChan <-chan *core.AASUpdateEvent) error {
	slog.Info("MQTT connecting")
	if token := e.client.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to connect to MQTT broker: %w", token.Error())
	}

	// Process event loop
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-inChan:
			if !ok {
				return nil
			}
			e.publish(event)
		}
	}
}

func (e *Exporter) Stop() error {
	if e.client != nil && e.client.IsConnected() {
		// Quiesce for 250ms to allow pending messages to send
		e.client.Disconnect(250)
		slog.Info("MQTT disconnected")
	}
	return nil
}

func (e *Exporter) publish(event *core.AASUpdateEvent) {
	// AAS TOPIC STANDARD (Common Pattern):
	// <Prefix>/submodels/<Base64_SubmodelID>/elements/<IdShortPath>

	smIDEncoded := base64.URLEncoding.EncodeToString([]byte(event.SubmodelID))
	// PropertyID is usually an idShort path (e.g. "Position.Latitude"), which is safe for topics.
	// We replace dots with slashes to make it a true MQTT hierarchy.
	path := strings.ReplaceAll(event.PropertyID, ".", "/")

	topic := fmt.Sprintf("%s/submodels/%s/elements/%s", e.topicPrefix, smIDEncoded, path)

	// Create Payload
	payload := Payload{
		Value:     event.Value,
		Timestamp: event.Timestamp.UTC().Format(time.RFC3339Nano),
	}

	data, err := json.Marshal(payload)
	if err != nil {
		slog.Error("MQTT error marshaling payload", "error", err)
		return
	}

	instrumentation.Record("MQTT_SENDING", event.PropertyID, event.Timestamp)

	token := e.client.Publish(topic, 0, false, data)

	// Non-blocking wait (fire and forget for performance) or log error asynchronously
	go func() {
		// WaitWithTimeout prevents infinite blocking if broker hangs
		if token.WaitTimeout(2 * time.Second) {
			if token.Error() != nil {
				slog.Error("MQTT publish failed", "error", token.Error())
			} else {
				instrumentation.Record("MQTT_SENT", event.PropertyID, event.Timestamp)
			}
		}
	}()
}
