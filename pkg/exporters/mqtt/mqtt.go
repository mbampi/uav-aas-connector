package mqtt

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
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
	taskChan    chan publishTask // Channel to feed the worker pool
	wg          sync.WaitGroup   // WaitGroup for graceful shutdown
}

// Payload represents a standard "Time-stamped Value" for telemetry.
type Payload struct {
	Value     any    `json:"value"`
	Timestamp string `json:"timestamp"` // ISO 8601
}

// publishTask holds the token and metadata for the worker pool
type publishTask struct {
	token      mqtt.Token
	propertyID string
	timestamp  time.Time
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
		taskChan:    make(chan publishTask, 10000), // Initialize with a buffer
	}, nil
}

func (e *Exporter) Start(ctx context.Context, inChan <-chan *core.AASUpdateEvent) error {
	slog.Info("MQTT connecting")
	if token := e.client.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to connect to MQTT broker: %w", token.Error())
	}

	const numWorkers = 50
	for i := 0; i < numWorkers; i++ {
		e.wg.Add(1)
		go e.tokenWorker()
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

// Stop gracefully shuts down the exporter, waiting for all pending messages to be sent.
func (e *Exporter) Stop() error {
	// Close the task channel to signal workers to stop
	if e.taskChan != nil {
		close(e.taskChan)
	}

	// Wait for all workers to finish processing pending tokens
	e.wg.Wait()

	if e.client != nil && e.client.IsConnected() {
		// Quiesce for 250ms to allow pending messages to send
		e.client.Disconnect(250)
		slog.Info("MQTT disconnected")
	}
	return nil
}

// tokenWorker processes publish tasks from the channel and waits for their completion.
func (e *Exporter) tokenWorker() {
	defer e.wg.Done()

	// Read from the channel until it is closed
	for task := range e.taskChan {
		// WaitWithTimeout prevents infinite blocking if broker hangs
		if task.token.WaitTimeout(2 * time.Second) {
			if task.token.Error() != nil {
				slog.Error("MQTT publish failed", "error", task.token.Error())
			} else {
				instrumentation.Record("MQTT_SENT", task.propertyID, task.timestamp)
			}
		}
	}
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

	task := publishTask{
		token:      token,
		propertyID: event.PropertyID,
		timestamp:  event.Timestamp,
	}

	// Non-blocking send to the worker pool
	select {
	case e.taskChan <- task:
		// Task successfully handed off to worker
	default:
		// Buffer is full. We drop the wait to prevent blocking the main thread.
		// The message was still sent to the broker, we just won't log the "MQTT_SENT" confirmation.
		slog.Warn("MQTT worker pool saturated, dropping publish acknowledgment wait", "property", event.PropertyID)
	}
}
