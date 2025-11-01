package core

import "time"

// RawDataPoint is the standardized, protocol-agnostic struct
// that all input adapters must produce.
// Produced by InputAdapters, Consumed by ProcessingCore
type RawDataPoint struct {
	// SourceID uniquely identifies the data source,
	// e.g., "mavlink-sys-1-comp-1"
	SourceID string

	// MessageName is the protocol-neutral name of the message,
	// e.g., "HEARTBEAT", "GPS_RAW_INT"
	MessageName string

	// Timestamp is when the message was received by the adapter.
	Timestamp time.Time

	// Fields is the raw key-value data from the source.
	// e.g., {"Type": 2, "SystemStatus": 4, "Autopilot": 3}
	Fields map[string]any
}
