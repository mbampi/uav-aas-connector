package instrumentation

import (
	"fmt"
	"os"
	"sync"
	"time"
)

var (
	logFile *os.File
	mu      sync.Mutex
)

// Init opens the latency log file for appending.
func Init(filename string) error {
	var err error
	logFile, err = os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	stat, err := logFile.Stat()
	if err == nil && stat.Size() == 0 {
		_, _ = logFile.WriteString("ProcessTimestamp_ns,Stage,PropertyID,TraceID_ns\n")
	}

	return nil
}

// Close closes the log file.
func Close() {
	if logFile != nil {
		logFile.Close()
	}
}

// Record writes a log entry.
// stage: ADAPTER, CORE, or EXPORTER
// propertyID: The identifier of the property (e.g., "Latitude")
// traceID: The original timestamp from the adapter (used to correlate)
func Record(stage string, propertyID string, traceID time.Time) {
	if logFile == nil {
		return
	}

	// Use UnixNano for high precision
	now := time.Now().UnixNano()
	trace := traceID.UnixNano()

	line := fmt.Sprintf("%d,%s,%s,%d\n", now, stage, propertyID, trace)

	mu.Lock()
	defer mu.Unlock()
	if _, err := logFile.WriteString(line); err != nil {
		fmt.Printf("Error writing to latency log: %v\n", err)
	}
}
