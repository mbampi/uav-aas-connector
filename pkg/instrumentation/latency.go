package instrumentation

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// logEntry represents a single latency log entry.
type logEntry struct {
	Timestamp_ns int64
	Stage        string
	PropertyID   string
	TraceID_ns   int64
}

var (
	logFile *os.File
	logChan chan logEntry  // Buffer for asynchronous logging
	wg      sync.WaitGroup // To ensure logs are flushed on shutdown
)

// BufferSize determines how many logs can be queued before we start dropping them.
const BufferSize = 10000

// Init opens the latency log file and starts the background writer.
func Init(metricsDir string) error {
	// Automatically create the directory if it doesn't exist
	if err := os.MkdirAll(metricsDir, 0755); err != nil {
		return fmt.Errorf("could not create dir %s: %w", metricsDir, err)
	}

	strTime := time.Now().Format("20060102_150405")
	latencyPath := filepath.Join(metricsDir, "latency_"+strTime+".log")

	var err error
	logFile, err = os.OpenFile(latencyPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	stat, err := logFile.Stat()
	if err == nil && stat.Size() == 0 {
		_, _ = logFile.WriteString("ProcessTimestamp_ns,Stage,PropertyID,TraceID_ns\n")
	}

	// 3. Initialize the typed channel
	logChan = make(chan logEntry, BufferSize)

	// Start the background writer
	wg.Add(1)
	go writer()

	return nil
}

// writer is the background worker that handles file I/O.
func writer() {
	defer wg.Done()
	// Format the string only when writing to the file, offloading the CPU/GC cost
	for entry := range logChan {
		if logFile != nil {
			line := fmt.Sprintf("%d,%s,%s,%d\n", entry.Timestamp_ns, entry.Stage, entry.PropertyID, entry.TraceID_ns)
			_, err := logFile.WriteString(line)
			if err != nil {
				slog.Error("Error writing to latency log", "error", err)
			}
		}
	}
}

// Close flushes the channel and closes the file.
func Close() {
	if logChan != nil {
		close(logChan) // Signal writer to stop processing after draining buffer
	}

	wg.Wait() // Wait for writer to finish writing pending logs

	if logFile != nil {
		logFile.Close()
	}
}

// Record queues a log entry asynchronously.
// It returns immediately, preventing I/O from blocking the caller.
func Record(stage string, propertyID string, traceID time.Time) {
	if logChan == nil {
		return
	}

	// Use UnixNano for high precision
	now := time.Now().UnixNano()
	trace := traceID.UnixNano()

	// Create a stack-allocated struct (Zero GC overhead)
	entry := logEntry{
		Timestamp_ns: now,
		Stage:        stage,
		PropertyID:   propertyID,
		TraceID_ns:   trace,
	}

	select {
	case logChan <- entry:
	default:
		// Buffer full - drop message to preserve system stability (should never happen)
		slog.Warn("Latency log buffer full, dropping log", "property", propertyID, "stage", stage)
	}
}
