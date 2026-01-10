package instrumentation

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	logFile *os.File
	logChan chan string    // Buffer for log lines
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

	// Initialize the buffered channel
	logChan = make(chan string, BufferSize)

	// Start the background writer
	wg.Add(1)
	go writer()

	return nil
}

// writer is the background worker that handles file I/O.
func writer() {
	defer wg.Done()
	for line := range logChan {
		if logFile != nil {
			// This is the only place we write to the file, so no Mutex needed here
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

	// Format string immediately to capture correct 'now' timestamp
	line := fmt.Sprintf("%d,%s,%s,%d\n", now, stage, propertyID, trace)

	// Non-blocking send:
	// If the buffer is full, we drop the log rather than blocking the system.
	select {
	case logChan <- line:
	// Queued successfully
	default:
		// Buffer full - drop message to preserve system stability (should never happen)
		slog.Warn("Latency log buffer full, dropping log", "property", propertyID, "stage", stage)
	}
}
