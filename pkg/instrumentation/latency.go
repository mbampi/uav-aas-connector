package instrumentation

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
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

// stageStat holds always-on latency aggregates for one pipeline stage.
// Updates use only atomic operations, so folding in a sample never allocates
// or blocks - cheap enough to run for every event, independently of whether
// that event is also written to the detailed log.
type stageStat struct {
	count atomic.Uint64
	sumNs atomic.Uint64
	maxNs atomic.Uint64
}

// observe folds one cumulative-latency sample (ingest -> this stage) into the
// aggregate.
func (s *stageStat) observe(latencyNs uint64) {
	s.count.Add(1)
	s.sumNs.Add(latencyNs)
	for {
		old := s.maxNs.Load()
		if latencyNs <= old || s.maxNs.CompareAndSwap(old, latencyNs) {
			break
		}
	}
}

// stageStats maps a stage name to its *stageStat. The set of stages is small
// and fixed at runtime, so after each stage is seen once every Record does a
// lock-free Load with no allocation.
var stageStats sync.Map

// statFor returns the aggregate for a stage, creating it on first use.
func statFor(stage string) *stageStat {
	if v, ok := stageStats.Load(stage); ok {
		return v.(*stageStat)
	}
	actual, _ := stageStats.LoadOrStore(stage, &stageStat{})
	return actual.(*stageStat)
}

// StageLatency is a point-in-time snapshot of one stage's latency aggregate.
// Latencies are cumulative from ingestion (the trace timestamp) to the stage.
type StageLatency struct {
	Stage  string
	Count  uint64
	MeanNs float64
	MaxNs  uint64
}

// Snapshot returns the current per-stage latency aggregates, sorted by stage
// name. It is safe to call at any time and is intended to feed a metrics
// endpoint or the shutdown summary.
func Snapshot() []StageLatency {
	var out []StageLatency
	stageStats.Range(func(key, value any) bool {
		s := value.(*stageStat)
		count := s.count.Load()
		var mean float64
		if count > 0 {
			mean = float64(s.sumNs.Load()) / float64(count)
		}
		out = append(out, StageLatency{
			Stage:  key.(string),
			Count:  count,
			MeanNs: mean,
			MaxNs:  s.maxNs.Load(),
		})
		return true
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Stage < out[j].Stage })
	return out
}

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
	logSummary()
	logBackpressureSummary()

	if logChan != nil {
		close(logChan) // Signal writer to stop processing after draining buffer
	}

	wg.Wait() // Wait for writer to finish writing pending logs

	if logFile != nil {
		logFile.Close()
	}
}

// logSummary prints the accumulated per-stage latency aggregates on shutdown,
// so the operator sees end-to-end latency without parsing the detailed log.
func logSummary() {
	stats := Snapshot()
	if len(stats) == 0 {
		return
	}
	slog.Info("Latency summary (cumulative from ingest, per stage)")
	for _, s := range stats {
		slog.Info("stage latency",
			"stage", s.Stage,
			"count", s.Count,
			"mean", time.Duration(int64(s.MeanNs)).String(),
			"max", time.Duration(s.MaxNs).String(),
		)
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

	// Always-on in-memory aggregates. A lock-free Load plus a few
	// atomics, so this runs for every event regardless of whether the
	// detailed row below is sampled. A negative delta (e.g. a zero-value
	// trace) is skipped so it can't poison the aggregate.
	if latency := now - trace; latency >= 0 {
		statFor(stage).observe(uint64(latency))
	}

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
