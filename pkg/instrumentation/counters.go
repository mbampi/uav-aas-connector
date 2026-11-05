package instrumentation

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultBackpressureReportInterval is how often ReportBackpressure logs newly
// dropped/coalesced events when no interval is supplied. It is a compromise
// between surfacing a degraded link/exporter promptly and not flooding the log
// while congestion persists.
const DefaultBackpressureReportInterval = 30 * time.Second

// Counter is a monotonic, lock-free event counter. Sites resolve their *Counter
// once (via DropCounter/CoalescedCounter) and call Inc/Add on the hot path, so
// accounting for an event is a single atomic add with no allocation and no map
// lookup.
type Counter struct {
	n atomic.Uint64
}

// Inc records a single event.
func (c *Counter) Inc() { c.n.Add(1) }

// Add records n events at once (e.g. a whole batch discarded).
func (c *Counter) Add(n uint64) { c.n.Add(n) }

// Value returns the running total.
func (c *Counter) Value() uint64 { return c.n.Load() }

// Backpressure registries:
//
//   - dropCounters: the event was discarded (data loss).
//   - coalescedCounters: the event was replaced by a newer value for the same
//     property before being sent (the latest is kept).
//
// Names are fixed shortly after startup, so reads are a lock-free Load.
var (
	dropCounters      sync.Map
	coalescedCounters sync.Map
)

func namedCounter(reg *sync.Map, name string) *Counter {
	if v, ok := reg.Load(name); ok {
		return v.(*Counter)
	}
	actual, _ := reg.LoadOrStore(name, &Counter{})
	return actual.(*Counter)
}

// DropCounter returns the counter for a named point where events are discarded
// entirely (data loss), e.g. "adapter.mavlink", "core.broadcast.mqtt", or
// "exporter.grpc". Calling it repeatedly with the same name (e.g. across a
// supervised exporter restart) returns the same counter, so totals are
// cumulative for the process lifetime.
func DropCounter(name string) *Counter { return namedCounter(&dropCounters, name) }

// CoalescedCounter returns the counter for a named point where events are
// superseded by a newer value for the same property before being sent (latest
// kept). Same naming/idempotency semantics as DropCounter.
func CoalescedCounter(name string) *Counter { return namedCounter(&coalescedCounters, name) }

// CounterValue is a point-in-time snapshot of one named counter.
type CounterValue struct {
	Name  string
	Count uint64
}

func snapshotOf(reg *sync.Map) []CounterValue {
	var out []CounterValue
	reg.Range(func(key, value any) bool {
		out = append(out, CounterValue{Name: key.(string), Count: value.(*Counter).Value()})
		return true
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// DropSnapshot returns every registered drop counter, sorted by name. Safe to
// call at any time; intended to feed the periodic reporter, the shutdown
// summary, or a future metrics endpoint.
func DropSnapshot() []CounterValue { return snapshotOf(&dropCounters) }

// CoalescedSnapshot returns every registered coalesced counter, sorted by name.
func CoalescedSnapshot() []CounterValue { return snapshotOf(&coalescedCounters) }

// ReportBackpressure periodically logs drop/coalesce counters whose totals grew
// since the previous tick. Drops are logged at Warn, coalesces at Info. It runs
// until ctx is canceled and doesn't depend on Init having succeeded.
func ReportBackpressure(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = DefaultBackpressureReportInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	prevDrop := make(map[string]uint64)
	prevCoalesced := make(map[string]uint64)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reportDeltas(slog.LevelWarn, "events dropped under backpressure (data loss)",
				DropSnapshot(), prevDrop, interval)
			reportDeltas(slog.LevelInfo, "events coalesced under backpressure (superseded, latest kept)",
				CoalescedSnapshot(), prevCoalesced, interval)
		}
	}
}

// reportDeltas logs each counter whose total grew since prev, and updates prev.
func reportDeltas(level slog.Level, msg string, snap []CounterValue, prev map[string]uint64, interval time.Duration) {
	for _, d := range snap {
		last := prev[d.Name]
		if d.Count <= last {
			continue
		}
		slog.Log(context.Background(), level, msg,
			"point", d.Name,
			"delta", d.Count-last,
			"total", d.Count,
			"window", interval.String(),
		)
		prev[d.Name] = d.Count
	}
}

// logBackpressureSummary prints a final tally of every point that dropped or
// coalesced at least one event, so an operator sees the run's totals without
// parsing logs. Called once from Close on shutdown.
func logBackpressureSummary() {
	logCounterTotals("Dropped-event totals (discarded — data loss)", DropSnapshot())
	logCounterTotals("Coalesced-event totals (superseded, latest kept)", CoalescedSnapshot())
}

func logCounterTotals(header string, snap []CounterValue) {
	var total uint64
	for _, d := range snap {
		total += d.Count
	}
	if total == 0 {
		return
	}
	slog.Info(header)
	for _, d := range snap {
		if d.Count > 0 {
			slog.Info("backpressure total", "point", d.Name, "total", d.Count)
		}
	}
}
