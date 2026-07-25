package instrumentation

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultDropReportInterval is how often ReportDrops logs newly dropped events
// when no interval is supplied. It is a compromise between surfacing a degraded
// link/exporter promptly and not flooding the log while congestion persists.
const DefaultDropReportInterval = 30 * time.Second

// Counter is a monotonic, lock-free event counter. Drop sites resolve their
// *Counter once (via DropCounter) and call Inc/Add on the hot path, so
// accounting for a discarded event is a single atomic add with no allocation
// and no map lookup.
type Counter struct {
	n atomic.Uint64
}

// Inc records a single dropped event.
func (c *Counter) Inc() { c.n.Add(1) }

// Add records n dropped events at once (e.g. a whole batch discarded).
func (c *Counter) Add(n uint64) { c.n.Add(n) }

// Value returns the running total.
func (c *Counter) Value() uint64 { return c.n.Load() }

// dropCounters maps a drop-point name to its *Counter. Like stageStats, the set
// of names is small and fixed shortly after startup, so steady-state reads are a
// lock-free Load with no allocation.
var dropCounters sync.Map

// DropCounter returns the counter for a named drop point, creating it on first
// use. Names identify where events are being discarded, e.g. "adapter.mavlink",
// "core.broadcast.mqtt", or "exporter.grpc". Calling it repeatedly with the
// same name (e.g. across a supervised exporter restart) returns the same
// counter, so totals are cumulative for the process lifetime.
func DropCounter(name string) *Counter {
	if v, ok := dropCounters.Load(name); ok {
		return v.(*Counter)
	}
	actual, _ := dropCounters.LoadOrStore(name, &Counter{})
	return actual.(*Counter)
}

// DropCount is a point-in-time snapshot of one drop counter.
type DropCount struct {
	Name  string
	Count uint64
}

// DropSnapshot returns every registered drop counter, sorted by name. Safe to
// call at any time; intended to feed the periodic reporter, the shutdown
// summary, or a future metrics endpoint.
func DropSnapshot() []DropCount {
	var out []DropCount
	dropCounters.Range(func(key, value any) bool {
		out = append(out, DropCount{Name: key.(string), Count: value.(*Counter).Value()})
		return true
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// ReportDrops periodically logs drop counters whose totals grew since the
// previous tick, so a degraded exporter or link surfaces in the log within one
// interval while a healthy system stays silent. It is effectively a per-site
// rate-limited warning, replacing the per-event logs that previously flooded
// the log under sustained congestion. It runs until ctx is canceled.
//
// It does not depend on Init having succeeded: the counters are independent of
// the latency-log file, so drop observability works even if that file could not
// be opened.
func ReportDrops(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = DefaultDropReportInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	prev := make(map[string]uint64)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, d := range DropSnapshot() {
				last := prev[d.Name]
				if d.Count <= last {
					continue
				}
				slog.Warn("events dropped under backpressure",
					"point", d.Name,
					"delta", d.Count-last,
					"total", d.Count,
					"window", interval.String(),
				)
				prev[d.Name] = d.Count
			}
		}
	}
}

// logDropSummary prints a final tally of every drop point that discarded at
// least one event, so an operator sees total loss for the run without parsing
// logs. Called once from Close on shutdown.
func logDropSummary() {
	drops := DropSnapshot()
	var total uint64
	for _, d := range drops {
		total += d.Count
	}
	if total == 0 {
		return
	}
	slog.Info("Dropped-event summary (events discarded under backpressure/failure)")
	for _, d := range drops {
		if d.Count == 0 {
			continue
		}
		slog.Warn("dropped events", "point", d.Name, "total", d.Count)
	}
}
