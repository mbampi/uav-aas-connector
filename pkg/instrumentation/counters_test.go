package instrumentation

import (
	"bytes"
	"context"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBuffer is a mutex-guarded buffer so the reporter goroutine's log writes
// don't race the test's reads.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestDropCounter_IdempotentAndCounts verifies that the registry returns the
// same counter for a name (so drop sites and readers agree) and that Inc/Add
// accumulate.
func TestDropCounter_IdempotentAndCounts(t *testing.T) {
	c1 := DropCounter("test.point.a")
	c2 := DropCounter("test.point.a")
	if c1 != c2 {
		t.Fatal("DropCounter must return the same instance for the same name")
	}

	c1.Inc()
	c1.Add(4)
	if got := c2.Value(); got != 5 {
		t.Fatalf("Value = %d, want 5", got)
	}
}

// TestDropSnapshot_SortedAndIncludesCounts verifies the snapshot is sorted by
// name (stable output for logs/metrics) and reflects the recorded totals.
func TestDropSnapshot_SortedAndIncludesCounts(t *testing.T) {
	DropCounter("test.snap.zzz").Inc()
	DropCounter("test.snap.aaa").Add(2)

	snap := DropSnapshot()

	if !sort.SliceIsSorted(snap, func(i, j int) bool { return snap[i].Name < snap[j].Name }) {
		t.Fatalf("DropSnapshot not sorted by name: %+v", snap)
	}

	counts := make(map[string]uint64, len(snap))
	for _, d := range snap {
		counts[d.Name] = d.Count
	}
	if counts["test.snap.aaa"] != 2 {
		t.Errorf("test.snap.aaa = %d, want 2", counts["test.snap.aaa"])
	}
	if counts["test.snap.zzz"] != 1 {
		t.Errorf("test.snap.zzz = %d, want 1", counts["test.snap.zzz"])
	}
}

// TestReportDrops_LogsNewDrops verifies the reporter actually surfaces a drop:
// a recorded loss produces a log line naming the drop point within one interval.
// This is the whole point of the feature — the loss is observable, not silent.
func TestReportDrops_LogsNewDrops(t *testing.T) {
	var buf syncBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(prev)

	DropCounter("test.report.point").Add(3)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		ReportDrops(ctx, 5*time.Millisecond)
		close(done)
	}()

	deadline := time.After(2 * time.Second)
	for !strings.Contains(buf.String(), "test.report.point") {
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("reporter never logged the drop; output=%q", buf.String())
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	<-done

	out := buf.String()
	if !strings.Contains(out, "events dropped under backpressure") {
		t.Errorf("missing drop warning in output: %q", out)
	}
	if !strings.Contains(out, "total=3") {
		t.Errorf("expected total=3 in output: %q", out)
	}
}
