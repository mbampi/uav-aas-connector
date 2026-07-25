package instrumentation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// find returns the snapshot entry for stage, or nil if absent.
func find(snap []StageLatency, stage string) *StageLatency {
	for i := range snap {
		if snap[i].Stage == stage {
			return &snap[i]
		}
	}
	return nil
}

// TestStageStat_Aggregation exercises the aggregate directly (bypassing
// Record's time.Now) so count/mean/max are deterministic.
func TestStageStat_Aggregation(t *testing.T) {
	const stage = "TEST_AGG_STAGE"
	statFor(stage).observe(100)
	statFor(stage).observe(300)
	statFor(stage).observe(200)

	got := find(Snapshot(), stage)
	require.NotNil(t, got)
	assert.Equal(t, uint64(3), got.Count)
	assert.Equal(t, uint64(300), got.MaxNs)
	assert.InDelta(t, 200.0, got.MeanNs, 0.001)
}

// TestRecord_UpdatesAggregates verifies Record folds a sample into the
// aggregates. The trace is backdated so the measured latency is comfortably
// positive without asserting an exact (scheduler-dependent) value.
func TestRecord_UpdatesAggregates(t *testing.T) {
	require.NoError(t, Init(t.TempDir()))
	defer Close()

	const stage = "TEST_REC_STAGE"
	Record(stage, "prop", time.Now().Add(-5*time.Millisecond))

	got := find(Snapshot(), stage)
	require.NotNil(t, got)
	assert.Equal(t, uint64(1), got.Count)
	assert.Greater(t, got.MaxNs, uint64(time.Millisecond)) // ~5ms, allow slack
}

// TestRecord_SkipsNegativeLatency ensures a future-dated trace (now < trace)
// never creates or pollutes a stage aggregate.
func TestRecord_SkipsNegativeLatency(t *testing.T) {
	require.NoError(t, Init(t.TempDir()))
	defer Close()

	const stage = "TEST_FUTURE_STAGE"
	Record(stage, "prop", time.Now().Add(time.Hour))

	assert.Nil(t, find(Snapshot(), stage))
}
