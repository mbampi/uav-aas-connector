package mqtt

import (
	"testing"

	"uav-aas-connector/pkg/core"
)

// TestShardFor_StableAndInRange verifies the routing invariant Fix 1 relies on:
// a given property always maps to the same shard (so its updates stay ordered),
// and the result is always a valid shard index.
func TestShardFor_StableAndInRange(t *testing.T) {
	const n = numWorkers
	events := []*core.AASUpdateEvent{
		{SubmodelID: "Navigation", PropertyID: "GlobalPosition.Latitude"},
		{SubmodelID: "Navigation", PropertyID: "GlobalPosition.Longitude"},
		{SubmodelID: "Status", PropertyID: "Battery.Voltage"},
		{SubmodelID: "", PropertyID: ""},
	}

	for _, ev := range events {
		first := shardFor(ev, n)
		if first < 0 || first >= n {
			t.Fatalf("shardFor(%q/%q) = %d, out of range [0,%d)", ev.SubmodelID, ev.PropertyID, first, n)
		}
		// Same property must always route to the same shard.
		for i := 0; i < 100; i++ {
			if got := shardFor(ev, n); got != first {
				t.Fatalf("shardFor not stable for %q/%q: got %d then %d", ev.SubmodelID, ev.PropertyID, first, got)
			}
		}
	}
}
