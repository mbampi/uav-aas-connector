package mqtt

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"uav-aas-connector/pkg/core"
)

// TestEncodePayload verifies the hand-rolled encoder produces valid JSON that
// round-trips for every value type the pipeline emits (numbers unmarshal back
// as float64), including a string that needs escaping.
func TestEncodePayload(t *testing.T) {
	ts := time.Date(2026, 7, 25, 16, 37, 14, 123456789, time.UTC)
	const wantTS = "2026-07-25T16:37:14.123456789Z"

	cases := []struct {
		name      string
		value     any
		wantValue any
	}{
		{"int64", int64(100), float64(100)},
		{"uint64", uint64(42), float64(42)},
		{"float64", float64(15.5), float64(15.5)},
		{"bool", true, true},
		{"string", "hello", "hello"},
		{"string with escapes", "a\"b\\c\nd", "a\"b\\c\nd"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			data := encodePayload(c.value, ts)

			var got map[string]any
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatalf("produced invalid JSON %q: %v", data, err)
			}
			if got["value"] != c.wantValue {
				t.Errorf("value = %#v, want %#v (json: %s)", got["value"], c.wantValue, data)
			}
			if got["timestamp"] != wantTS {
				t.Errorf("timestamp = %v, want %v", got["timestamp"], wantTS)
			}
		})
	}
}

// TestEncodePayload_NaNInfBecomeNull ensures non-JSON floats degrade to null
// rather than producing an unparseable payload.
func TestEncodePayload_NaNInfBecomeNull(t *testing.T) {
	for _, f := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		data := encodePayload(f, time.Now())

		var got map[string]any
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("produced invalid JSON for %v: %q: %v", f, data, err)
		}
		if got["value"] != nil {
			t.Errorf("value for %v = %#v, want nil", f, got["value"])
		}
	}
}

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
