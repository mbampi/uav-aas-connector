package coalesce

import (
	"context"
	"testing"
	"time"

	"uav-aas-connector/pkg/core"
	"uav-aas-connector/pkg/instrumentation"
)

func ev(sm, prop string, v any) *core.AASUpdateEvent {
	return &core.AASUpdateEvent{SubmodelID: sm, PropertyID: prop, Value: v, Timestamp: time.Now()}
}

// TestPutCoalescesToLatestPerProperty verifies repeated updates to one property
// collapse to the latest value, and that each supersession is counted.
func TestPutCoalescesToLatestPerProperty(t *testing.T) {
	counter := instrumentation.CoalescedCounter("test.coalesce.latest")
	before := counter.Value()

	c := New(counter)
	c.Put(ev("Nav", "Lat", 1.0))
	c.Put(ev("Nav", "Lat", 2.0))
	c.Put(ev("Nav", "Lat", 3.0))

	batch := c.WaitDrain(context.Background())
	if len(batch) != 1 {
		t.Fatalf("len(batch) = %d, want 1 (coalesced to latest)", len(batch))
	}
	if batch[0].Value != 3.0 {
		t.Errorf("Value = %v, want 3.0 (latest)", batch[0].Value)
	}
	if got := counter.Value() - before; got != 2 {
		t.Errorf("coalesced count = %d, want 2 (two supersessions)", got)
	}
}

// TestWaitDrainReturnsAllPendingProperties verifies distinct properties are all
// returned (coalescing is per-property, not across the whole buffer).
func TestWaitDrainReturnsAllPendingProperties(t *testing.T) {
	c := New(nil) // nil counter must be tolerated
	c.Put(ev("Nav", "Lat", 1.0))
	c.Put(ev("Nav", "Lon", 2.0))
	c.Put(ev("Status", "Battery", 3.0))

	if batch := c.WaitDrain(context.Background()); len(batch) != 3 {
		t.Fatalf("len(batch) = %d, want 3", len(batch))
	}
}

// TestWaitDrainUnblocksOnPut verifies a consumer blocked with nothing pending is
// woken by a Put.
func TestWaitDrainUnblocksOnPut(t *testing.T) {
	c := New(nil)
	done := make(chan []*core.AASUpdateEvent, 1)
	go func() { done <- c.WaitDrain(context.Background()) }()

	time.Sleep(10 * time.Millisecond) // let the consumer block (test still valid if not)
	c.Put(ev("Nav", "Lat", 1.0))

	select {
	case batch := <-done:
		if len(batch) != 1 {
			t.Fatalf("len(batch) = %d, want 1", len(batch))
		}
	case <-time.After(time.Second):
		t.Fatal("WaitDrain did not wake on Put")
	}
}

// TestWaitDrainReturnsNilOnCancel verifies cancellation is the consumer's stop
// signal.
func TestWaitDrainReturnsNilOnCancel(t *testing.T) {
	c := New(nil)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan []*core.AASUpdateEvent, 1)
	go func() { done <- c.WaitDrain(ctx) }()

	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case batch := <-done:
		if batch != nil {
			t.Fatalf("batch = %v, want nil on cancel", batch)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitDrain did not return on cancel")
	}
}
