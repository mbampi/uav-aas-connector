// Package coalesce provides a latest-value-per-property buffer between a fast
// producer (the core's broadcast) and a slow consumer (a network exporter).
// Memory is bounded by the number of properties, and the consumer always gets
// the freshest value.
//
// Usage is a producer calling Put and a single consumer looping on WaitDrain:
//
//	for {
//		batch := c.WaitDrain(ctx)
//		if batch == nil { // ctx canceled
//			return
//		}
//		for _, ev := range batch {
//			send(ev)
//		}
//	}
package coalesce

import (
	"context"
	"sync"

	"uav-aas-connector/pkg/core"
	"uav-aas-connector/pkg/instrumentation"
)

// Coalescer holds at most one pending event per property and wakes a consumer
// when new data arrives. It is safe for concurrent producers and one consumer.
type Coalescer struct {
	mu      sync.Mutex
	pending map[string]*core.AASUpdateEvent
	signal  chan struct{} // buffered(1) wake; internal to WaitDrain

	// coalesced counts events superseded before they were drained (a
	// backpressure signal). May be nil.
	coalesced *instrumentation.Counter
}

// New returns an empty Coalescer. coalesced, if non-nil, is incremented each
// time a Put supersedes an already-pending value for the same property.
func New(coalesced *instrumentation.Counter) *Coalescer {
	return &Coalescer{
		pending:   make(map[string]*core.AASUpdateEvent),
		signal:    make(chan struct{}, 1),
		coalesced: coalesced,
	}
}

// Put stores ev as the latest value for its property, superseding any pending
// value for the same property, and wakes a waiting consumer. It never blocks
// and never discards the newest value.
func (c *Coalescer) Put(ev *core.AASUpdateEvent) {
	key := ev.SubmodelID + "." + ev.PropertyID

	c.mu.Lock()
	if _, superseded := c.pending[key]; superseded && c.coalesced != nil {
		c.coalesced.Inc()
	}
	c.pending[key] = ev
	c.mu.Unlock()

	// A buffer of 1 means a wake is already pending; the consumer will drain
	// everything accumulated so far, so we don't need to signal again.
	select {
	case c.signal <- struct{}{}:
	default:
	}
}

// WaitDrain blocks until at least one event is pending, then returns the latest
// value for every pending property, in unspecified order. Because a property has
// at most one pending entry and WaitDrain calls are sequential, a property's
// updates are still delivered in order. It returns nil only when ctx is
// canceled, which is the consumer's signal to stop.
func (c *Coalescer) WaitDrain(ctx context.Context) []*core.AASUpdateEvent {
	for {
		if batch := c.drain(); batch != nil {
			return batch
		}
		select {
		case <-ctx.Done():
			return nil
		case <-c.signal:
			// New data may be pending; loop and drain it.
		}
	}
}

// drain removes and returns all pending events, or nil when empty.
func (c *Coalescer) drain() []*core.AASUpdateEvent {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.pending) == 0 {
		return nil
	}
	batch := make([]*core.AASUpdateEvent, 0, len(c.pending))
	for k, ev := range c.pending {
		batch = append(batch, ev)
		delete(c.pending, k)
	}
	return batch
}
