package mavlink

import (
	"testing"
	"time"

	"uav-aas-connector/pkg/config"
	"uav-aas-connector/pkg/core"

	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/frame"
)

// FakeMessage is a small test message used to simulate a MAVLink message.
// It implements the github.com/bluenviron/gomavlib/v3/pkg/message.Message
// interface by exposing GetID().
type FakeMessage struct {
	Value int32
}

func (m *FakeMessage) GetID() uint32 { return 0x123 }

func TestGoTypeToMavName(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"MessageGpsRawInt", "GPS_RAW_INT"},
		{"MessageHeartbeat", "HEARTBEAT"},
		{"FakeMessage", "FAKE_MESSAGE"},
		{"MessageABCxyz", "AB_CXYZ"},
	}

	for _, c := range cases {
		got := goTypeToMavName(c.in)
		if got != c.want {
			t.Fatalf("goTypeToMavName(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestApplyTransform(t *testing.T) {
	a := &MavlinkAdapter{}

	tests := []struct {
		name  string
		input any
		scale string
		want  any
	}{
		{"float64_no_scale", float64(1.5), "", float64(1.5)},
		{"int_scale", int32(10), "2", float64(20)},
		{"uint16_scale", uint16(3), "0.5", float64(1.5)},
		{"float32_scale", float32(2.5), "2", float64(5)},
	}

	for _, tt := range tests {
		got, err := a.applyTransform(tt.input, tt.scale)
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", tt.name, err)
		}
		if got != tt.want {
			t.Fatalf("%s: got %v (%T), want %v (%T)", tt.name, got, got, tt.want, tt.want)
		}
	}
}

func TestHandleFrame_MappingAndTransform(t *testing.T) {
	// Prepare adapter with a mapping for the FakeMessage type (which becomes FAKE_MESSAGE).
	ch := make(chan *core.AASUpdateEvent, 10)
	a := &MavlinkAdapter{
		outChan: ch,
		mappingRules: map[string][]config.MappingEntry{
			"FAKE_MESSAGE": {
				{
					SourceMessageName: "FAKE_MESSAGE",
					SourceField:       "Value",
					TargetAASSubmodel: "Nav",
					TargetAASProperty: "Speed",
					Scale:             "0.1",
				},
			},
		},
	}

	// Create a V2 frame that wraps our FakeMessage.
	msg := &FakeMessage{Value: 100}
	v2 := frame.V2Frame{
		SequenceNumber: 1,
		SystemID:       1,
		ComponentID:    1,
		Message:        msg,
	}

	evt := &gomavlib.EventFrame{Frame: &v2}

	// Call handleFrame (synchronously). It should push one update into the channel.
	a.handleFrame(evt)

	select {
	case upd := <-ch:
		if upd.SubmodelID != "Nav" {
			t.Fatalf("unexpected SubmodelID: %s", upd.SubmodelID)
		}
		if upd.PropertyID != "Speed" {
			t.Fatalf("unexpected PropertyID: %s", upd.PropertyID)
		}

		// Value should be scaled: 100 * 0.1 => 10.0
		if upd.Value != "10.0" {
			t.Fatalf("unexpected Value: %#v", upd.Value)
		}
		// Timestamp should be recent
		if time.Since(upd.Timestamp) > time.Second*5 {
			t.Fatalf("timestamp too old: %v", upd.Timestamp)
		}
	default:
		t.Fatalf("expected an update event but channel was empty")
	}
}
