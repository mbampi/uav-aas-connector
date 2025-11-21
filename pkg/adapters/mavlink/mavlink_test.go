package mavlink

import (
	"testing"
	"time"

	"uav-aas-connector/pkg/config"
	"uav-aas-connector/pkg/core"

	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/frame"
	"github.com/stretchr/testify/assert"
)

// FakeMessage mocks a MAVLink message struct.
// Reflection in the adapter will see fields "ValInt" and "ValFloat".
type FakeMessage struct {
	ValInt   int32
	ValFloat float32
}

func (m *FakeMessage) GetID() uint32 { return 0x999 } // Arbitrary ID

func TestGoTypeToMavName(t *testing.T) {
	// ... [Keep existing tests] ...
	cases := []struct {
		in   string
		want string
	}{
		{"MessageGpsRawInt", "GPS_RAW_INT"},
		{"FakeMessage", "FAKE_MESSAGE"},
	}
	for _, c := range cases {
		if got := goTypeToMavName(c.in); got != c.want {
			t.Errorf("goTypeToMavName(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestApplyTransform(t *testing.T) {
	a := &MavlinkAdapter{}
	_, err := a.applyTransform(float64(10), "2")
	assert.NoError(t, err)
}

// TestHandleFrame_Integration verifies the logic from extracting a field
// to generating an AAS event.
func TestHandleFrame_Integration(t *testing.T) {
	// 1. Setup Manual Configuration (Skip loading file)
	mappings := []config.MappingEntry{
		{
			SourceMessage:     "FAKE_MESSAGE",
			SourceField:       "ValInt",
			TargetAASSubmodel: "TestSubmodel",
			TargetAASProperty: "TestPropertyInt",
			Scale:             "", // No scale
		},
		{
			SourceMessage:     "FAKE_MESSAGE",
			SourceField:       "ValFloat",
			TargetAASSubmodel: "TestSubmodel",
			TargetAASProperty: "TestPropertyScaled",
			Scale:             "10.0", // Apply Scale
		},
	}

	// Manually populate rules map
	rules := make(map[string][]config.MappingEntry)
	rules["FAKE_MESSAGE"] = mappings

	outChan := make(chan *core.AASUpdateEvent, 5)

	adapter := &MavlinkAdapter{
		mappingRules: rules,
		outChan:      outChan,
	}

	// 2. Create a Dummy Frame containing our FakeMessage
	fakeMsg := &FakeMessage{
		ValInt:   100,
		ValFloat: 1.5,
	}

	// We use a minimal mock for the EventFrame if possible,
	// but gomavlib.EventFrame uses specific internal fields.
	// Ideally, we construct a frame.Frame.
	// Since gomavlib structs can be complex to mock exactly without a real node,
	// we inspect how handleFrame works: it calls e.Frame.GetMessage().

	// Mocking the Frame interface structure:
	// Note: gomavlib/v3/pkg/frame is an interface or struct? It is a valid interface holder.
	// We create a real V2Frame which is standard.
	frm := &frame.V2Frame{
		Message: fakeMsg,
	}

	evt := &gomavlib.EventFrame{
		Frame: frm,
	}

	// 3. Execute Logic
	adapter.handleFrame(evt)

	// 4. Assert Results
	// We expect 2 events (one for Int, one for Float)

	timeout := time.After(1 * time.Second)
	receivedEvents := 0

	for receivedEvents < 2 {
		select {
		case e := <-outChan:
			receivedEvents++
			switch e.PropertyID {
			case "TestPropertyInt":
				assert.Equal(t, "TestSubmodel", e.SubmodelID)
				assert.Equal(t, int32(100), e.Value) // No scale, original type/value preserved?
				// Wait, applyTransform returns original if no scale.
			case "TestPropertyScaled":
				assert.Equal(t, "TestSubmodel", e.SubmodelID)
				// 1.5 * 10.0 = 15.0
				assert.Equal(t, float64(15.0), e.Value)
			default:
				t.Errorf("Unexpected property ID: %s", e.PropertyID)
			}
		case <-timeout:
			t.Fatal("Timeout waiting for events")
		}
	}
}

// TestHandleFrame_UnknownMessage verifies ignored messages don't crash or send events
func TestHandleFrame_UnknownMessage(t *testing.T) {
	outChan := make(chan *core.AASUpdateEvent, 1)
	adapter := &MavlinkAdapter{
		mappingRules: make(map[string][]config.MappingEntry), // Empty rules
		outChan:      outChan,
	}

	fakeMsg := &FakeMessage{ValInt: 1}
	frm := &frame.V2Frame{Message: fakeMsg}
	evt := &gomavlib.EventFrame{Frame: frm}

	adapter.handleFrame(evt)

	select {
	case <-outChan:
		t.Fatal("Should not have received an event for unknown message")
	default:
		// Pass
	}
}
