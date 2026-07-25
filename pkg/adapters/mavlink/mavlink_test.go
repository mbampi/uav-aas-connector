package mavlink

import (
	"reflect"
	"testing"
	"time"

	"uav-aas-connector/pkg/config"
	"uav-aas-connector/pkg/core"

	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/frame"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// FakeMessage mocks a MAVLink message struct.
// Reflection in the adapter will see fields "ValInt" and "ValFloat".
type FakeMessage struct {
	ValInt   int32
	ValFloat float32
}

func (m *FakeMessage) GetID() uint32 { return 0x999 } // Arbitrary ID

// FakeArrayMessage exercises array-indexed field extraction, e.g. "Motors[2]".
type FakeArrayMessage struct {
	Motors [4]uint16
}

func (m *FakeArrayMessage) GetID() uint32 { return 0x998 }

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

// TestNormalizeReflectValue locks the type-standardization contract that
// extractField now applies directly: signed ints -> int64, unsigned -> uint64,
// everything else unchanged.
func TestNormalizeReflectValue(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want any
	}{
		{"int8 -> int64", int8(7), int64(7)},
		{"int32 -> int64", int32(-5), int64(-5)},
		{"uint8 -> uint64", uint8(200), uint64(200)},
		{"uint32 -> uint64", uint32(4000000000), uint64(4000000000)},
		{"float32 unchanged", float32(1.5), float32(1.5)},
		{"float64 unchanged", float64(2.5), float64(2.5)},
		{"string unchanged", "hi", "hi"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, normalizeReflectValue(reflect.ValueOf(c.in)))
		})
	}
}

// TestExtractField_ArrayIndexNormalized verifies the array-indexed path also
// normalizes its element type (uint16 -> uint64).
func TestExtractField_ArrayIndexNormalized(t *testing.T) {
	adapter := &MavlinkAdapter{fieldIndexCache: make(map[reflect.Type]map[string]int)}
	msg := &FakeArrayMessage{Motors: [4]uint16{10, 20, 30, 40}}

	v, ok := adapter.extractField(msg, "Motors[2]")
	require.True(t, ok)
	assert.Equal(t, uint64(30), v)
}

func TestApplyTransform(t *testing.T) {
	a := &MavlinkAdapter{}

	// Test with scale (hasScale = true)
	resScaled, err := a.applyTransform(float64(10), 2.0, true)
	assert.NoError(t, err)
	assert.Equal(t, float64(20.0), resScaled)

	// Test without scale (hasScale = false)
	resUnscaled, err := a.applyTransform(float64(10), 0.0, false)
	assert.NoError(t, err)
	assert.Equal(t, float64(10), resUnscaled)
}

// TestHandleFrame_Integration verifies the logic from extracting a field
// to generating an AAS event.
func TestHandleFrame_Integration(t *testing.T) {
	// Setup Manual Configuration (Skip loading file)
	// Manually populate rules map
	rules := make(map[string][]mavlinkRule)

	rules["FAKE_MESSAGE"] = []mavlinkRule{
		{
			MappingEntry: config.MappingEntry{
				SourceMessage:     "FAKE_MESSAGE",
				SourceField:       "ValInt",
				TargetAASSubmodel: "TestSubmodel",
				TargetAASProperty: "TestPropertyInt",
				Scale:             "",
			},
			hasScale: false,
			fullPath: "TestPropertyInt", // Pre-calculated path
		},
		{
			MappingEntry: config.MappingEntry{
				SourceMessage:     "FAKE_MESSAGE",
				SourceField:       "ValFloat",
				TargetAASSubmodel: "TestSubmodel",
				TargetAASProperty: "TestPropertyScaled",
				Scale:             "10.0",
			},
			parsedScale: 10.0,
			hasScale:    true,
			fullPath:    "TestPropertyScaled", // Pre-calculated path
		},
	}

	outChan := make(chan *core.AASUpdateEvent, 5)
	adapter := &MavlinkAdapter{
		mappingRules:    rules,
		outChan:         outChan,
		typeNameCache:   make(map[reflect.Type]string),
		fieldIndexCache: make(map[reflect.Type]map[string]int),
	}

	// Create a Dummy Frame containing our FakeMessage
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

	// Execute Logic
	adapter.handleFrame(evt)

	// Assert Results
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
				assert.Equal(t, int64(100), e.Value)
			case "TestPropertyScaled":
				assert.Equal(t, "TestSubmodel", e.SubmodelID)
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
		mappingRules:    make(map[string][]mavlinkRule),
		outChan:         outChan,
		typeNameCache:   make(map[reflect.Type]string),
		fieldIndexCache: make(map[reflect.Type]map[string]int),
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
