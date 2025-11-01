package mavlink

import (
	"reflect"
	"testing"

	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
)

// Test that extractFields returns a non-empty map for a common dialect message.
func TestExtractFields_Heartbeat_NotEmpty(t *testing.T) {
	a := &MavlinkAdapter{}
	msg := &common.MessageHeartbeat{}

	fields := a.extractFields(msg)
	if len(fields) == 0 {
		t.Fatalf("expected extractFields to return at least one field, got 0")
	}
}

// Test that the reflection-based message name resolution yields the Go type name.
func TestMessageNameReflection(t *testing.T) {
	msg := &common.MessageHeartbeat{}
	tpe := reflect.TypeOf(msg)
	if tpe.Kind() != reflect.Ptr {
		t.Fatalf("expected pointer kind, got %v", tpe.Kind())
	}
	name := tpe.Elem().Name()
	if name == "" {
		t.Fatalf("expected non-empty type name")
	}
	// Basic sanity: should equal "MessageHeartbeat"
	if name != "MessageHeartbeat" {
		t.Fatalf("unexpected reflected message name: %s", name)
	}
}

func TestGoTypeToMavName(t *testing.T) {
	cases := map[string]string{
		"MessageHeartbeat":   "HEARTBEAT",
		"MessageGpsRawInt":   "GPS_RAW_INT",
		"MessageGlobalPos":   "GLOBAL_POS",
		"MessageMessageTest": "MESSAGE_TEST",
		"MessageGPSRawInt":   "GPS_RAW_INT",
	}
	for in, want := range cases {
		got := goTypeToMavName(in)
		if got != want {
			t.Fatalf("goTypeToMavName(%s) = %s; want %s", in, got, want)
		}
	}
}
