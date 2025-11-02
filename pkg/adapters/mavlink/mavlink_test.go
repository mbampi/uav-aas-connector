package mavlink

import (
	"testing"
	"uav-aas-connector/pkg/config"

	"os"
	"path/filepath"
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

func TestLoadMapping_Success(t *testing.T) {
	// Create a temporary mapping file
	yamlContent := `
mappings:
  - sourceMessage: "GPS_RAW_INT"
    sourceField: "Lat"
    targetAASSubmodel: "Navigation"
    targetAASProperty: "Latitude"
    scale: "0.0000001"
`
	dir := t.TempDir()
	path := filepath.Join(dir, "mapping.yaml")
	if err := os.WriteFile(path, []byte(yamlContent), 0o644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}

	// Expected config
	cfg := &config.MappingConfig{
		Mappings: []config.MappingEntry{
			{
				SourceMessage:     "GPS_RAW_INT",
				SourceField:       "Lat",
				TargetAASSubmodel: "Navigation",
				TargetAASProperty: "Latitude",
				Scale:             "0.0000001",
			},
		},
	}

	loaded, err := loadMapping(path)
	if err != nil {
		t.Fatalf("loadMapping returned error: %v", err)
	}
	if loaded == nil {
		t.Fatalf("expected non-nil MappingConfig")
	}
	if len(loaded.Mappings) != 1 {
		t.Fatalf("expected 1 mapping, got %d", len(loaded.Mappings))
	}
	m := loaded.Mappings[0]
	if m.SourceMessage != cfg.Mappings[0].SourceMessage ||
		m.SourceField != cfg.Mappings[0].SourceField ||
		m.TargetAASSubmodel != cfg.Mappings[0].TargetAASSubmodel ||
		m.TargetAASProperty != cfg.Mappings[0].TargetAASProperty ||
		m.Scale != cfg.Mappings[0].Scale {
		t.Fatalf("loaded mapping does not match original: got %+v want %+v", m, cfg.Mappings[0])
	}
}

func TestLoadMapping_FileNotFound(t *testing.T) {
	_, err := loadMapping("/path/that/does/not/exist.yaml")
	if err == nil {
		t.Fatalf("expected error for non-existent file, got nil")
	}
}

func TestLoadMapping_InvalidYAML(t *testing.T) {
	// Write invalid YAML content
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte("::: not valid yaml :::"), 0o644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}

	_, err := loadMapping(path)
	if err == nil {
		t.Fatalf("expected error when decoding invalid YAML, got nil")
	}
}
