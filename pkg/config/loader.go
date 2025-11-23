package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the overall configuration structure for the UAV AAS Connector.
type Config struct {
	InputAdapter    AdapterConfig             `yaml:"adapter"`
	OutputExporters map[string]ExporterConfig `yaml:"exporters"`
	AAS             AASConfig                 `yaml:"aas"`
	Advanced        AdvancedConfig            `yaml:"advanced"`
}

// AASConfig represents the configuration specific to the Asset Administration Shell.
type AASConfig struct {
	FilePath string `yaml:"filePath"` // Path to the AAS model file (JSON)
}

// AdapterConfig represents the configuration for an input adapter.
type AdapterConfig struct {
	Type        string `yaml:"type"` // e.g., "mavlink"
	Connection  string `yaml:"connection"`
	Address     string `yaml:"address"` // can be IP:port for UDP/TCP or device path for serial
	MappingFile string `yaml:"mappingFile"`
}

// ExporterConfig represents the configuration for an output exporter.
type ExporterConfig struct {
	Type    string `yaml:"type,omitempty"` // e.g., "opcua", "http"
	Enabled bool   `yaml:"enabled"`
	Port    int    `yaml:"port"`
	Host    string `yaml:"host,omitempty"`

	// MQTT specific
	ClientID    string `yaml:"clientID,omitempty"`
	TopicPrefix string `yaml:"topicPrefix,omitempty"`
}

// AdvancedConfig represents advanced configuration options.
type AdvancedConfig struct {
	MetricsPath string `yaml:"metrics_path"` // e.g., "/metrics"
	LogLevel    string `yaml:"log_level"`    // e.g., "debug", "info", "warn", "error"
}

// MappingConfig represents the mapping from data source fields to AAS properties.
type MappingConfig struct {
	Mappings []MappingEntry `yaml:"mappings"`
}

// MappingEntry represents a single mapping from a data source to an AAS property.
type MappingEntry struct {
	SourceMessage       string `yaml:"sourceMessage"`                 // e.g., "GPS_RAW_INT"
	SourceField         string `yaml:"sourceField"`                   // e.g., "Lat"
	TargetAASSubmodel   string `yaml:"targetAASSubmodel"`             // idShort e.g., "Navigation"
	TargetAASCollection string `yaml:"targetAASCollection,omitempty"` // idShort e.g., "Position"
	TargetAASProperty   string `yaml:"targetAASProperty"`             // idShort e.g., "Latitude"
	Scale               string `yaml:"scale,omitempty"`               // e.g., "0.0000001"

	Path string `yaml:",omitempty"` // cached full dot-notation path
}

// GetPath constructs the full AAS property path (dot-notation).
// This keeps the Adapter logic simple and generic.
func (m MappingEntry) GetPath() string {
	if m.Path != "" {
		return m.Path
	}

	if m.TargetAASCollection != "" {
		path := fmt.Sprintf("%s.%s", m.TargetAASCollection, m.TargetAASProperty)
		m.Path = path
		return m.Path
	}
	m.Path = m.TargetAASProperty
	return m.Path
}

func LoadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	var cfg Config
	if err := yaml.NewDecoder(file).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("failed to decode config file: %w", err)
	}

	for key, ec := range cfg.OutputExporters {
		ec.Type = key
		cfg.OutputExporters[key] = ec
	}

	return &cfg, nil
}
