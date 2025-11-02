package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the overall configuration structure for the UAV AAS Connector.
type Config struct {
	InputAdapter    AdapterConfig
	OutputExporters []ExporterConfig
	AAS             AASConfig
}

// AASConfig represents the configuration specific to the Asset Administration Shell.
type AASConfig struct {
	FilePath string // Path to the AAS model file (JSON)
}

// AdapterConfig represents the configuration for an input adapter.
type AdapterConfig struct {
	Type        string // e.g., "mavlink"
	Connection  string
	Address     string // can be IP:port for UDP/TCP or device path for serial
	MappingFile string
}

// ExporterConfig represents the configuration for an output exporter.
type ExporterConfig struct {
	Type string // e.g., "opcua", "http"
	Port int
}

// MappingConfig represents the mapping from data source fields to AAS properties.
type MappingConfig struct {
	Entries []MappingEntry `json:"entries"`
}

// MappingEntry represents a single mapping from a data source to an AAS property.
type MappingEntry struct {
	SourceMessageName string `json:"sourceMessageName"` // e.g., "GPS_RAW_INT"
	SourceField       string `json:"sourceField"`       // e.g., "Lat"
	TargetAASSubmodel string `json:"targetAASSubmodel"` // idShort e.g., "Navigation"
	TargetAASProperty string `json:"targetAASProperty"` // idShort e.g., "Latitude"
	Scale             string `json:"scale,omitempty"`   // e.g., "0.0000001"
}

// LoadConfig loads the configuration from the specified YAML file.
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

	return &cfg, nil
}
