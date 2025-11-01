package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	// Define configuration fields here
	InputAdapter    AdapterConfig
	OutputExporters []ExporterConfig
	Mapping         MappingConfig
}

type AdapterConfig struct {
	// Define adapter configuration fields here
	Connection  string
	Address     string
	MappingFile string
}

type ExporterConfig struct {
	// Define exporter configuration fields here
}

// Mapping represents the mapping configuration for data points.
type MappingConfig struct {
	Entries []MappingEntry `json:"entries"`
}

// MappingEntry represents a single mapping from a data source to an AAS property.
type MappingEntry struct {
	SourceMessageName string `json:"sourceMessageName"` // e.g., "GPS_RAW_INT"
	TargetAASSubmodel string `json:"targetAASSubmodel"` // idShort e.g., "Navigation"
	TargetAASProperty string `json:"targetAASProperty"` // idShort e.g., "Latitude"
	Scale             string `json:"scale,omitempty"`   // e.g., "0.0000001"
}

func LoadConfig(path string) (*Config, error) {
	// Implementation for loading configuration from the specified path
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
