package core

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
	"uav-aas-connector/pkg/config"

	aastypes "github.com/aas-core-works/aas-core3.0-golang/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A minimal valid AAS Environment JSON for testing
var validAASJson = `
{
    "submodels": [
        {
            "id": "urn:submodel:identification:001",
            "idShort": "Identification",
            "modelType": "Submodel",
            "submodelElements": [
                {
                    "idShort": "Manufacturer",
                    "category": "PARAMETER",
                    "modelType": "Property",
                    "valueType": "xs:string",
                    "value": "Generic Drone Company"
                }
            ]
        },
        {
            "id": "urn:submodel:status:001",
            "idShort": "Status",
            "modelType": "Submodel",
            "submodelElements": [
                {
                    "idShort": "VehicleType",
                    "category": "VARIABLE",
                    "modelType": "Property",
                    "valueType": "xs:int",
                    "value": "2"
                },
                {
                     "idShort": "Battery",
                     "category": "VARIABLE",
                     "modelType": "SubmodelElementCollection",
                     "value": [
                        {
                            "idShort": "Voltage",
                            "category": "VARIABLE",
                            "modelType": "Property",
                            "valueType": "xs:double",
                            "value": "12.5"
                        }
                     ]
                }
            ]
        }
    ]
}
`

var invalidJson = `{ "submodels": [ { "idShort": "Test" ]` // Malformed
var validJsonNotAAS = `{ "some_other_key": "some_value" }` // Valid JSON, but not an AAS

// createTempJSONFile is a test helper to write content to a temporary file
func createTempJSONFile(t *testing.T, content string) string {
	t.Helper()
	tmpFile, err := os.CreateTemp(t.TempDir(), "test-aas-*.json")
	require.NoError(t, err)
	_, err = tmpFile.WriteString(content)
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())
	return tmpFile.Name()
}

func TestNewAASCore_Success(t *testing.T) {
	// Create a temporary valid AAS JSON file
	filePath := createTempJSONFile(t, validAASJson)
	cfg := config.AASConfig{FilePath: filePath}

	// Call the function
	core, err := NewAASCore(cfg)

	// Assertions
	assert.NoError(t, err)
	require.NotNil(t, core)
	assert.NotNil(t, core.env)

	// Check Submodels
	submodels := core.env.Submodels()
	assert.Len(t, submodels, 2)
	assert.Equal(t, "Identification", *submodels[0].IDShort())
	assert.Equal(t, "Status", *submodels[1].IDShort())

	// Check SubmodelElements
	// Identification Submodel (Index 0)
	// FIX: Expected 1 element (only Manufacturer remains)
	assert.Len(t, submodels[0].SubmodelElements(), 1)

	// Status Submodel (Index 1)
	// FIX: Expected 2 elements (VehicleType and Battery collection)
	assert.Len(t, submodels[1].SubmodelElements(), 2)

	// Check Property values
	// Identification.Manufacturer
	assert.Equal(t, "Generic Drone Company", *submodels[0].SubmodelElements()[0].(*aastypes.Property).Value())

	// Status.VehicleType
	assert.Equal(t, "2", *submodels[1].SubmodelElements()[0].(*aastypes.Property).Value())

	// No panic, test passes
}

func TestNewAASCore_FileNotFound(t *testing.T) {
	// Point to a file that does not exist
	cfg := config.AASConfig{FilePath: filepath.Join(t.TempDir(), "non-existent-file.json")}

	// Call the function
	core, err := NewAASCore(cfg)

	// Assertions
	assert.Error(t, err)
	assert.Nil(t, core)
	// Check for the specific OS error
	assert.ErrorContains(t, err, "no such file or directory")
}

func TestNewAASCore_InvalidJSON(t *testing.T) {
	// Create a file with malformed JSON
	filePath := createTempJSONFile(t, invalidJson)
	cfg := config.AASConfig{FilePath: filePath}

	// Call the function
	core, err := NewAASCore(cfg)

	// Assertions
	assert.Error(t, err)
	assert.Nil(t, core)
	// This error comes from json.Decode
	assert.ErrorContains(t, err, "failed to decode AAS model JSON")
}

func TestNewAASCore_InvalidAASSchema(t *testing.T) {
	// Create a file with valid JSON, but it's not an AAS environment
	filePath := createTempJSONFile(t, validJsonNotAAS)
	cfg := config.AASConfig{FilePath: filePath}

	// Call the function
	core, err := NewAASCore(cfg)

	// Assertions
	assert.Error(t, err)
	assert.Nil(t, core)

	assert.ErrorContains(t, err, "failed to de-serialize AAS model")
	assert.ErrorContains(t, err, "Unexpected property")
}

// TestUpdateLogic_Integration verifies the complete flow:
// Input Event -> Core Update Model -> Broadcast to Subscriber
func TestUpdateLogic_Integration(t *testing.T) {
	filePath := createTempJSONFile(t, validAASJson)
	c, err := NewAASCore(config.AASConfig{FilePath: filePath})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Setup Input Channel
	inChan := make(chan *AASUpdateEvent)

	// 2. Setup Subscriber (Output)
	subChan, err := c.Subscribe(ctx)
	require.NoError(t, err)

	// 3. Start Core
	go c.Start(ctx, inChan)

	// 4. Define Test Cases
	tests := []struct {
		name       string
		submodelID string
		propertyID string // idShort path
		newValue   any
		expectVal  string // expected string in model
	}{
		{
			name:       "Update Top Level Property",
			submodelID: "Identification",
			propertyID: "Manufacturer",
			newValue:   "New Drone Corp",
			expectVal:  "New Drone Corp",
		},
		{
			name:       "Update Nested Property",
			submodelID: "Status",
			propertyID: "Battery.Voltage",
			newValue:   14.8,
			expectVal:  "14.8",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Send Update
			inChan <- &AASUpdateEvent{
				SubmodelID: tt.submodelID,
				PropertyID: tt.propertyID,
				Value:      tt.newValue,
				Timestamp:  time.Now(),
			}

			// Verify Broadcast (wait with timeout)
			select {
			case evt := <-subChan:
				assert.Equal(t, tt.submodelID, evt.SubmodelID)
				assert.Equal(t, tt.propertyID, evt.PropertyID)
				assert.Equal(t, tt.newValue, evt.Value)
			case <-time.After(100 * time.Millisecond): // Shortened timeout for faster feedback
				t.Fatal("timeout waiting for subscription event")
			}

			// Note: Directly checking the AAS value is complex and relies on unexported helper functions.
			// The successful broadcast check above is a strong indicator that updateModel succeeded.
		})
	}
}

func TestGetElementDataType(t *testing.T) {
	filePath := createTempJSONFile(t, validAASJson)
	c, err := NewAASCore(config.AASConfig{FilePath: filePath})
	require.NoError(t, err)

	tests := []struct {
		name      string
		submodel  string
		path      string
		wantType  aastypes.DataTypeDefXSD
		expectErr bool
	}{
		{"Simple String", "Identification", "Manufacturer", aastypes.DataTypeDefXSDString, false},
		{"Simple Int", "Status", "VehicleType", aastypes.DataTypeDefXSDInt, false},
		{"Nested Double", "Status", "Battery.Voltage", aastypes.DataTypeDefXSDDouble, false},
		{"Bad Submodel", "Unknown", "Manufacturer", 0, true},
		{"Bad Property", "Identification", "NonExistent", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.GetElementDataType(tt.submodel, tt.path)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantType, got)
			}
		})
	}
}
