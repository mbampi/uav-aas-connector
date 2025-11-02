package core

import (
	"os"
	"path/filepath"
	"testing"
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
            "description": [
                {
                    "language": "en",
                    "text": "Asset identification and basic information"
                }
            ],
            "submodelElements": [
                {
                    "idShort": "Manufacturer",
                    "category": "PARAMETER",
                    "modelType": "Property",
                    "valueType": "xs:string",
                    "value": "Generic Drone Company"
                },
                {
                    "idShort": "ProductName",
                    "category": "PARAMETER",
                    "modelType": "Property",
                    "valueType": "xs:string",
                    "value": "Quadcopter Model X1"
                }
            ]
        },
        {
            "id": "urn:submodel:status:001",
            "idShort": "Status",
            "modelType": "Submodel",
            "description": [
                {
                    "language": "en",
                    "text": "Current status and operational state"
                }
            ],
            "submodelElements": [
                {
                    "idShort": "VehicleType",
                    "category": "VARIABLE",
                    "modelType": "Property",
                    "valueType": "xs:int",
                    "value": "2",
                    "description": [
                        {
                            "language": "en",
                            "text": "MAVLink vehicle type (2 = Quadrotor)"
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
	assert.Len(t, core.env.Submodels(), 2)
	assert.Equal(t, "Identification", *core.env.Submodels()[0].IDShort())
	assert.Equal(t, "Status", *core.env.Submodels()[1].IDShort())
	// Check SubmodelElements
	assert.Len(t, core.env.Submodels()[0].SubmodelElements(), 2)
	assert.Len(t, core.env.Submodels()[1].SubmodelElements(), 1)
	// Check Property values
	assert.Equal(t, "Generic Drone Company", *core.env.Submodels()[0].SubmodelElements()[0].(*aastypes.Property).Value())
	assert.Equal(t, "Quadcopter Model X1", *core.env.Submodels()[0].SubmodelElements()[1].(*aastypes.Property).Value())
	assert.Equal(t, "2", *core.env.Submodels()[1].SubmodelElements()[0].(*aastypes.Property).Value())
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
