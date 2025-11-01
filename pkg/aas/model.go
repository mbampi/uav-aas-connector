// Package aas defines the data structures for Asset Administration Shell (AAS) models.
package aas

// AssetAdministrationShell represents the top-level structure of an AAS.
type AssetAdministrationShell struct {
	ID               string           `json:"id"`
	IDShort          string           `json:"idShort"`
	AssetInformation AssetInformation `json:"assetInformation"`
	Submodels        []*Submodel      `json:"submodels"`
}

// AssetInformation holds metadata about the asset.
type AssetInformation struct {
	AssetKind     string `json:"assetKind"`
	GlobalAssetID string `json:"globalAssetId"`
}

// Submodel represents a Submodel in the AAS.
type Submodel struct {
	ID               string             `json:"id"`
	IDShort          string             `json:"idShort"`
	SubmodelElements []*SubmodelElement `json:"submodelElements"`
}

func (sm *Submodel) DeepCopy() *Submodel {
	if sm == nil {
		return nil
	}
	copy := &Submodel{
		ID:      sm.ID,
		IDShort: sm.IDShort,
	}
	if sm.SubmodelElements != nil {
		copy.SubmodelElements = make([]*SubmodelElement, len(sm.SubmodelElements))
		for i, elem := range sm.SubmodelElements {
			if elem != nil {
				elemCopy := *elem // Shallow copy of SubmodelElement
				copy.SubmodelElements[i] = &elemCopy
			}
		}
	}
	return copy
}

// SubmodelElement represents an element within a Submodel.
type SubmodelElement struct {
	IDShort   string `json:"idShort"`
	ModelType string `json:"modelType"`
	ValueType string `json:"valueType"`
	Value     any    `json:"value"` // Use 'any' for simplicity
	Unit      string `json:"unit,omitempty"`
	// ... other fields like category, description
}

// AASModel represents the entire AAS model, including all shells and submodels.
type AASModel struct {
	Shells    []*AssetAdministrationShell `json:"assetAdministrationShells"`
	Submodels []*Submodel                 `json:"submodels"`
}
