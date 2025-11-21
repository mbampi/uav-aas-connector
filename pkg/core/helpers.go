package core

import (
	"fmt"

	aastypes "github.com/aas-core-works/aas-core3.0-golang/types"
)

// Existing findSubmodel (by idShort)
func findSubmodel(env aastypes.IEnvironment, idShort string) aastypes.ISubmodel {
	if env.Submodels() == nil {
		return nil
	}
	for _, sm := range env.Submodels() {
		if sm.IDShort() != nil && *sm.IDShort() == idShort {
			return sm
		}
	}
	return nil
}

func findSubmodelByID(env aastypes.IEnvironment, id string) aastypes.ISubmodel {
	if env.Submodels() == nil {
		return nil
	}
	for _, sm := range env.Submodels() {
		if sm.ID() == id {
			return sm
		}
	}
	return nil
}

func findProperty(elements []aastypes.ISubmodelElement, path []string) (aastypes.IProperty, error) {
	if len(path) == 0 {
		return nil, fmt.Errorf("empty property path")
	}

	currentIDShort := path[0]
	var foundElement aastypes.ISubmodelElement
	for _, el := range elements {
		if el.IDShort() != nil && *el.IDShort() == currentIDShort {
			foundElement = el
			break
		}
	}

	if foundElement == nil {
		return nil, fmt.Errorf("element '%s' not found", currentIDShort)
	}

	if len(path) == 1 {
		if prop, ok := foundElement.(aastypes.IProperty); ok {
			return prop, nil
		}
		return nil, fmt.Errorf("element '%s' is not a Property", currentIDShort)
	}

	if smc, ok := foundElement.(aastypes.ISubmodelElementCollection); ok {
		return findProperty(smc.Value(), path[1:])
	}

	return nil, fmt.Errorf("element '%s' is not a Collection", currentIDShort)
}
