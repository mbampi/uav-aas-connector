package core

import (
	"sync"

	aastypes "github.com/aas-core-works/aas-core3.0-golang/types"
)

// findSubmodel finds a submodel by its idShort.
func findSubmodel(env aastypes.IEnvironment, idShort string) aastypes.ISubmodel {
	for _, sm := range env.Submodels() {
		if sm.IDShort() != nil && *sm.IDShort() == idShort {
			return sm
		}
	}
	return nil
}

// findSubmodelByID finds a submodel by its global ID.
func findSubmodelByID(env aastypes.IEnvironment, id string) aastypes.ISubmodel {
	for _, sm := range env.Submodels() {
		if sm.ID() == id {
			return sm
		}
	}
	return nil
}

// buildPropertyMap creates a flattened, O(1) lookup map of all properties.
func buildPropertyMap(env aastypes.IEnvironment) map[string]aastypes.IProperty {
	m := make(map[string]aastypes.IProperty)
	for _, sm := range env.Submodels() {
		if sm.IDShort() == nil {
			continue
		}
		smID := *sm.IDShort()
		flattenProperties(m, sm.SubmodelElements(), smID, "")
	}
	return m
}

// buildSubmodelLocks creates one lock per submodel so that property value
// reads/writes in independent submodels never contend with each other.
func buildSubmodelLocks(env aastypes.IEnvironment) map[string]*sync.RWMutex {
	locks := make(map[string]*sync.RWMutex)
	for _, sm := range env.Submodels() {
		if sm.IDShort() == nil {
			continue
		}
		locks[*sm.IDShort()] = &sync.RWMutex{}
	}
	return locks
}

// flattenProperties recursively flattens the submodel elements into a map for O(1) lookups.
func flattenProperties(m map[string]aastypes.IProperty, elements []aastypes.ISubmodelElement, smID string, prefix string) {
	for _, el := range elements {
		if el.IDShort() == nil {
			continue
		}

		name := *el.IDShort()
		path := name
		if prefix != "" {
			path = prefix + "." + name
		}

		fullKey := smID + "." + path

		if prop, ok := el.(aastypes.IProperty); ok {
			m[fullKey] = prop
		} else if coll, ok := el.(aastypes.ISubmodelElementCollection); ok {
			flattenProperties(m, coll.Value(), smID, path)
		}
	}
}
