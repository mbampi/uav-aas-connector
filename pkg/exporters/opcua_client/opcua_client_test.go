package opcua_client

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"uav-aas-connector/pkg/config"
	"uav-aas-connector/pkg/core"

	aastypes "github.com/aas-core-works/aas-core3.0-golang/types"
	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	"github.com/stretchr/testify/require"
)

// newUnreachableTestClient builds an opcua.Client configured to talk to a
// port nothing is listening on, so Connect() fails fast with "connection
// refused" instead of hanging.
func newUnreachableTestClient(t *testing.T) (*opcua.Client, error) {
	t.Helper()
	return opcua.NewClient("opc.tcp://127.0.0.1:1", opcua.SecurityMode(ua.MessageSecurityModeNone), opcua.AuthAnonymous())
}

const testAASJSON = `
{
    "submodels": [
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

func newTestCore(t *testing.T) *core.AASCore {
	t.Helper()
	tmpFile, err := os.CreateTemp(t.TempDir(), "test-aas-*.json")
	require.NoError(t, err)
	_, err = tmpFile.WriteString(testAASJSON)
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())

	c, err := core.NewAASCore(config.AASConfig{FilePath: filepath.Clean(tmpFile.Name())}, true)
	require.NoError(t, err)
	return c
}

func newTestExporter(t *testing.T) *Exporter {
	t.Helper()
	return &Exporter{
		core:                newTestCore(t),
		namespace:           2,
		typeCache:           make(map[string]aastypes.DataTypeDefXSD),
		nodeIDCache:         make(map[string]*ua.NodeID),
		maxConcurrentWrites: defaultMaxConcurrentWrites,
	}
}

// TestPrepareWriteValue_ConcurrentAccess exercises prepareWriteValue from many
// goroutines at once, across both a shared cache key (contended, cold-cache
// races on first fill) and distinct keys. networkWorker now dispatches
// batches to a pool of goroutines instead of a single serialized worker, so
// typeCache/nodeIDCache are no longer owned by one goroutine; run with
// `go test -race` to prove the cacheMu locking actually prevents a data race.
func TestPrepareWriteValue_ConcurrentAccess(t *testing.T) {
	e := newTestExporter(t)

	const goroutines = 50
	const itersPerGoroutine = 100

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < itersPerGoroutine; i++ {
				// Alternate between a shared key (contended cache fill) and
				// a goroutine-local key (distinct cache entries).
				var event *core.AASUpdateEvent
				if i%2 == 0 {
					event = &core.AASUpdateEvent{SubmodelID: "Status", PropertyID: "VehicleType", Value: int32(id)}
				} else {
					event = &core.AASUpdateEvent{SubmodelID: "Status", PropertyID: "Battery.Voltage", Value: float64(id)}
				}

				if _, err := e.prepareWriteValue(event); err != nil {
					errCh <- fmt.Errorf("goroutine %d: %w", id, err)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

// TestReconnectIfCurrent_SkipsWhenAlreadyReconnected verifies that a write
// goroutine which observed a stale connGen does not trigger a second,
// redundant reconnect once another goroutine has already recovered the
// connection. The exporter's client is left nil here: if reconnectIfCurrent
// incorrectly attempted to reconnect, it would panic dereferencing it,
// which is exactly the behavior this test guards against.
func TestReconnectIfCurrent_SkipsWhenAlreadyReconnected(t *testing.T) {
	e := &Exporter{connGen: 5}

	err := e.reconnectIfCurrent(context.Background(), 3 /* stale generation */)

	require.NoError(t, err)
	require.EqualValues(t, 5, e.connGeneration(), "generation must not change when the reconnect is skipped")
}

// TestReconnectIfCurrent_AttemptsWhenGenerationMatches verifies the
// complementary path: when the observed generation is still current, the
// goroutine does attempt a reconnect. A short-lived context is used so the
// exponential backoff loop in reconnect() exits quickly via ctx.Done()
// instead of retrying against the unreachable test endpoint.
func TestReconnectIfCurrent_AttemptsWhenGenerationMatches(t *testing.T) {
	client, err := newUnreachableTestClient(t)
	require.NoError(t, err)

	e := &Exporter{client: client, connGen: 0, endpoint: "opc.tcp://127.0.0.1:0"}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err = e.reconnectIfCurrent(ctx, 0 /* current generation */)

	// The endpoint is unreachable, so this must fail via the context
	// deadline rather than hang or panic; the generation must stay put.
	require.Error(t, err)
	require.EqualValues(t, 0, e.connGeneration())
}
