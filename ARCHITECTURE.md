# System Architecture & Context

## 1. System Identity

**Name:** UAV-AAS Connector
**Purpose:** A bridge application that ingests telemetry from UAVs (via MAVLink), maintains a live state representation using the Asset Administration Shell (AAS) standard, and exports this state via multiple industrial protocols (OPC UA, MQTT, HTTP, gRPC).
**Architecture Pattern:** Hexagonal Architecture (Ports and Adapters).

## 2. Directory Structure & Key Files

* **Entry Point:** `cmd/main.go` (Orchestrates initialization, signal handling, and dependency injection).
* **Domain Logic (Core):** `pkg/core/`
* `core.go`: Holds the AAS State (`env`), handles `AASUpdateEvent` processing, and manages pub/sub channels.
* `helpers.go`: Traversal logic for the AAS component hierarchy.


* **Ports (Interfaces):**
* Input: `pkg/adapters/adapter.go` (`InputAdapter` interface).
* Output: `pkg/exporters/exporter.go` (`OutputExporter` interface).


* **Adapters (Inputs):** `pkg/adapters/mavlink/`
* Uses reflection to map incoming MAVLink structs to AAS properties based on YAML rules.


* **Exporters (Outputs):** `pkg/exporters/`
* `opcua_server/`: Hosts a local OPC UA server representing the AAS.
* `opcua_client/`: Pushes updates to a remote OPC UA server.
* `http/`: Exposes a REST API to query current state (Pull model).
* `mqtt/`: Publishes updates to an MQTT broker.
* `grpc/`: Streams updates via Protobuf.


* **Configuration:** `pkg/config/` & `config.yaml`.
* **Instrumentation:** `pkg/instrumentation/` (Latency tracking).

## 3. Data Flow Architecture

The data flow is unidirectional and event-driven:

`[Source Protocol] -> [Input Adapter] -> [AAS Core] -> [Output Exporters] -> [Sink Protocols]`

### Step 1: Ingestion (Input Adapter)

* **Component:** `pkg/adapters/mavlink`
* **Mechanism:** Listens on UDP/TCP/Serial.
* **Logic:**
1. Receives a Frame (e.g., `GLOBAL_POSITION_INT`).
2. Looks up mapping rules in `config/mappings/mavlink_ops.yaml`.
3. Uses reflection (`reflect` package) to extract the field value.
4. Applies scaling (e.g., `1e-7` for Lat/Lon).
5. Wraps data in `core.AASUpdateEvent`.
6. Sends event to `rawChan`.



### Step 2: State Management (Core)

* **Component:** `pkg/core/core.go`
* **State:** Holds `aastypes.IEnvironment` (the AAS Model loaded from `config/aas/*.json`).
* **Concurrency:** `sync.RWMutex` protects the AAS model.
* **Logic:**
1. Consumes `AASUpdateEvent` from `rawChan`.
2. Updates the specific `IProperty` value in the in-memory AAS model (`updateModel`).
3. Broadcasts the event to all subscriber channels.



### Step 3: Egress (Output Exporters)

* **Component:** `pkg/exporters/*`
* **Mechanism:** Most exporters subscribe to the Core's broadcast channel.
* **Logic:**
1. `Start(ctx, inChan)` is called.
2. Consumes `AASUpdateEvent` from `inChan`.
3. Converts the generic `event.Value` (type `any`) to the specific protocol format.
4. Transmits data.



## 4. Key Data Structures

### `core.AASUpdateEvent`

The universal data packet passed between adapters, core, and exporters.

```go
type AASUpdateEvent struct {
    SubmodelID string    // idShort of the Submodel (e.g., "Navigation")
    PropertyID string    // Dot-notation path (e.g., "GlobalPosition.Latitude")
    Value      any       // Raw value (float64, int32, string, bool)
    Timestamp  time.Time // Capture time
}

```

### `config.MappingEntry`

Defines how external data maps to the internal AAS model.

```go
type MappingEntry struct {
    SourceMessage     string // External message ID (e.g., "HEARTBEAT")
    SourceField       string // External field name (e.g., "Type")
    TargetAASSubmodel string // Internal Submodel idShort
    TargetAASProperty string // Internal Property idShort
    Scale             string // Optional multiplier
}

```

## 5. Critical Implementation Details

* **Type System:**
* The `AASUpdateEvent.Value` is `any`.
* Exporters must perform type assertions (switch statements) to handle `float64`, `int`, `string`, etc., appropriately for their protocol.
* See `pkg/exporters/grpc/grpc.go` or `pkg/adapters/mavlink/mavlink.go` (extractField) for examples of type handling.


* **AAS Library:**
* Uses `github.com/aas-core-works/aas-core3.0-golang`.
* The model is strictly typed. Updating a property requires traversing the `SubmodelElements` to find the `IProperty`.


* **Performance:**
* **Reflection Caching:** The MAVLink adapter caches reflection types and field indices (`typeNameCache`, `fieldIndexCache`) to minimize runtime overhead.
* **Batching:** The OPC UA Client exporter (`pkg/exporters/opcua_client`) implements opportunistic batching to reduce network calls.
* **Latency Logging:** `pkg/instrumentation` writes high-frequency timestamps to disk for performance analysis.


* **Concurrency:**
* Main loop runs in `cmd/main.go`.
* Each Adapter and Exporter runs in its own Goroutine.
* Core logic (`Start` and `Subscribe`) manages channel distribution.
* Shutdown is handled via `context.Context` cancellation.



## 6. Extension Guide for Agents

### How to add a new Input Adapter

1. Create `pkg/adapters/newprotocol/`.
2. Implement `adapters.InputAdapter` interface.
3. Define a new struct in `config.AdapterConfig` or parse a generic config.
4. In `Start()`, loop incoming data, convert to `core.AASUpdateEvent`, and push to `outChan`.
5. Register in `pkg/adapters/adapter.go`.

### How to add a new Output Exporter

1. Create `pkg/exporters/newprotocol/`.
2. Implement `exporters.OutputExporter` interface.
3. In `Start()`, loop over `<-chan *core.AASUpdateEvent`.
4. Register in `pkg/exporters/exporter.go` (factory switch).
5. Add configuration in `pkg/config/loader.go`.

### How to modify the Data Model

1. Modify `config/aas/quad_op.json` (The AAS definition).
2. Modify `config/mappings/*.yaml` to map new telemetry to the new AAS elements.
3. Restart the application (Model is loaded at runtime).

## 7. Known Dependencies

* **MAVLink:** `github.com/bluenviron/gomavlib/v3`
* **OPC UA:** `github.com/gopcua/opcua` and `github.com/awcullen/opcua` (Server implementation).
* **MQTT:** `github.com/eclipse/paho.mqtt.golang`
* **gRPC:** `google.golang.org/grpc`
* **AAS:** `github.com/aas-core-works/aas-core3.0-golang`