# UAV AAS Connector

- Flow: `Adapter -(Raw Data)-> Processing Core -(AAS Data)-> Exporter`
- Architecture:
  - Adapter: Ingests UAV data via various protocols (MavLink, HTTP, MQTT).
  - Processing Core: Transforms data into Asset Administration Shell (AAS) format.
  - Exporter: Sends processed AAS data to external systems.

## Components

### Input Adapters

- Collects data from UAVs (Unmanned Aerial Vehicles) through various protocols (e.g., MavLink, HTTP, MQTT).
- Connect to a data source (MAVLink), translate it into AAS format, based on mapping files.

### Processing Core

- Processes and transforms the ingested data into a format compatible with Asset Administration Shell (AAS) standards.
- On Start: It loads the quadcopter.json file to build its initial in-memory state. This state is a full Go struct representation of the AAS.
- In-bound: It subscribes to the rawChan from the Input Adapters.
- Logic: When it receives a RawDataPoint (e.g., HEARTBEAT), it uses the quadcopter.yaml mapping file to find the target.
- Action: Instead of just outputting a new message, it updates its internal in-memory AAS model. For example, it finds the "SystemStatus" property and changes its .value field.
- Out-bound (Two-Way):
  - Push (Events): It broadcasts an AASUpdateEvent to all subscribed Output Exporters to notify them of the specific change.
  - Pull (State): It provides methods (like GetSubmodel(id)) that OutputExporters can call at any time to get the full current state of any part of the AAS.

### Output Exporters

- Exports the processed data to AAS repositories or other target systems.

### Main Characteristics

- Modular architecture allowing easy addition of new adapters and exporters.
- Configurable via external configuration files.
- Designed for scalability and real-time data processing.
- Prioritize goroutine-safe code for high throughput
- Ensure zero-downtime
- Include logging/monitoring with zap or Prometheus for production readiness
- Graceful shutdown on interrupt signals
- Unit tests for reliability
