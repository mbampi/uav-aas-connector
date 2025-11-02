# UAV AAS Connector

A Go service that connects to a UAV (via MAVLink), translates its data, and updates an in-memory Asset Administration Shell (AAS) model.

It allows external systems to monitor the UAV's telemetry through a standardized AAS interface.

## How It Works

The application runs as a single process with three main components:

### Input Adapter

- Connects to a UAV data source, specified in the config.yaml.

- Currently, a Mavlink adapter is implemented, which can connect to a UAV via UDP, TCP, or Serial.

- It uses the config/mappings/mavlink.yaml file to translate raw MAVLink messages (e.g., GLOBAL_POSITION_INT) into an internal AASUpdateEvent.

### Processing Core

- On startup, it loads the complete AAS model structure from config/aas/quadcopter.json into memory.

- It listens for AASUpdateEvent messages from the adapter.

- When an event is received (e.g., a new latitude value), it finds the corresponding property in the in-memory AAS model and updates its value.

- After updating its state, it broadcasts the AASUpdateEvent to all subscribed exporters.

### Output Exporters

- Exporters subscribe to the Processing Core to receive a live stream of AASUpdateEvents.

- A dummy exporter is included, which is enabled by default and simply logs all events it receives.

- Stubs for http and opcua exporters exist but are not yet implemented.

## Configuration

The service is configured using three main files:

**config/config.yaml**: The main configuration file. It defines which adapter to use, how to connect to it, and which exporters to enable.

**config/aas/quadcopter.json**: The complete AAS model. This file defines the structure, including all Submodels (e.g., "Navigation," "Status") and their Properties (e.g., "Latitude," "SystemStatus").

**config/mappings/mavlink.yaml**: The data mapping file. It creates the link between a MAVLink message field (e.g., HEARTBEAT.SystemStatus) and an AAS property (e.g., Status.SystemStatus).

## How to Run

From the root of the uav-aas-connector directory:

```golang
go run ./cmd/main.go
```

The service will load the configuration and start the adapter and exporters.

# Next Features

- Implement Real Exporters: The dummy exporter is only for testing. The top priority is to implement the http and opcua exporters defined in the configuration.

- Build HTTP "Pull" Exporter: Implement the http exporter to serve the entire current state of the AAS model. It should use the core.GetSubmodel(id) function to fetch the latest data on demand (a "pull" model).

- Build OPC UA "Push" Exporter: Implement the opcua exporter to subscribe to the Core's update events and "push" those changes to an external OPC UA server as they happen.

- Improve Scalability for High-Frequency Data: To handle high-rate messages (e.g., 100Hz from MAVLink), the AASCore.updateModel function should be modified to batch updates. Instead of locking and broadcasting on every single message, it could aggregate changes in a queue and apply/broadcast them at a fixed interval (e.g., every 100ms) to reduce lock contention and network noise.

- Production-Grade Logging: Replace the standard log package with a structured, configurable logger like zap or logrus for better monitoring in production.