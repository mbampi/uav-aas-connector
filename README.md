# UAV AAS Connector

<!-- Digital Twin Connector -->

- Event-driven pipeline
- ETL (Extract, Transform, Load)

## Components

- **Input Adapters**: Collects data from UAVs (Unmanned Aerial Vehicles) through various protocols (e.g., MavLink, HTTP, MQTT).
- **Processing Core**: Processes and transforms the ingested data into a format compatible with Asset Administration Shell (AAS) standards.
- **Output Exporters**: Exports the processed data to AAS repositories or other target systems.

### Main Characteristics

- Modular architecture allowing easy addition of new adapters and exporters.
- Configurable via external configuration files.
- Designed for scalability and real-time data processing.
- Prioritize goroutine-safe code for high throughput
- Ensure zero-downtime
- Include logging/monitoring with zap or Prometheus for production readiness
- Graceful shutdown on interrupt signals
- Unit tests for reliability
