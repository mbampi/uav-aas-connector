# UAV AAS Connector

A Go service that connects to a UAV (via MAVLink), translates its data, and updates an in-memory Asset Administration Shell (AAS) model.

It allows external systems to monitor the UAV state through a standardized AAS interface.

## How It Works

The application contains three main components:

### Input Adapter

- Connects to a UAV data source, specified in the config.yaml.

- Currently, a Mavlink adapter is implemented, which can connect to a UAV via UDP, TCP, or Serial.

- It uses the mapping file set in config to translate raw MAVLink messages into internal AAS events.

### Processing Core

- On startup, it loads the complete AAS model structure from config into memory.

- It listens for AASUpdateEvent messages from the adapter.

- When an event is received (e.g., a new latitude value), it finds the corresponding property in the in-memory AAS model and updates its value.

- After updating its state, it broadcasts the AASUpdateEvent to all subscribed exporters.

### Output Exporters

- Exporters subscribe to the Processing Core to receive a live stream of AASUpdateEvents. They can then expose or push the updated AAS model to external systems.

- Current Implementations
    - Dummy: Logs received AASUpdateEvents for debugging.
    - OPC UA Client: Exposes the AAS model via an OPC UA server, allowing external systems to query the UAV state.
    - OPC UA Server: Connects to an external OPC UA server to push AAS property updates.
    - MQTT: Publishes AAS property updates to an MQTT broker.
    - HTTP REST API: Exposes the AAS model via a RESTful API.

## Configuration

The service is configured using three main files:

**config/config.yaml**: The main configuration file. It defines which adapter to use, how to connect to it, and which exporters to enable.

**config/aas/quadcopter.json**: The complete AAS model. This file defines the structure, including all Submodels (e.g., "Navigation," "Status") and their Properties (e.g., "Latitude," "SystemStatus").

**config/mappings/mavlink.yaml**: The data mapping file. It creates the link between a MAVLink message field (e.g., HEARTBEAT.SystemStatus) and an AAS property (e.g., Status.SystemStatus).

## How to Run

### Non-Docker

From the root of the uav-aas-connector directory:

```bash
go run ./cmd/main.go
```

Or build the binary first:

For Raspberry Pi 4 aarch64 - Ubuntu 24.04.3 LTS
```bash
GOOS=linux GOARCH=arm64 go build -o uav-aas-connector ./cmd/main.go
```

For macos
```bash
GOOS=darwin GOARCH=arm64 go build -o uav-aas-connector ./cmd/main.go
```

And then just run:
```bash
./uav-aas-connector
```

The service will load the configuration and start the adapter and exporters.

## To Raspberry Pi

In this Mac
```bash
scp -r pkg/ cmd/ docker-compose.yml Dockerfile go.mod go.sum config/ pi@raspberrypi.local:/home/pi/uav-aas-connector/
```

### In Raspberry Pi

```bash
mkdir uav-aas-connector
```

Install docker and compose
```bash
sudo apt update && sudo apt upgrade -y
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo apt-get install docker-compose-plugin -y
sudo usermod -aG docker $USER
```

### Docker

Build the Docker image:
```bash
docker compose build
``` 

Run the Docker container:
```bash
docker compose up
```

### PX4 SITL Gazebo

In Ubuntu, in directory `UAV-Collision-Avoidance/`
```bash
source ./macros.sh
setros && buildall && remodel && loadmission && sim
```

MavProxy: redirect MAVLink from SITL to the connector:
```bash
mavproxy.py --master=udpin:127.0.0.1:14550 --out=udpout:raspberrypi.local:14550
```


## Run Experiments

### Mac — terminal 1
```bash
python opcua_server.py quad_op.nodeset.xml 4841
```

### Pi — connector with opcua_client → Mac IP:4841
```bash
./uav-aas-connector
```

### Mac — terminal 2
```bash
python3 replay.py synthetic_200hz.tlog --ip <pi-ip> --port 14550 --log dispatch_log.csv
```

### Mac — analyze
```bash
python3 latency-calculate.py dispatch_log.csv opcua_ros_log4841.csv
```