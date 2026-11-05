# Naive MAVLink Gateways (Benchmark Baseline)

Two Python gateways used as a baseline for the Go `uav-aas-connector`. Both take the same MAVLink stream over UDP and the same message set and property mapping. Only the output differs:

- `main.py` - MAVLink to OPC UA.
- `main_mqtt.py` - MAVLink to MQTT, with the same topic scheme (`<prefix>/submodels/<base64(idShort)>/elements/<path>`) and payload (`{"value":...,"timestamp":...}`) as the connector's MQTT exporter, so the same broker and receiver work with either.

Run only one at a time.

Both are naive on purpose: single-threaded, one blocking write/publish per property, no batching, coalescing or worker pool. See `../README.md` for the methodology. `main_mqtt.py` leaves socket I/O to paho-mqtt's background thread (the library requires it), but translation and `publish()` stay sequential on the main thread.

## Prerequisites

- Python 3.7+
- A MAVLink stream on UDP port `14550` (PX4 SITL, MavProxy, `mavlink-replay` or a flight controller)
- For `main.py`: an OPC UA server with the AAS nodes
- For `main_mqtt.py`: an MQTT broker, e.g. Mosquitto (see `uav-aas-tools/mqtt_receiver`)

```bash
pip install pymavlink asyncua paho-mqtt psutil
```

## Running

OPC UA:

1. Set `MAVLINK_CONNECTION_STRING` and `OPCUA_SERVER_ENDPOINT` (environment variables, or the defaults at the top of `main.py`).
2. Update the `get_node()` strings to match your server's node IDs.
3. Start the gateway before the telemetry source. MAVLink over UDP has no retry, so anything sent before the gateway binds its socket, including the first heartbeat, is lost.

```bash
python main.py
```

MQTT:

1. Set `MQTT_BROKER_URL` (default `tcp://127.0.0.1:1883`) and `MQTT_TOPIC_PREFIX` (default `aas/uav-1`, same as the connector's `config.yaml`).
2. Start the gateway before the telemetry source.

```bash
python main_mqtt.py
```

`Ctrl+C` exits the loop, disconnects and finalizes the metrics log.

## Running with Docker (Raspberry Pi)

`docker-compose.yml` has three services:

- `python-baseline-conn` - OPC UA gateway (`main.py`)
- `python-baseline-conn-mqtt` - MQTT gateway (`main_mqtt.py`), same image with an `entrypoint:` override
- `metrics-collector` - resource usage sidecar (see Resource Monitoring below)

The two gateways share a `container_name` and UDP port `14550`, so they can't run together. The shared name lets `metrics-collector` monitor whichever one is running. Always name the services in `docker compose up`.

Setup:

1. Copy this directory to the Pi and `cd` into it.
2. Edit the `environment:` block in `docker-compose.yml`:
   - `OPCUA_SERVER_ENDPOINT` (OPC UA), e.g. `opc.tcp://192.168.1.50:4840/`
   - `MQTT_BROKER_URL` (MQTT), e.g. `tcp://192.168.1.50:1883/`
   - `MAVLINK_CONNECTION_STRING`: leave as `udp:0.0.0.0:14550` unless the sender uses another port. The sender must stream to the Pi's IP. For a serial radio, use the device path (e.g. `/dev/ttyUSB0`) and uncomment the line under `devices:`.
3. Start one gateway and the sidecar:
   ```bash
   # OPC UA:
   docker compose up -d --build python-baseline-conn metrics-collector
   # MQTT:
   docker compose up -d --build python-baseline-conn-mqtt metrics-collector
   ```
4. Check the logs:
   ```bash
   docker compose logs -f
   ```
   You should see `MAVLink connected!`, then `OPC UA connected!` or `MQTT connected!`, then `Processed: ...` every 50 messages.
5. The metrics CSV is written to `./metrics/` on the host. It's line-buffered, so you can copy it while the container runs.
6. Stop everything:
   ```bash
   docker compose down
   ```

The services use `network_mode: host` to avoid NAT overhead on the MAVLink stream and to reach the server or broker directly by LAN IP. Port `14550/udp` must be free on the Pi and allowed through any firewall.

## Resource Monitoring

`metrics-collector` uses the same script as the connector (`resource_metrics/resource_metrics.py` is an exact copy of `uav-aas-connector/resource_metrics`). It polls the Docker stats API, uses the same CPU and memory formulas as `docker stats`, and writes `Timestamp,CPU_Percent,Memory_Bytes,Memory_Percent` to `./metrics/python-baseline-conn_<timestamp>.csv`.

Use this CSV to compare CPU and memory with the connector. The `CPU_Percent` and `Memory_MB` columns in the gateway's own CSV come from `psutil` inside the process, sampled once per message, so they aren't directly comparable. They're still useful for matching a spike to a message type.

## Testing against `uav-aas-tools`

OPC UA defaults match `../../../uav-aas-tools/opcua_server` (`quad_op.nodeset.xml`) and `mavlink-replay`. Start the replay last:

```bash
# 1. OPC UA server (port must match OPCUA_SERVER_ENDPOINT, default 4840)
cd ../../../uav-aas-tools/opcua_server
python opcua_server.py quad_op.nodeset.xml 4840

# 2. Gateway
python main.py

# 3. Replay
cd ../../../uav-aas-tools/mavlink-replay
python3 replay.py ./data/flight_1.tlog --ip 127.0.0.1 --port 14550 --log dispatch_log.csv
```

MQTT defaults match `../../../uav-aas-tools/mqtt_receiver` (`mosquitto.conf`, `--prefix aas/uav-1`):

```bash
# 1. Broker
cd ../../../uav-aas-tools/mqtt_receiver
mosquitto -c mosquitto.conf

# 2. Receiver (same CSV schema as the OPC UA server log)
python3 mqtt_receiver.py --host localhost --port 1883 --prefix aas/uav-1 --log mqtt_received_log.csv

# 3. Gateway
python main_mqtt.py

# 4. Replay
cd ../../../uav-aas-tools/mavlink-replay
python3 replay.py ./data/flight_1.tlog --ip 127.0.0.1 --port 14550 --log dispatch_log.csv
```

`replay.py` defaults `--ip` to `192.168.1.50`, so always set it. The gateways listen on `0.0.0.0:14550`, so loopback and LAN both work.

## Metrics Output

Each script writes a CSV (`naive_baseline_metrics.csv` or `naive_baseline_mqtt_metrics.csv`) with the same columns:

| Column | Meaning |
| --- | --- |
| `System_Time` | Unix timestamp |
| `Message_Type` | MAVLink message, e.g. `GLOBAL_POSITION_INT` |
| `MAVLink_Seq` | Packet sequence number (0-255) |
| `Dropped_Since_Last` | Packets lost since the previous one, from sequence gaps. Not usable with replay (see below) |
| `Queue_Wait_ms` | Time since the previous tracked message finished. Add `Processing_Latency_ms` for the full latency |
| `Processing_Latency_ms` | Time to translate and write/publish all properties of the message |
| `Write_Success` | `True` if every property was written/published |
| `Error` | Exception message, up to 200 characters, when `Write_Success` is `False` |
| `CPU_Percent` | Process CPU from `psutil.Process.cpu_percent()` |
| `Memory_MB` | Process RSS from `psutil.Process.memory_info().rss` |

Notes:

- `Dropped_Since_Last` reads a constant 255 with `mavlink-replay`/`synth.py`. They call `msg.pack(mav)` but never `mav.send(...)`, so `seq` stays 0 on the wire. The same goes for the `total_dropped` stat. Use `SOURCE_TLOG_PATH` instead.
- `Processing_Latency_ms` differs by gateway. For OPC UA it's a confirmed round trip per property. For MQTT (QoS 0) it only confirms the local send: the gateway waits up to 2 s in `wait_for_publish()`, like the connector's `WaitTimeout`, then checks `is_published()`, because a QoS 0 timeout doesn't raise.
- When `Write_Success` is `False`, the rest of that message's fields are skipped and the loop continues. The latency on that row is the time until the failure, and `analysis/baseline_latency_analyzer.py` excludes it.

## Measuring Packet Loss (`SOURCE_TLOG_PATH`)

Set `SOURCE_TLOG_PATH` to the `.tlog` file being fed to `replay.py`:

```bash
export SOURCE_TLOG_PATH=../../../uav-aas-tools/mavlink-replay/data/synthetic/synth_50hz_15min.tlog
python main.py          # or: python main_mqtt.py
```

At startup the gateway scans the file to count the messages of each tracked type, since `replay.py` sends each message once with no filtering. The console then shows a delivery ratio, e.g. `Processed: 250 | Delivery: 250/37973 (0.7%) | Last Latency: 20.2ms`. On exit it writes a per-type breakdown to `<CSV_FILENAME base>_delivery_summary.json`:

```json
{
  "LOCAL_POSITION_NED": {"arrived": 5392, "expected": 5987, "delivery_pct": 90.06},
  "SERVO_OUTPUT_RAW":   {"arrived": 5427, "expected": 5988, "delivery_pct": 90.63},
  "_overall":           {"arrived": 34258, "expected": 37973, "delivery_pct": 90.22}
}
```

Leave it unset for live drones.

## Adding a New Sensor

The mapping is hardcoded, so it takes three steps. These are for `main.py`. In `main_mqtt.py`, steps 1 and 3 are a single `pub('SubmodelIdShort', 'Property/Path', value)` call.

1. Define the node at the top of `main()`. The path must exist in your server's nodeset XML:
   ```python
   lidar_node = client.get_node("ns=2;s=Sensors/Lidar/Distance")
   ```
2. Add the message type to the filter:
   ```python
   if msg_type not in ['GLOBAL_POSITION_INT', 'SYS_STATUS', 'DISTANCE_SENSOR']:
       continue
   ```
3. Add the translation and write in the main loop:
   ```python
   elif msg_type == 'DISTANCE_SENSOR':
       # centimeters to meters
       distance_meters = msg.current_distance / 100.0
       await lidar_node.set_value(ua.DataValue(ua.Variant(distance_meters, ua.VariantType.Double)))
   ```
