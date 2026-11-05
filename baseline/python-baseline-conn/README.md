# MAVLink to OPC UA / MQTT Naive Gateway (Benchmark Baseline)

### Overview
This directory holds two scripts serving as a "Naive Baseline" for benchmarking edge-native Industrial IoT translations against the Go `uav-aas-connector`. Both bridge the same high-frequency UAV telemetry stream (MAVLink over UDP) and the same MAVLink message set / AAS property mapping to an industrial digital twin, differing only in the output protocol:

* **`main.py`** - MAVLink to **OPC UA**.
* **`main_mqtt.py`** - MAVLink to **MQTT**, publishing to the exact same topic scheme (`<prefix>/submodels/<base64(idShort)>/elements/<path>`) and JSON payload shape (`{"value":...,"timestamp":...}`) as the Go connector's MQTT exporter, so the same broker/receiver setup and topic prefix (`aas/uav-1`) work against either the connector or this baseline unmodified.

Only one of the two should be run at a time - see [Running with Docker](#running-with-docker-raspberry-pi-deployment) below.

**Architectural Note:** Both scripts are intentionally designed with a "naive" architecture (single-threaded, synchronous mathematical translation, and blocking network writes/publishes - no batching, no coalescing, no worker pool). They are built to demonstrate the bottlenecks of traditional Python scripting (Global Interpreter Lock, buffer overflows, and latency spikes) when subjected to high-frequency edge data, and are primarily used to generate comparative metrics against the high-performance, concurrent Go connector. `main_mqtt.py` does hand paho-mqtt's socket I/O to that library's own background thread (there is no way to use it otherwise), but message translation and every `publish()` call remain entirely sequential and blocking on the main thread, one message at a time - that sequential, non-batched, non-coalesced path is the property under test, not the literal OS thread count.

### Prerequisites
* **Python 3.7+**
* A running MAVLink stream (e.g., PX4 SITL, MavProxy, `mavlink-replay`, or a physical flight controller) emitting UDP on port `14550`.
* For `main.py`: a running OPC UA Server hosting the target Asset Administration Shell (AAS) nodes.
* For `main_mqtt.py`: a running MQTT broker (e.g. Mosquitto) - see `uav-aas-tools/mqtt_receiver` for the reference broker config + receiver used to validate the connector's own MQTT runs.

**Install required libraries:**
```bash
pip install pymavlink asyncua paho-mqtt psutil
```

### How to Execute

**OPC UA (`main.py`):**
1. **Configure the Endpoints:** Open `main.py` and ensure `MAVLINK_CONNECTION_STRING` and `OPCUA_SERVER_ENDPOINT` match your local setup (both can also be overridden via environment variables of the same name, without editing the file - see [Running with Docker](#running-with-docker-raspberry-pi-deployment) below).
2. **Configure the Node IDs:** Update the `get_node()` strings to match the exact Node IDs of your running OPC UA server.
3. **Start this gateway first, then start the telemetry source.** `main.py` binds its MAVLink UDP socket and blocks on `wait_heartbeat()` before doing anything else, so it must already be listening before a sender starts — MAVLink UDP is fire-and-forget with no buffering or retry on the sender side, so anything sent before the gateway is bound (including the first heartbeat) is lost for good.
```bash
python main.py
```

**MQTT (`main_mqtt.py`):**
1. **Configure the broker:** set `MQTT_BROKER_URL` (default `tcp://127.0.0.1:1883`) and `MQTT_TOPIC_PREFIX` (default `aas/uav-1`, matching the Go connector's `config.yaml`) via environment variables, or edit the defaults at the top of the file.
2. **Start this gateway first, then start the telemetry source** - same MAVLink-UDP-is-fire-and-forget reasoning as `main.py` above.
```bash
python main_mqtt.py
```

**Stop either benchmark:** Press `Ctrl+C` to cleanly exit the loop, disconnect, and finalize the metrics log.

---

### Running with Docker (Raspberry Pi Deployment)

For a standalone deployment - e.g. a Raspberry Pi 5 sitting between a flight controller/companion computer (sending MAVLink) and an OPC UA server / MQTT broker (running on another device on the network) - use the included `Dockerfile` and `docker-compose.yml`. Both gateways share one image; environment variables (falling back to the same defaults as local dev if unset) select the endpoints, so IPs can be pointed at your actual devices entirely from `docker-compose.yml` - no code changes or image rebuilds needed when moving between networks.

**`docker-compose.yml` defines three services:**
* `python-baseline-conn` - the OPC UA gateway (`main.py`).
* `python-baseline-conn-mqtt` - the MQTT gateway (`main_mqtt.py`), same image, `entrypoint:` override.
* `metrics-collector` - the external resource-usage sidecar (see [Resource Monitoring](#resource-monitoring-same-method-as-the-go-connector) below).

**`python-baseline-conn` and `python-baseline-conn-mqtt` share the same `container_name` and must never run at the same time** - both bind MAVLink UDP `:14550` in host network mode, so starting both together would conflict on the port even before the container-name collision. This is deliberate: it lets `metrics-collector`'s fixed `TARGET_CONTAINER` monitor whichever one is actually running without editing its config, at the cost of always naming the gateway service explicitly in every command below (never bare `docker compose up`/`down`, which would try to start all three services, including both gateways, at once).

**Prerequisites (on the Pi):**
* Docker Engine and the Docker Compose plugin installed (`docker compose version` should work).

**Setup:**

1. **Get this directory onto the Pi** (`git clone`, `scp -r`, etc.), then `cd` into `python-baseline-conn/`.

2. **Edit `docker-compose.yml`'s `environment:` block** for whichever gateway you're running:
   * `python-baseline-conn` (OPC UA): `OPCUA_SERVER_ENDPOINT` - set to the LAN/static IP of the device hosting the OPC UA server, e.g. `opc.tcp://192.168.1.50:4840/`. Must be reachable from the Pi.
   * `python-baseline-conn-mqtt` (MQTT): `MQTT_BROKER_URL` - set to the LAN/static IP of the device hosting the MQTT broker, e.g. `tcp://192.168.1.50:1883/`. Must be reachable from the Pi.
   * Both: `MAVLINK_CONNECTION_STRING` - leave as `udp:0.0.0.0:14550` unless the sending device uses a different port. The device sending MAVLink telemetry must be configured to stream UDP to **the Pi's IP** on that port. If MAVLink instead arrives over a serial radio/telemetry module rather than UDP, change this to the serial device path (e.g. `/dev/ttyUSB0`) and uncomment the matching line under `devices:`.

3. **Build and start the gateway you want to benchmark, plus the metrics sidecar** (pick one gateway service, always include `metrics-collector`):
   ```bash
   # OPC UA:
   docker compose up -d --build python-baseline-conn metrics-collector
   # MQTT:
   docker compose up -d --build python-baseline-conn-mqtt metrics-collector
   ```

4. **Watch it run:**
   ```bash
   docker compose logs -f
   ```
   For OPC UA you should see `MAVLink connected!` followed by `OPC UA connected!`; for MQTT, `MAVLink connected!` followed by `MQTT connected!` - then periodic `Processed: ...` summaries every 50 messages either way.

5. **Retrieve metrics:** the CSV described below (`naive_baseline_metrics.csv` or `naive_baseline_mqtt_metrics.csv`) is written into `./metrics/` on the host (bind-mounted from the container), so it persists across restarts and can be pulled off the Pi at any time - even while the container is running, since the file is opened in line-buffered mode.

6. **Stop everything:**
   ```bash
   docker compose down
   ```
   (safe here since nothing is running to conflict on names/ports once you're just tearing down).

**Why `network_mode: host`:** the compose file runs each gateway on the Pi's host network instead of Docker's bridge/NAT. This avoids NAT overhead for the high-frequency MAVLink UDP stream and lets the container reach the OPC UA server/MQTT broker directly by LAN IP without any port publishing. It also means the container binds `14550/udp` directly on the Pi's network interfaces - make sure nothing else on the Pi is already using that port, and that any firewall on the Pi allows inbound UDP on it from the MAVLink source device.

---

### Resource Monitoring (same method as the Go connector)

`docker-compose.yml`'s `metrics-collector` service - `resource_metrics/Dockerfile` and `resource_metrics/resource_metrics.py` here are byte-for-byte copies of `uav-aas-connector/resource_metrics`, not a reimplementation - polls the Docker Engine stats API for whichever gateway container is running (both share `container_name: python-baseline-conn`, which is exactly what lets this one `TARGET_CONTAINER` setting work for either) and computes CPU%/Memory with the exact same formulas `docker stats` itself uses (see the script's `calculate_cpu_percent`/`calculate_mem_usage_no_cache`), on the same ~2 s cadence, writing the same four-column schema (`Timestamp,CPU_Percent,Memory_Bytes,Memory_Percent`) to `./metrics/python-baseline-conn_<timestamp>.csv`.

**This is the CSV to use for any CPU/memory comparison against the Go connector's own numbers**, for either gateway. `naive_baseline_metrics.csv`/`naive_baseline_mqtt_metrics.csv`'s own `CPU_Percent`/`Memory_MB` columns (documented below) are a *different, in-process* measurement - `psutil` self-reporting from inside the Python process, sampled once per MAVLink message rather than on a fixed wall-clock interval - and are not on the same measurement basis as the connector's Docker-stats-based numbers. Keep them as a secondary, supplementary signal (they're still useful for correlating a CPU/memory spike with a specific message type or moment in the run), not as the headline metric in a cross-system comparison.

The commands in step 3 above already include `metrics-collector` alongside whichever gateway you start, so both containers come up together; `docker compose down` stops both (and whichever gateway isn't running is simply not part of the stack, so there's nothing else to stop). Both CSVs land in `./metrics/` on the host and can be pulled off the Pi at any time, exactly as described in step 5 above.

---

### Testing against `uav-aas-tools`

**OPC UA:** this gateway's defaults (`MAVLINK_CONNECTION_STRING`, `OPCUA_SERVER_ENDPOINT`, and the `get_node()` paths in `main.py`) are pre-configured to match `../../../uav-aas-tools/opcua_server` (`quad_op.nodeset.xml`) and `../../../uav-aas-tools/mavlink-replay`. Start the OPC UA server and this gateway first (order between the two doesn't matter), then start the replay last:

```bash
# 1. OPC UA server (port must match OPCUA_SERVER_ENDPOINT above, default 4840)
cd ../../../uav-aas-tools/opcua_server
python opcua_server.py quad_op.nodeset.xml 4840

# 2. This gateway (binds :14550, waits for a heartbeat, then connects to the OPC UA server)
python main.py

# 3. MAVLink replay, started last (--ip must reach this gateway - loopback if same machine)
cd ../../../uav-aas-tools/mavlink-replay
python3 replay.py ./data/flight_1.tlog --ip 127.0.0.1 --port 14550 --log dispatch_log.csv
```

**MQTT:** `main_mqtt.py`'s defaults (`MQTT_BROKER_URL`, `MQTT_TOPIC_PREFIX`) match `../../../uav-aas-tools/mqtt_receiver`'s `mosquitto.conf` and `--prefix aas/uav-1`, the same reference broker/receiver used to validate the connector's own MQTT runs. Start the broker and receiver, then this gateway, then the replay last:

```bash
# 1. Broker
cd ../../../uav-aas-tools/mqtt_receiver
mosquitto -c mosquitto.conf

# 2. Receiver (logs arrival timestamps, same CSV schema as the OPC UA server log)
python3 mqtt_receiver.py --host localhost --port 1883 --prefix aas/uav-1 --log mqtt_received_log.csv

# 3. This gateway (binds :14550, waits for a heartbeat, then connects to the broker)
python main_mqtt.py

# 4. MAVLink replay, started last (--ip must reach this gateway - loopback if same machine)
cd ../../../uav-aas-tools/mavlink-replay
python3 replay.py ./data/flight_1.tlog --ip 127.0.0.1 --port 14550 --log dispatch_log.csv
```

Note `replay.py` defaults `--ip` to `192.168.1.50` (a LAN address) — it must be overridden to reach wherever the gateway is actually running. Either gateway listens on `0.0.0.0:14550` so it accepts either loopback traffic (same-machine testing) or LAN traffic (`replay.py` on a separate device), as long as `--ip`/`--port` are pointed at that machine.

---

### Metrics Output (`naive_baseline_metrics.csv` / `naive_baseline_mqtt_metrics.csv`)
Upon execution, each script generates a real-time CSV file logging the performance of the translation loop, with the same columns and schema for both gateways.

**Columns:**
* **`System_Time`:** Standard Unix timestamp of the event.
* **`Message_Type`:** The MAVLink message processed (e.g., `GLOBAL_POSITION_INT`).
* **`MAVLink_Seq`:** The sequence number (0-255) of the incoming MAVLink packet.
* **`Dropped_Since_Last`:** The number of MAVLink packets (of any type) lost between the previous raw packet and this one, based on gaps in the shared MAVLink sequence counter. A number higher than 0 indicates the gateway is failing to keep up with the telemetry rate.
  **Known unreliable with `mavlink-replay`/`synth.py` as of 2026-08:** both tools serialize messages with `msg.pack(mav)` but never call `mav.send(...)`, and pymavlink only advances its sequence counter inside `send()` — so every replayed/synthetic packet carries `seq = 0` on the wire, and this column reads a constant 255 on every row regardless of real network conditions. Don't use it (or the `total_dropped` console/exit stat, which is derived from it) as a loss metric until that's fixed upstream. Use the `SOURCE_TLOG_PATH`-based delivery ratio below instead.
* **`Queue_Wait_ms`:** Time elapsed since the previous *tracked* message finished processing. Captures backlog/queueing delay caused by the single-threaded, blocking design — add this to `Processing_Latency_ms` to get the full end-to-end pipeline latency for a message.
* **`Processing_Latency_ms`:** The time taken (in milliseconds) to perform the math and publish/write every property from one message, sequentially and blocking.
  * `main.py` (OPC UA): waits for the server to acknowledge each `set_value()` write - OPC UA's write service is confirmed, so this is a true round-trip per property.
  * `main_mqtt.py` (MQTT): waits (bounded to 2s, matching the Go connector's own `WaitTimeout`) for paho to hand each publish to the socket, via `wait_for_publish()`, then explicitly checks `is_published()` to confirm the publish actually completed rather than just timing out (see `Write_Success` below - `wait_for_publish()` alone returns silently on a QoS 0 timeout, with no exception, so without that check a stalled publish would be indistinguishable from a slow-but-real one). This still isn't a broker acknowledgment - MQTT QoS 0 (used here, matching the connector's own exporter) has none at the protocol level - so it measures confirmed local send, not a confirmed round-trip to the broker; a real, expected asymmetry with OPC UA's per-property round-trip, worth keeping in mind when comparing this column across gateways.
* **`Write_Success`:** `True` if every property this message produced was confirmed written/published; `False` if any of them failed or (MQTT only) timed out without confirmation. On failure, the rest of that message's fields are abandoned (no per-field recovery) and the loop moves on to the next message rather than crashing the whole run - see the comment directly above the try/except in either script for the full reasoning. `Processing_Latency_ms` on a `False` row is the time until the failure, not a completed operation's latency, and should be excluded from latency statistics (`analysis/baseline_latency_analyzer.py` already does this automatically).
* **`Error`:** The exception message (truncated to 200 characters) when `Write_Success` is `False`; empty otherwise.
* **`CPU_Percent`:** The current CPU utilization of the Python process during the translation cycle, self-reported via `psutil.Process.cpu_percent()`.
* **`Memory_MB`:** The Resident Set Size (RAM) consumed by the script in Megabytes, self-reported via `psutil.Process.memory_info().rss`.

CPU_Percent and Memory_MB are sampled once per processed message, in-process - see [Resource Monitoring](#resource-monitoring-same-method-as-the-go-connector) above for the separate, externally-sampled CSV to use when comparing CPU/memory against the Go connector.

### Measuring Real Packet Loss (`SOURCE_TLOG_PATH`)

Because the wire-level `Dropped_Since_Last` counter is currently unreliable (see above), set the `SOURCE_TLOG_PATH` environment variable to the same `.tlog` file being fed into `replay.py` for a benchmark run:

```bash
export SOURCE_TLOG_PATH=../../../uav-aas-tools/mavlink-replay/data/synthetic/synth_50hz_15min.tlog
python main.py          # or: python main_mqtt.py
```

At startup, before opening the live MAVLink socket, the gateway does a quick read-only pre-scan of that file to count how many messages of each *tracked* type it contains — `replay.py` sends every message in the file exactly once with no filtering, so this is an exact, ground-truth expected count, independent of MAVLink sequence numbers and of any changes to `replay.py`/`synth.py`.

While running, the periodic console line switches from the old (unreliable) drop count to a live delivery ratio: `Processed: 250 | Delivery: 250/37973 (0.7%) | Last Latency: 20.2ms`. On exit, a full per-type breakdown prints to the console and is written to `<CSV_FILENAME base>_delivery_summary.json` next to the CSV, e.g.:

```json
{
  "LOCAL_POSITION_NED": {"arrived": 5392, "expected": 5987, "delivery_pct": 90.06},
  "SERVO_OUTPUT_RAW":   {"arrived": 5427, "expected": 5988, "delivery_pct": 90.63},
  "_overall":           {"arrived": 34258, "expected": 37973, "delivery_pct": 90.22}
}
```

Leave `SOURCE_TLOG_PATH` unset for real/live-drone deployments (e.g. the Raspberry Pi use case above) — there's no reference file to compare against in that case, and the gateway falls back to its previous behavior unchanged.

---

### How to Add Support for New Sensors

Because this is a monolithic script, adding new telemetry parameters requires manually hardcoding the mapping logic. Follow these three steps (shown here for `main.py`/OPC UA; in `main_mqtt.py` the equivalent of Step 1 and 3 is a single `pub('SubmodelIdShort', 'Property/Path', value)` call - see the existing `elif` blocks in that file - since topics are built from the Submodel idShort + property path rather than a pre-resolved node handle).

**Step 1: Define the target OPC UA Node**
At the top of the `main()` function, add a variable linking to the new node in your server.
```python
# Example: Adding a Lidar distance sensor (path must match a NodeId that
# actually exists in your server's nodeset XML - "/" separated, no "AAS." prefix)
lidar_node = client.get_node("ns=2;s=Sensors/Lidar/Distance")
```

**Step 2: Whitelist the Message Type**
Locate the message filter loop and add the new MAVLink message ID to the tracking list so the script doesn't ignore it.
```python
if msg_type not in ['GLOBAL_POSITION_INT', 'SYS_STATUS', 'DISTANCE_SENSOR']:
    continue
```

**Step 3: Add the Translation and Write Logic**
Inside the main `while` loop, add a new `elif` block to catch the message, perform any necessary mathematical conversions, and push it to the server.
```python
elif msg_type == 'DISTANCE_SENSOR':
    # MAVLink sends distance in centimeters; convert to meters for AAS
    distance_meters = msg.current_distance / 100.0
    
    # Push the standardized value to OPC UA
    await lidar_node.set_value(ua.DataValue(ua.Variant(distance_meters, ua.VariantType.Double)))
```