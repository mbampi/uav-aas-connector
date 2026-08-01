# MAVLink to OPC UA Naive Gateway (Benchmark Baseline)

### Overview
This script serves as a "Naive Baseline" for benchmarking edge-native Industrial IoT translations. It bridges a high-frequency UAV telemetry stream (MAVLink over UDP) to an industrial digital twin framework (OPC UA). 

**Architectural Note:** This script is intentionally designed with a "naive" architecture (single-threaded, synchronous mathematical translation, and blocking network writes). It is built to demonstrate the bottlenecks of traditional Python scripting (Global Interpreter Lock, buffer overflows, and latency spikes) when subjected to high-frequency edge data. It is primarily used to generate comparative metrics against high-performance, concurrent middleware.

### Prerequisites
* **Python 3.7+**
* A running MAVLink stream (e.g., PX4 SITL, MavProxy, `mavlink-replay`, or a physical flight controller) emitting UDP on port `14550`.
* A running OPC UA Server hosting the target Asset Administration Shell (AAS) nodes.

**Install required libraries:**
```bash
pip install pymavlink asyncua psutil
```

### How to Execute

1. **Configure the Endpoints:** Open `main.py` and ensure `MAVLINK_CONNECTION_STRING` and `OPCUA_SERVER_ENDPOINT` match your local setup (both can also be overridden via environment variables of the same name, without editing the file - see [Running with Docker](#running-with-docker-raspberry-pi-deployment) below).
2. **Configure the Node IDs:** Update the `get_node()` strings to match the exact Node IDs of your running OPC UA server.
3. **Start this gateway first, then start the telemetry source.** `main.py` binds its MAVLink UDP socket and blocks on `wait_heartbeat()` before doing anything else, so it must already be listening before a sender starts — MAVLink UDP is fire-and-forget with no buffering or retry on the sender side, so anything sent before the gateway is bound (including the first heartbeat) is lost for good.
```bash
python main.py
```
4. **Stop the Benchmark:** Press `Ctrl+C` to cleanly exit the loop, disconnect from the server, and finalize the metrics log.

---

### Running with Docker (Raspberry Pi Deployment)

For a standalone deployment - e.g. a Raspberry Pi 5 sitting between a flight controller/companion computer (sending MAVLink) and an OPC UA server (running on another device on the network) - use the included `Dockerfile` and `docker-compose.yml`. `MAVLINK_CONNECTION_STRING`, `OPCUA_SERVER_ENDPOINT`, and `CSV_FILENAME` are all read from environment variables at startup (falling back to the same defaults as local dev if unset), so the two endpoint IPs can be pointed at your actual devices entirely from `docker-compose.yml` - no code changes or image rebuilds needed when moving between networks.

**Prerequisites (on the Pi):**
* Docker Engine and the Docker Compose plugin installed (`docker compose version` should work).

**Setup:**

1. **Get this directory onto the Pi** (`git clone`, `scp -r`, etc.), then `cd` into `python-baseline-conn/`.

2. **Edit `docker-compose.yml`'s `environment:` block:**
   * `OPCUA_SERVER_ENDPOINT` - set this to the LAN/static IP of the device hosting the OPC UA server, e.g. `opc.tcp://192.168.1.50:4840/`. This must be reachable from the Pi.
   * `MAVLINK_CONNECTION_STRING` - leave as `udp:0.0.0.0:14550` unless the sending device uses a different port. The device sending MAVLink telemetry must be configured to stream UDP to **the Pi's IP** on that port.
   * If MAVLink instead arrives over a serial radio/telemetry module rather than UDP, change `MAVLINK_CONNECTION_STRING` to the serial device path (e.g. `/dev/ttyUSB0`) and uncomment the matching line under `devices:`.

3. **Build and start the gateway:**
   ```bash
   docker compose up -d --build
   ```

4. **Watch it run:**
   ```bash
   docker compose logs -f
   ```
   You should see `MAVLink connected!` followed by `OPC UA connected!`, then periodic `Processed: ...` summaries every 50 messages.

5. **Retrieve metrics:** the CSV described below is written into `./metrics/naive_baseline_metrics.csv` on the host (bind-mounted from the container), so it persists across restarts and can be pulled off the Pi at any time - even while the container is running, since the file is opened in line-buffered mode.

6. **Stop the gateway:**
   ```bash
   docker compose down
   ```

**Why `network_mode: host`:** the compose file runs the container on the Pi's host network instead of Docker's bridge/NAT. This avoids NAT overhead for the high-frequency MAVLink UDP stream and lets the container reach the OPC UA server directly by its LAN IP without any port publishing. It also means the container binds `14550/udp` directly on the Pi's network interfaces - make sure nothing else on the Pi is already using that port, and that any firewall on the Pi allows inbound UDP on it from the MAVLink source device.

---

### Testing against `uav-aas-tools`

This gateway's defaults (`MAVLINK_CONNECTION_STRING`, `OPCUA_SERVER_ENDPOINT`, and the four `get_node()` paths) are pre-configured to match `../../../uav-aas-tools/opcua_server` (`quad_op.nodeset.xml`) and `../../../uav-aas-tools/mavlink-replay`. Start the OPC UA server and this gateway first (order between the two doesn't matter), then start the replay last:

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

Note `replay.py` defaults `--ip` to `192.168.1.50` (a LAN address) — it must be overridden to reach wherever this gateway is actually running. `main.py` listens on `0.0.0.0:14550` so it accepts either loopback traffic (same-machine testing) or LAN traffic (`replay.py` on a separate device), as long as `--ip`/`--port` are pointed at this machine.

---

### Metrics Output (`naive_baseline_metrics.csv`)
Upon execution, the script generates a real-time CSV file logging the performance of the translation loop. 

**Columns:**
* **`System_Time`:** Standard Unix timestamp of the event.
* **`Message_Type`:** The MAVLink message processed (e.g., `GLOBAL_POSITION_INT`).
* **`MAVLink_Seq`:** The sequence number (0-255) of the incoming MAVLink packet.
* **`Dropped_Since_Last`:** The number of MAVLink packets (of any type) lost between the previous raw packet and this one, based on gaps in the shared MAVLink sequence counter. A number higher than 0 indicates the gateway is failing to keep up with the telemetry rate.
  **Known unreliable with `mavlink-replay`/`synth.py` as of 2026-08:** both tools serialize messages with `msg.pack(mav)` but never call `mav.send(...)`, and pymavlink only advances its sequence counter inside `send()` — so every replayed/synthetic packet carries `seq = 0` on the wire, and this column reads a constant 255 on every row regardless of real network conditions. Don't use it (or the `total_dropped` console/exit stat, which is derived from it) as a loss metric until that's fixed upstream. Use the `SOURCE_TLOG_PATH`-based delivery ratio below instead.
* **`Queue_Wait_ms`:** Time elapsed since the previous *tracked* message finished processing. Captures backlog/queueing delay caused by the single-threaded, blocking design — add this to `Processing_Latency_ms` to get the full end-to-end pipeline latency for a message.
* **`Processing_Latency_ms`:** The time taken (in milliseconds) to perform the math and wait for the OPC UA server to acknowledge the write. 
* **`CPU_Usage_%`:** The current CPU utilization of the Python process during the translation cycle.
* **`Memory_RSS_MB`:** The Resident Set Size (RAM) consumed by the script in Megabytes.

### Measuring Real Packet Loss (`SOURCE_TLOG_PATH`)

Because the wire-level `Dropped_Since_Last` counter is currently unreliable (see above), set the `SOURCE_TLOG_PATH` environment variable to the same `.tlog` file being fed into `replay.py` for a benchmark run:

```bash
export SOURCE_TLOG_PATH=../../../uav-aas-tools/mavlink-replay/data/synthetic/synth_50hz_15min.tlog
python main.py
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

Because this is a monolithic script, adding new telemetry parameters requires manually hardcoding the mapping logic. Follow these three steps:

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