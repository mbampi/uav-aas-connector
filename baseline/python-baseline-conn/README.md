# MAVLink to OPC UA Naive Gateway (Benchmark Baseline)

### Overview
This script serves as a "Naive Baseline" for benchmarking edge-native Industrial IoT translations. It bridges a high-frequency UAV telemetry stream (MAVLink over UDP) to an industrial digital twin framework (OPC UA). 

**Architectural Note:** This script is intentionally designed with a "naive" architecture (single-threaded, synchronous mathematical translation, and blocking network writes). It is built to demonstrate the bottlenecks of traditional Python scripting (Global Interpreter Lock, buffer overflows, and latency spikes) when subjected to high-frequency edge data. It is primarily used to generate comparative metrics against high-performance, concurrent middleware.

### Prerequisites
* **Python 3.7+**
* A running MAVLink stream (e.g., PX4 SITL, MavProxy, or a physical flight controller) emitting UDP on `127.0.0.1:14550`.
* A running OPC UA Server hosting the target Asset Administration Shell (AAS) nodes.

**Install required libraries:**
```bash
pip install pymavlink asyncua psutil
```

### How to Execute

1. **Configure the Endpoints:** Open `gateway.py` and ensure `MAVLINK_CONNECTION_STRING` and `OPCUA_SERVER_ENDPOINT` match your local setup.
2. **Configure the Node IDs:** Update the `get_node()` strings to match the exact Node IDs of your running OPC UA server.
3. **Run the Script:**
```bash
python gateway.py
```
4. **Stop the Benchmark:** Press `Ctrl+C` to cleanly exit the loop, disconnect from the server, and finalize the metrics log.

---

### Metrics Output (`naive_baseline_metrics.csv`)
Upon execution, the script generates a real-time CSV file logging the performance of the translation loop. 

**Columns:**
* **`System_Time`:** Standard Unix timestamp of the event.
* **`Message_Type`:** The MAVLink message processed (e.g., `GLOBAL_POSITION_INT`).
* **`MAVLink_Seq`:** The sequence number (0-255) of the incoming MAVLink packet.
* **`Dropped_Since_Last`:** The number of UDP packets dropped by the OS buffer since the last successfully processed message. A number higher than 0 indicates the gateway is failing to keep up with the telemetry rate.
* **`Processing_Latency_ms`:** The time taken (in milliseconds) to perform the math and wait for the OPC UA server to acknowledge the write. 
* **`CPU_Usage_%`:** The current CPU utilization of the Python process during the translation cycle.
* **`Memory_RSS_MB`:** The Resident Set Size (RAM) consumed by the script in Megabytes.

---

### How to Add Support for New Sensors

Because this is a monolithic script, adding new telemetry parameters requires manually hardcoding the mapping logic. Follow these three steps:

**Step 1: Define the target OPC UA Node**
At the top of the `main()` function, add a variable linking to the new node in your server.
```python
# Example: Adding a Lidar distance sensor
lidar_node = client.get_node("ns=2;s=AAS.Sensors.Lidar.Distance")
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