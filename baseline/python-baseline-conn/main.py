import asyncio
import time
import csv
import psutil
import os
from pymavlink import mavutil
from asyncua import Client, ua

# ==========================================
# HARDCODED CONFIGURATION
# ==========================================
MAVLINK_CONNECTION_STRING = 'udp:0.0.0.0:14550'
OPCUA_SERVER_ENDPOINT = 'opc.tcp://127.0.0.1:4840/'
CSV_FILENAME = 'naive_baseline_metrics.csv'
HEARTBEAT_TIMEOUT_S = 10

# MAVLink's documented sentinel for "field not populated" on voltage_battery.
BATTERY_VOLTAGE_UNKNOWN = 65535

async def main():
    # Bound up front so the `finally` block below can always reference them,
    # even if we crash before the main loop starts. Previously these were
    # only assigned deep inside the try block, so a failure during connect()
    # raised an UnboundLocalError from `finally` that masked the real error.
    total_messages_processed = 0
    total_dropped = 0
    opcua_connected = False

    print(f"Connecting to MAVLink on {MAVLINK_CONNECTION_STRING}...")
    mav = mavutil.mavlink_connection(MAVLINK_CONNECTION_STRING)
    if mav.wait_heartbeat(timeout=HEARTBEAT_TIMEOUT_S) is None:
        print(f"No MAVLink heartbeat received within {HEARTBEAT_TIMEOUT_S}s, aborting.")
        return
    print("MAVLink connected!")

    print(f"Connecting to OPC UA Server at {OPCUA_SERVER_ENDPOINT}...")
    client = Client(url=OPCUA_SERVER_ENDPOINT)

    try:
        await client.connect()
        opcua_connected = True
        print("OPC UA connected!")

        # OPC UA Nodes - paths match quad_op.nodeset.xml (uav-aas-tools/opcua_server).
        # ns=2 is where that server's import_xml() lands the AAS namespace at
        # runtime (ns=0 is the OPC UA foundation namespace, ns=1 is the
        # server's own application URI) - update if your server's namespace
        # registration order ever changes.
        lat_node = client.get_node("ns=2;s=Navigation/GlobalPosition/Latitude")
        lon_node = client.get_node("ns=2;s=Navigation/GlobalPosition/Longitude")
        alt_node = client.get_node("ns=2;s=Navigation/GlobalPosition/AltitudeMSL")
        bat_node = client.get_node("ns=2;s=Power/Voltage")

        # ==========================================
        # METRICS TRACKING SETUP
        # ==========================================
        last_seq = -1

        process = psutil.Process(os.getpid())
        process.cpu_percent()  # Prime it - the first real call always returns a meaningless 0.0

        t_prev_end = time.perf_counter()

        # Write mode (overwrites any previous run's CSV); line-buffered so
        # rows already written survive if the process crashes mid-run,
        # instead of sitting lost in an unflushed write buffer.
        with open(CSV_FILENAME, mode='w', newline='', buffering=1) as file:
            writer = csv.writer(file)

            # Write CSV Header
            writer.writerow(["System_Time", "Message_Type", "MAVLink_Seq", "Dropped_Since_Last", "Queue_Wait_ms", "Processing_Latency_ms", "CPU_Percent", "Memory_MB"])

            print("Starting instrumented read-write loop... (Press Ctrl+C to stop)")

            while True:
                # 1. Read MAVLink
                msg = mav.recv_match(blocking=True, timeout=1.0)
                if not msg:
                    continue

                # 2. Sequence & Drop Tracking - evaluated for every MAVLink message
                # received, not just the types we act on below. The seq counter
                # increments across ALL message types from the source, so if this
                # only ran after the type filter, every HEARTBEAT/ATTITUDE/etc.
                # message in between would be miscounted as a "dropped" packet.
                current_seq = msg.get_header().seq
                dropped_this_cycle = 0

                if last_seq != -1:
                    # Calculate drops, accounting for MAVLink's 0-255 sequence wrap-around
                    dropped_this_cycle = (current_seq - last_seq - 1) % 256
                    total_dropped += dropped_this_cycle

                last_seq = current_seq

                msg_type = msg.get_type()

                # Only track target messages to avoid logging background noise
                if msg_type not in ['GLOBAL_POSITION_INT', 'SYS_STATUS']:
                    continue

                # Time since the previous tracked message finished: captures
                # queueing/backlog delay from the naive blocking loop, on top
                # of the pure translation+write cost measured below.
                t_start = time.perf_counter()
                queue_wait_ms = (t_start - t_prev_end) * 1000

                # 3. Hardcoded Translation & Blocking Writes
                if msg_type == 'GLOBAL_POSITION_INT':
                    lat = msg.lat / 1e7
                    lon = msg.lon / 1e7
                    alt = msg.alt / 1000.0

                    await lat_node.set_value(ua.DataValue(ua.Variant(lat, ua.VariantType.Double)))
                    await lon_node.set_value(ua.DataValue(ua.Variant(lon, ua.VariantType.Double)))
                    await alt_node.set_value(ua.DataValue(ua.Variant(alt, ua.VariantType.Double)))

                elif msg_type == 'SYS_STATUS':
                    if msg.voltage_battery == BATTERY_VOLTAGE_UNKNOWN:
                        # MAVLink sentinel for "not reported" - skip rather than
                        # writing a bogus 65.535V into the digital twin.
                        t_prev_end = time.perf_counter()
                        continue
                    voltage = msg.voltage_battery / 1000.0
                    await bat_node.set_value(ua.DataValue(ua.Variant(voltage, ua.VariantType.Double)))

                # End latency timer
                t_end = time.perf_counter()
                latency_ms = (t_end - t_start) * 1000  # Convert to milliseconds
                t_prev_end = t_end

                # Capture CPU and Memory
                cpu_percent = process.cpu_percent()
                mem_info = process.memory_info().rss / (1024 * 1024) # Convert bytes to MB

                total_messages_processed += 1

                # 4. Log to CSV
                writer.writerow([
                    time.time(),
                    msg_type,
                    current_seq,
                    dropped_this_cycle,
                    round(queue_wait_ms, 3),
                    round(latency_ms, 3),
                    round(cpu_percent, 2),
                    round(mem_info, 2)
                ])
                # Print a summary to console every 50 messages so you can watch it fail
                if total_messages_processed % 50 == 0:
                    print(f"Processed: {total_messages_processed} | Total Dropped: {total_dropped} | Last Latency: {latency_ms:.1f}ms")

    except KeyboardInterrupt:
        print("\nBenchmark stopped by user.")
    except Exception as e:
        print(f"\nGateway crashed: {e}")
    finally:
        print(f"Final Stats: {total_messages_processed} processed, {total_dropped} dropped.")
        if opcua_connected:
            print("Disconnecting OPC UA...")
            await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())