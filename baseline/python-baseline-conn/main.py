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
MAVLINK_CONNECTION_STRING = 'udp:127.0.0.1:14550'
OPCUA_SERVER_ENDPOINT = 'opc.tcp://127.0.0.1:4840/freeopcua/server/'
CSV_FILENAME = 'naive_baseline_metrics.csv'

async def main():
    print(f"Connecting to MAVLink on {MAVLINK_CONNECTION_STRING}...")
    mav = mavutil.mavlink_connection(MAVLINK_CONNECTION_STRING)
    mav.wait_heartbeat()
    print("MAVLink connected!")

    print(f"Connecting to OPC UA Server at {OPCUA_SERVER_ENDPOINT}...")
    client = Client(url=OPCUA_SERVER_ENDPOINT)
    
    try:
        await client.connect()
        print("OPC UA connected!")

        # OPC UA Nodes (Update these to match your server)
        lat_node = client.get_node("ns=2;s=AAS.Navigation.GlobalPosition.Latitude")
        lon_node = client.get_node("ns=2;s=AAS.Navigation.GlobalPosition.Longitude")
        alt_node = client.get_node("ns=2;s=AAS.Navigation.GlobalPosition.Altitude")
        bat_node = client.get_node("ns=2;s=AAS.PowerSystems.Battery.Voltage")

        # ==========================================
        # METRICS TRACKING SETUP
        # ==========================================
        last_seq = -1
        total_messages_processed = 0
        total_dropped = 0

        process = psutil.Process(os.getpid())

        # Open CSV file in append mode
        with open(CSV_FILENAME, mode='w', newline='') as file:
            writer = csv.writer(file)
            
            # Write CSV Header
            writer.writerow(["System_Time", "Message_Type", "MAVLink_Seq", "Dropped_Since_Last", "Processing_Latency_ms", "CPU_Percent", "Memory_MB"])

            print("Starting instrumented read-write loop... (Press Ctrl+C to stop)")

            while True:
                # 1. Read MAVLink
                msg = mav.recv_match(blocking=True, timeout=1.0)
                if not msg:
                    continue

                msg_type = msg.get_type()
                
                # Only track target messages to avoid logging background noise
                if msg_type not in ['GLOBAL_POSITION_INT', 'SYS_STATUS']:
                    continue

                # Start latency timer
                t_start = time.perf_counter()

                # 2. Sequence & Drop Tracking
                current_seq = msg.get_header().seq
                dropped_this_cycle = 0
                
                if last_seq != -1:
                    # Calculate drops, accounting for MAVLink's 0-255 sequence wrap-around
                    dropped_this_cycle = (current_seq - last_seq - 1) % 256
                    total_dropped += dropped_this_cycle
                
                last_seq = current_seq

                # 3. Hardcoded Translation & Blocking Writes
                if msg_type == 'GLOBAL_POSITION_INT':
                    lat = msg.lat / 1e7
                    lon = msg.lon / 1e7
                    alt = msg.alt / 1000.0

                    await lat_node.set_value(ua.DataValue(ua.Variant(lat, ua.VariantType.Double)))
                    await lon_node.set_value(ua.DataValue(ua.Variant(lon, ua.VariantType.Double)))
                    await alt_node.set_value(ua.DataValue(ua.Variant(alt, ua.VariantType.Double)))
                    
                elif msg_type == 'SYS_STATUS':
                    voltage = msg.voltage_battery / 1000.0
                    await bat_node.set_value(ua.DataValue(ua.Variant(voltage, ua.VariantType.Double)))

                # End latency timer
                t_end = time.perf_counter()
                latency_ms = (t_end - t_start) * 1000  # Convert to milliseconds

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
        print("Disconnecting OPC UA...")
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())