import asyncio
import time
import csv
import json
import psutil
import os
from pymavlink import mavutil
from asyncua import Client, ua

# ==========================================
# CONFIGURATION
# ==========================================
# Defaults match local dev usage (gateway and OPC UA server on the same
# machine). All three are overridable via environment variables so the
# Docker image can be pointed at a different MAVLink source / OPC UA target
# per deployment (e.g. the Raspberry Pi) without editing this file or
# rebuilding the image.
MAVLINK_CONNECTION_STRING = os.environ.get('MAVLINK_CONNECTION_STRING', 'udp:0.0.0.0:14550')
OPCUA_SERVER_ENDPOINT = os.environ.get('OPCUA_SERVER_ENDPOINT', 'opc.tcp://127.0.0.1:4840/')
CSV_FILENAME = os.environ.get('CSV_FILENAME', 'naive_baseline_metrics.csv')
HEARTBEAT_TIMEOUT_S = 10

# Optional: path to the exact .tlog file being fed to this gateway via
# mavlink-replay's replay.py (read-only reference, not a live connection).
# replay.py sends every message in that file exactly once with no filtering,
# so a full pre-scan of it gives an exact, ground-truth per-message-type
# expected count for the run - independent of MAVLink's wire sequence
# number, which the current synth.py/replay.py never advance (both call
# msg.pack() without ever calling mav.send(), so pymavlink's seq counter -
# only incremented inside send() - stays at 0 for every packet on the wire).
# That makes header-seq-based drop counting meaningless with this tooling
# today. Leave unset for live/physical-drone deployments, where there is no
# reference file to compare against.
SOURCE_TLOG_PATH = os.environ.get('SOURCE_TLOG_PATH')

# MAVLink's documented sentinel for "field not populated" on voltage_battery.
BATTERY_VOLTAGE_UNKNOWN = 65535

# Message types this gateway translates, matching every sourceMessage in
# config/mappings/mavlink_ops.yaml except ESC_STATUS - that message isn't
# defined in pymavlink's installed dialect (nor emitted by the synthetic
# replay data), so there's nothing to receive and translate.
TRACKED_MESSAGE_TYPES = [
    'HEARTBEAT',
    'SYS_STATUS',
    'GLOBAL_POSITION_INT',
    'LOCAL_POSITION_NED',
    'GPS_RAW_INT',
    'ALTITUDE',
    'ATTITUDE',
    'ATTITUDE_QUATERNION',
    'ATTITUDE_TARGET',
    'SERVO_OUTPUT_RAW',
    'VFR_HUD',
    'OPEN_DRONE_ID_SYSTEM',
]

def scan_expected_counts(tlog_path, tracked_types):
    # Reads the file directly - independent of the live UDP connection and of
    # whatever replay.py is doing with the same file over the network.
    counts = {t: 0 for t in tracked_types}
    log = mavutil.mavlink_connection(tlog_path)
    while True:
        msg = log.recv_match(blocking=False)
        if msg is None:
            break
        msg_type = msg.get_type()
        if msg_type in counts:
            counts[msg_type] += 1
    return counts

async def main():
    # Bound up front so the `finally` block below can always reference them,
    # even if we crash before the main loop starts. Previously these were
    # only assigned deep inside the try block, so a failure during connect()
    # raised an UnboundLocalError from `finally` that masked the real error.
    total_messages_processed = 0
    total_write_success = 0
    total_write_failures = 0
    total_dropped = 0
    opcua_connected = False
    arrived_counts = {t: 0 for t in TRACKED_MESSAGE_TYPES}
    expected_counts = {}

    if SOURCE_TLOG_PATH:
        print(f"Pre-scanning {SOURCE_TLOG_PATH} for expected per-type message counts...")
        expected_counts = scan_expected_counts(SOURCE_TLOG_PATH, TRACKED_MESSAGE_TYPES)
        print(f"Expected (tracked types only): {sum(expected_counts.values())} messages "
              f"across {sum(1 for c in expected_counts.values() if c > 0)} types")

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

        # OPC UA Nodes - paths match quad_op.nodeset.xml (uav-aas-tools/opcua_server)
        # and mirror config/mappings/mavlink_ops.yaml from the Go connector,
        # section by section. ns=2 is where that server's import_xml() lands the
        # AAS namespace at runtime (ns=0 is the OPC UA foundation namespace, ns=1
        # is the server's own application URI) - update if your server's
        # namespace registration order ever changes.

        # --- 1. Identification (OPEN_DRONE_ID_SYSTEM) ---
        operator_location_type_node = client.get_node("ns=2;s=Identification/RemoteID_System/OperatorLocationType")
        classification_type_node = client.get_node("ns=2;s=Identification/RemoteID_System/ClassificationType")
        # Note: mavlink_ops.yaml also maps a "UaType" field for this message, but
        # OPEN_DRONE_ID_SYSTEM has no such field in the MAVLink spec (it belongs
        # to OPEN_DRONE_ID_BASIC_ID) - the Go connector's reflection-based field
        # lookup silently no-ops on it too, so it's intentionally left unmapped.

        # --- 2. Status (HEARTBEAT) ---
        vehicle_type_node = client.get_node("ns=2;s=Status/VehicleType")
        autopilot_type_node = client.get_node("ns=2;s=Status/AutopilotType")
        system_status_node = client.get_node("ns=2;s=Status/SystemStatus")

        # --- 3. Power (SYS_STATUS) ---
        bat_node = client.get_node("ns=2;s=Power/Voltage")
        current_node = client.get_node("ns=2;s=Power/Current")
        remaining_node = client.get_node("ns=2;s=Power/Remaining")
        cpu_load_node = client.get_node("ns=2;s=Power/CPULoad")
        drop_rate_comm_node = client.get_node("ns=2;s=Power/DropRateComm")

        # --- 4. Navigation: GlobalPosition (GLOBAL_POSITION_INT) ---
        lat_node = client.get_node("ns=2;s=Navigation/GlobalPosition/Latitude")
        lon_node = client.get_node("ns=2;s=Navigation/GlobalPosition/Longitude")
        alt_node = client.get_node("ns=2;s=Navigation/GlobalPosition/AltitudeMSL")
        hdg_node = client.get_node("ns=2;s=Navigation/GlobalPosition/Heading")

        # --- 4. Navigation: LocalPositionNED (LOCAL_POSITION_NED) ---
        local_x_node = client.get_node("ns=2;s=Navigation/LocalPositionNED/X")
        local_y_node = client.get_node("ns=2;s=Navigation/LocalPositionNED/Y")
        local_z_node = client.get_node("ns=2;s=Navigation/LocalPositionNED/Z")
        local_vx_node = client.get_node("ns=2;s=Navigation/LocalPositionNED/VX")
        local_vy_node = client.get_node("ns=2;s=Navigation/LocalPositionNED/VY")
        local_vz_node = client.get_node("ns=2;s=Navigation/LocalPositionNED/VZ")

        # --- 4. Navigation: GPS_Raw (GPS_RAW_INT) ---
        gps_fix_type_node = client.get_node("ns=2;s=Navigation/GPS_Raw/FixType")
        gps_sats_node = client.get_node("ns=2;s=Navigation/GPS_Raw/SatellitesVisible")
        gps_hdop_node = client.get_node("ns=2;s=Navigation/GPS_Raw/HDOP")

        # --- 4. Navigation: TerrainData (ALTITUDE) ---
        terrain_alt_node = client.get_node("ns=2;s=Navigation/TerrainData/TerrainAltitude")
        bottom_clearance_node = client.get_node("ns=2;s=Navigation/TerrainData/BottomClearance")

        # --- 5. Attitude: EulerAngles (ATTITUDE) ---
        roll_node = client.get_node("ns=2;s=Attitude/EulerAngles/Roll")
        pitch_node = client.get_node("ns=2;s=Attitude/EulerAngles/Pitch")
        yaw_node = client.get_node("ns=2;s=Attitude/EulerAngles/Yaw")
        rollspeed_node = client.get_node("ns=2;s=Attitude/EulerAngles/RollSpeed")
        pitchspeed_node = client.get_node("ns=2;s=Attitude/EulerAngles/PitchSpeed")
        yawspeed_node = client.get_node("ns=2;s=Attitude/EulerAngles/YawSpeed")

        # --- 5. Attitude: Quaternion (ATTITUDE_QUATERNION) ---
        q1_node = client.get_node("ns=2;s=Attitude/Quaternion/Q1")
        q2_node = client.get_node("ns=2;s=Attitude/Quaternion/Q2")
        q3_node = client.get_node("ns=2;s=Attitude/Quaternion/Q3")
        q4_node = client.get_node("ns=2;s=Attitude/Quaternion/Q4")

        # --- 5. Attitude: AttitudeTarget (ATTITUDE_TARGET) ---
        body_roll_rate_node = client.get_node("ns=2;s=Attitude/AttitudeTarget/BodyRollRate")
        body_pitch_rate_node = client.get_node("ns=2;s=Attitude/AttitudeTarget/BodyPitchRate")
        body_yaw_rate_node = client.get_node("ns=2;s=Attitude/AttitudeTarget/BodyYawRate")
        thrust_node = client.get_node("ns=2;s=Attitude/AttitudeTarget/Thrust")

        # --- 6. Actuation: ServoOutput (SERVO_OUTPUT_RAW) ---
        servo1_node = client.get_node("ns=2;s=Actuation/ServoOutput/Servo1")
        servo2_node = client.get_node("ns=2;s=Actuation/ServoOutput/Servo2")
        servo3_node = client.get_node("ns=2;s=Actuation/ServoOutput/Servo3")
        servo4_node = client.get_node("ns=2;s=Actuation/ServoOutput/Servo4")
        # (Actuation/ESC_Status/* nodes exist on the server but are left unwritten -
        # see the ESC_STATUS note by TRACKED_MESSAGE_TYPES above.)

        # --- 7. HUD (VFR_HUD) ---
        airspeed_node = client.get_node("ns=2;s=HUD/Airspeed")
        groundspeed_node = client.get_node("ns=2;s=HUD/Groundspeed")
        hud_heading_node = client.get_node("ns=2;s=HUD/Heading")
        throttle_node = client.get_node("ns=2;s=HUD/Throttle")
        hud_altitude_node = client.get_node("ns=2;s=HUD/Altitude")
        climb_rate_node = client.get_node("ns=2;s=HUD/ClimbRate")

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
            writer.writerow(["System_Time", "Message_Type", "MAVLink_Seq", "Dropped_Since_Last", "Queue_Wait_ms", "Processing_Latency_ms", "CPU_Percent", "Memory_MB", "Write_Success", "Error"])

            print("Starting instrumented read-write loop... (Press Ctrl+C to stop)")

            while True:
                # 1. Read MAVLink
                msg = mav.recv_match(blocking=True, timeout=1.0)
                if not msg:
                    continue

                # 2. Sequence & Drop Tracking - evaluated for every MAVLink message
                # received, not just the types we act on below. The seq counter
                # increments across ALL message types from the source, so if this
                # only ran after the type filter, every message we don't otherwise
                # track in between would be miscounted as a "dropped" packet.
                current_seq = msg.get_header().seq
                dropped_this_cycle = 0

                if last_seq != -1:
                    # Calculate drops, accounting for MAVLink's 0-255 sequence wrap-around
                    dropped_this_cycle = (current_seq - last_seq - 1) % 256
                    total_dropped += dropped_this_cycle

                last_seq = current_seq

                msg_type = msg.get_type()

                # Only track target messages to avoid logging background noise
                if msg_type not in TRACKED_MESSAGE_TYPES:
                    continue

                # Time since the previous tracked message finished: captures
                # queueing/backlog delay from the naive blocking loop, on top
                # of the pure translation+write cost measured below.
                t_start = time.perf_counter()
                queue_wait_ms = (t_start - t_prev_end) * 1000

                # 3. Hardcoded Translation & Blocking Writes.
                # Scaling and target OPC UA types below mirror
                # config/mappings/mavlink_ops.yaml + quad_op.nodeset.xml exactly:
                # like the Go connector, MAVLink "unknown" sentinel values (e.g.
                # eph/hdg/satellites_visible at their UINT16_MAX default) are NOT
                # filtered out here - they're scaled and written through as-is,
                # except voltage_battery below, which was already special-cased
                # before this change and is left as-is.
                #
                # Wrapped per message, not per run: asyncua raises on a failed
                # write (Node.write_attribute() calls result[0].check(), which
                # raises ua.UaStatusCodeError on a bad status code), so without
                # this a single transient write error - plausible over a real,
                # non-loopback LAN hop to the OPC UA server across a 15-minute
                # run - would otherwise propagate out of the whole loop and end
                # the benchmark, discarding all data collected up to that point.
                # This does not make the gateway any less naive: writes are
                # still sequential, blocking, and unbatched; a mid-message
                # failure still abandons whatever fields of that message were
                # not yet written (no per-field recovery), and there is still
                # no reconnection logic, so a truly dead connection shows up
                # honestly as a run of consecutive failures rather than being
                # silently recovered from. It only stops one bad write from
                # discarding an otherwise-good run, and gives Write_Success
                # below a real per-message signal instead of every logged row
                # being unconditionally "successful" by construction.
                write_success = True
                write_error = ""
                try:
                    if msg_type == 'HEARTBEAT':
                        await vehicle_type_node.set_value(ua.DataValue(ua.Variant(int(msg.type), ua.VariantType.Int32)))
                        await autopilot_type_node.set_value(ua.DataValue(ua.Variant(int(msg.autopilot), ua.VariantType.Int32)))
                        await system_status_node.set_value(ua.DataValue(ua.Variant(int(msg.system_status), ua.VariantType.Int32)))

                    elif msg_type == 'SYS_STATUS':
                        if msg.voltage_battery != BATTERY_VOLTAGE_UNKNOWN:
                            # MAVLink sentinel for "not reported" - skip rather than
                            # writing a bogus 65.535V into the digital twin.
                            voltage = msg.voltage_battery / 1000.0
                            await bat_node.set_value(ua.DataValue(ua.Variant(voltage, ua.VariantType.Double)))

                        current = msg.current_battery * 0.01  # cA -> A
                        await current_node.set_value(ua.DataValue(ua.Variant(current, ua.VariantType.Double)))

                        await remaining_node.set_value(ua.DataValue(ua.Variant(int(msg.battery_remaining), ua.VariantType.Int32)))

                        cpu_load = int(msg.load * 0.1)  # d% -> %, truncated (Power/CPULoad is Int32)
                        await cpu_load_node.set_value(ua.DataValue(ua.Variant(cpu_load, ua.VariantType.Int32)))

                        drop_rate = int(msg.drop_rate_comm * 0.01)  # c% -> %, truncated (Power/DropRateComm is Int32)
                        await drop_rate_comm_node.set_value(ua.DataValue(ua.Variant(drop_rate, ua.VariantType.Int32)))

                    elif msg_type == 'GLOBAL_POSITION_INT':
                        lat = msg.lat / 1e7
                        lon = msg.lon / 1e7
                        alt = msg.alt / 1000.0
                        hdg = msg.hdg * 0.01  # cdeg -> deg

                        await lat_node.set_value(ua.DataValue(ua.Variant(lat, ua.VariantType.Double)))
                        await lon_node.set_value(ua.DataValue(ua.Variant(lon, ua.VariantType.Double)))
                        await alt_node.set_value(ua.DataValue(ua.Variant(alt, ua.VariantType.Double)))
                        await hdg_node.set_value(ua.DataValue(ua.Variant(hdg, ua.VariantType.Double)))

                    elif msg_type == 'LOCAL_POSITION_NED':
                        await local_x_node.set_value(ua.DataValue(ua.Variant(msg.x, ua.VariantType.Double)))
                        await local_y_node.set_value(ua.DataValue(ua.Variant(msg.y, ua.VariantType.Double)))
                        await local_z_node.set_value(ua.DataValue(ua.Variant(msg.z, ua.VariantType.Double)))
                        await local_vx_node.set_value(ua.DataValue(ua.Variant(msg.vx, ua.VariantType.Double)))
                        await local_vy_node.set_value(ua.DataValue(ua.Variant(msg.vy, ua.VariantType.Double)))
                        await local_vz_node.set_value(ua.DataValue(ua.Variant(msg.vz, ua.VariantType.Double)))

                    elif msg_type == 'GPS_RAW_INT':
                        await gps_fix_type_node.set_value(ua.DataValue(ua.Variant(int(msg.fix_type), ua.VariantType.Int32)))
                        await gps_sats_node.set_value(ua.DataValue(ua.Variant(int(msg.satellites_visible), ua.VariantType.Int32)))

                        hdop = msg.eph * 0.01  # cm -> m
                        await gps_hdop_node.set_value(ua.DataValue(ua.Variant(hdop, ua.VariantType.Double)))

                    elif msg_type == 'ALTITUDE':
                        # Navigation/TerrainData/* nodes are 32-bit Float, not Double.
                        await terrain_alt_node.set_value(ua.DataValue(ua.Variant(msg.altitude_terrain, ua.VariantType.Float)))
                        await bottom_clearance_node.set_value(ua.DataValue(ua.Variant(msg.bottom_clearance, ua.VariantType.Float)))

                    elif msg_type == 'ATTITUDE':
                        await roll_node.set_value(ua.DataValue(ua.Variant(msg.roll, ua.VariantType.Double)))
                        await pitch_node.set_value(ua.DataValue(ua.Variant(msg.pitch, ua.VariantType.Double)))
                        await yaw_node.set_value(ua.DataValue(ua.Variant(msg.yaw, ua.VariantType.Double)))
                        await rollspeed_node.set_value(ua.DataValue(ua.Variant(msg.rollspeed, ua.VariantType.Double)))
                        await pitchspeed_node.set_value(ua.DataValue(ua.Variant(msg.pitchspeed, ua.VariantType.Double)))
                        await yawspeed_node.set_value(ua.DataValue(ua.Variant(msg.yawspeed, ua.VariantType.Double)))

                    elif msg_type == 'ATTITUDE_QUATERNION':
                        await q1_node.set_value(ua.DataValue(ua.Variant(msg.q1, ua.VariantType.Double)))
                        await q2_node.set_value(ua.DataValue(ua.Variant(msg.q2, ua.VariantType.Double)))
                        await q3_node.set_value(ua.DataValue(ua.Variant(msg.q3, ua.VariantType.Double)))
                        await q4_node.set_value(ua.DataValue(ua.Variant(msg.q4, ua.VariantType.Double)))

                    elif msg_type == 'ATTITUDE_TARGET':
                        await body_roll_rate_node.set_value(ua.DataValue(ua.Variant(msg.body_roll_rate, ua.VariantType.Double)))
                        await body_pitch_rate_node.set_value(ua.DataValue(ua.Variant(msg.body_pitch_rate, ua.VariantType.Double)))
                        await body_yaw_rate_node.set_value(ua.DataValue(ua.Variant(msg.body_yaw_rate, ua.VariantType.Double)))
                        await thrust_node.set_value(ua.DataValue(ua.Variant(msg.thrust, ua.VariantType.Double)))

                    elif msg_type == 'SERVO_OUTPUT_RAW':
                        await servo1_node.set_value(ua.DataValue(ua.Variant(int(msg.servo1_raw), ua.VariantType.Int32)))
                        await servo2_node.set_value(ua.DataValue(ua.Variant(int(msg.servo2_raw), ua.VariantType.Int32)))
                        await servo3_node.set_value(ua.DataValue(ua.Variant(int(msg.servo3_raw), ua.VariantType.Int32)))
                        await servo4_node.set_value(ua.DataValue(ua.Variant(int(msg.servo4_raw), ua.VariantType.Int32)))

                    elif msg_type == 'VFR_HUD':
                        await airspeed_node.set_value(ua.DataValue(ua.Variant(msg.airspeed, ua.VariantType.Double)))
                        await groundspeed_node.set_value(ua.DataValue(ua.Variant(msg.groundspeed, ua.VariantType.Double)))
                        await hud_heading_node.set_value(ua.DataValue(ua.Variant(int(msg.heading), ua.VariantType.Int32)))
                        await throttle_node.set_value(ua.DataValue(ua.Variant(int(msg.throttle), ua.VariantType.Int32)))
                        await hud_altitude_node.set_value(ua.DataValue(ua.Variant(msg.alt, ua.VariantType.Double)))
                        await climb_rate_node.set_value(ua.DataValue(ua.Variant(msg.climb, ua.VariantType.Double)))

                    elif msg_type == 'OPEN_DRONE_ID_SYSTEM':
                        await operator_location_type_node.set_value(ua.DataValue(ua.Variant(int(msg.operator_location_type), ua.VariantType.Int32)))
                        await classification_type_node.set_value(ua.DataValue(ua.Variant(int(msg.classification_type), ua.VariantType.Int32)))
                except Exception as e:
                    write_success = False
                    write_error = str(e)[:200]
                    print(f"Write failed for {msg_type} (seq={current_seq}): {write_error}")

                # End latency timer
                t_end = time.perf_counter()
                latency_ms = (t_end - t_start) * 1000  # Convert to milliseconds
                t_prev_end = t_end

                # Capture CPU and Memory
                cpu_percent = process.cpu_percent()
                mem_info = process.memory_info().rss / (1024 * 1024) # Convert bytes to MB

                total_messages_processed += 1
                if write_success:
                    total_write_success += 1
                    arrived_counts[msg_type] += 1
                else:
                    total_write_failures += 1

                # 4. Log to CSV
                writer.writerow([
                    time.time(),
                    msg_type,
                    current_seq,
                    dropped_this_cycle,
                    round(queue_wait_ms, 3),
                    round(latency_ms, 3),
                    round(cpu_percent, 2),
                    round(mem_info, 2),
                    write_success,
                    write_error,
                ])
                # Print a summary to console every 50 messages so you can watch it fail
                if total_messages_processed % 50 == 0:
                    if expected_counts:
                        expected_total = sum(expected_counts.values())
                        delivery_pct = (total_write_success / expected_total * 100) if expected_total else 0.0
                        print(f"Processed: {total_messages_processed} (success: {total_write_success}, write failures: {total_write_failures}) | "
                              f"Delivery: {total_write_success}/{expected_total} ({delivery_pct:.1f}%) | Last Latency: {latency_ms:.1f}ms")
                    else:
                        print(f"Processed: {total_messages_processed} (success: {total_write_success}, write failures: {total_write_failures}) | "
                              f"Total Dropped (raw wire-seq, unreliable - see README): {total_dropped} | Last Latency: {latency_ms:.1f}ms")

    except KeyboardInterrupt:
        print("\nBenchmark stopped by user.")
    except Exception as e:
        print(f"\nGateway crashed: {e}")
    finally:
        write_success_pct = (total_write_success / total_messages_processed * 100) if total_messages_processed else 0.0
        print(f"Final Stats: {total_messages_processed} processed "
              f"({total_write_success} write-confirmed [{write_success_pct:.2f}%], {total_write_failures} write failures), "
              f"{total_dropped} dropped (raw wire-seq count - unreliable with the current replay/synth tooling, see README).")

        if expected_counts:
            print("Per-type delivery ratio (arrived / expected from source .tlog, tracked types only):")
            summary = {}
            for msg_type in sorted(expected_counts):
                expected = expected_counts[msg_type]
                arrived = arrived_counts.get(msg_type, 0)
                pct = (arrived / expected * 100) if expected else 0.0
                print(f"  {msg_type:24s} {arrived:6d} / {expected:6d}  ({pct:5.1f}%)")
                summary[msg_type] = {"arrived": arrived, "expected": expected, "delivery_pct": round(pct, 2)}

            total_expected = sum(expected_counts.values())
            total_arrived = sum(arrived_counts.get(t, 0) for t in expected_counts)
            overall_pct = (total_arrived / total_expected * 100) if total_expected else 0.0
            print(f"  {'OVERALL':24s} {total_arrived:6d} / {total_expected:6d}  ({overall_pct:5.1f}%)")
            summary["_overall"] = {"arrived": total_arrived, "expected": total_expected, "delivery_pct": round(overall_pct, 2)}

            summary_path = os.path.splitext(CSV_FILENAME)[0] + "_delivery_summary.json"
            with open(summary_path, "w") as summary_file:
                json.dump(summary, summary_file, indent=2)
            print(f"Delivery summary written to {summary_path}")

        if opcua_connected:
            print("Disconnecting OPC UA...")
            await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
