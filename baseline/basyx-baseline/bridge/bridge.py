import base64
import json
import os
import time
import csv

import psutil
import requests
from pymavlink import mavutil

# ==========================================
# CONFIGURATION
# ==========================================
# Defaults match local dev usage (bridge and BaSyx on the same machine). All
# overridable via environment variables so the Docker image can be pointed at
# a different MAVLink source / BaSyx instance per deployment (e.g. the
# Raspberry Pi) without editing this file or rebuilding the image - same
# pattern as ../../python-baseline-conn/main.py.
MAVLINK_CONNECTION_STRING = os.environ.get('MAVLINK_CONNECTION_STRING', 'udp:0.0.0.0:14550')
BASYX_BASE_URL = os.environ.get('BASYX_BASE_URL', 'http://127.0.0.1:8081').rstrip('/')
CSV_FILENAME = os.environ.get('CSV_FILENAME', 'basyx_baseline_bridge_metrics.csv')
HEARTBEAT_TIMEOUT_S = 10
BASYX_READY_TIMEOUT_S = 60
# Bounded so a truly dead/stalled BaSyx instance shows up as a run of
# consecutive per-message failures (like main.py's unbounded OPC UA writes
# would eventually time out at the transport level) rather than hanging the
# naive, single-threaded loop forever on one property.
REQUEST_TIMEOUT_S = 5.0

# Same reasoning as python-baseline-conn/main.py's SOURCE_TLOG_PATH: optional
# read-only pre-scan of the exact .tlog fed into replay.py, giving an exact
# ground-truth per-message-type expected count independent of MAVLink's wire
# sequence number (unusable with replay.py/synth.py - see that file's docstring).
SOURCE_TLOG_PATH = os.environ.get('SOURCE_TLOG_PATH')

# MAVLink's documented sentinel for "field not populated" on voltage_battery.
BATTERY_VOLTAGE_UNKNOWN = 65535

# Message types this bridge translates - identical set to
# python-baseline-conn/main.py (see that file's comment on ESC_STATUS).
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

# Submodel ids, matching ../aas_definitions/uav_environment.json (converted
# 1:1 from config/aas/quad_op.json - same ids, same idShorts, regenerable via
# ../aas_definitions/generate_environment.py).
#
# Every do_patch() call below is hand-duplicated from
# config/mappings/mavlink_ops.yaml (same source, same scale factors/sentinels
# as ../python-baseline-conn/main.py's node writes), not generated from it -
# verified entry-by-entry against that file 2026-08-09 via
# ../verify_mapping.py, which re-run after any change to either file.
# Two categories of YAML entries are intentionally NOT implemented here,
# matching main.py exactly:
#   - OPEN_DRONE_ID_SYSTEM.UaType: not a real field on this MAVLink message
#     (it belongs to OPEN_DRONE_ID_BASIC_ID) - the Go connector's own
#     reflection-based field lookup silently no-ops on it too, so leaving it
#     unmapped matches the actual system under comparison, not just this
#     baseline's convenience.
#   - ESC_STATUS.*: not in this project's installed pymavlink dialect, and
#     not emitted by the synthetic replay traces - see TRACKED_MESSAGE_TYPES.
SUBMODEL_IDS = {
    'Identification': 'urn:submodel:identification:001',
    'Status': 'urn:submodel:status:001',
    'Power': 'urn:submodel:power:001',
    'Navigation': 'urn:submodel:navigation:001',
    'Attitude': 'urn:submodel:attitude:001',
    'Actuation': 'urn:submodel:actuation:001',
    'HUD': 'urn:submodel:hud:001',
}


def b64url(raw):
    # Unpadded Base64URL, matching BaSyx's own Base64UrlEncoder (RFC 4648
    # section 5, padding stripped) - required for every Identifiable id
    # (AAS/Submodel) that appears in a BaSyx REST path or MQTT topic segment.
    return base64.urlsafe_b64encode(raw.encode('utf-8')).rstrip(b'=').decode('ascii')


# Precomputed once at import time - the encoded submodel id never changes
# during a run, so there is no reason to re-encode it on every property write.
SUBMODEL_IDS_B64 = {k: b64url(v) for k, v in SUBMODEL_IDS.items()}


def scan_expected_counts(tlog_path, tracked_types):
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


def wait_for_basyx(session, base_url, timeout_s):
    # BaSyx (a Spring Boot / JVM app) takes several seconds to boot - this is
    # a bounded startup wait, not an optimization, so `docker compose up` on
    # both containers at once doesn't just crash-loop the bridge against a
    # connection-refused BaSyx. Mirrors main.py's blocking wait_heartbeat()
    # pattern: block on the dependency being ready before doing real work.
    deadline = time.time() + timeout_s
    url = f"{base_url}/submodels"
    while time.time() < deadline:
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT_S)
            if resp.status_code < 500:
                return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(1)
    return False


def patch_value(session, base_url, submodel_key, id_short_path, value):
    """PATCH a single property's value via the AAS V3 REST $value endpoint.

    One blocking HTTP round-trip per property, sequential, no batching - the
    naive baseline property under test (see ../README.md). idShortPath
    segments are '.'-separated (AAS V3 idShortPath grammar - confirmed
    against BaSyx's SubmodelElementIdShortPathParser, NOT the '/'-separated
    convention used by the OPC UA/MQTT baseline variants or the Go
    connector's own exporters).

    BaSyx maps this path to PATCH (SubmodelRepositoryHTTPApi.java); PUT gives
    405. The body is the value JSON-encoded as a string, e.g. "82.487" with
    the quotes, even for numbers (a bare number gives 500).
    """
    url = (f"{base_url}/submodels/{SUBMODEL_IDS_B64[submodel_key]}"
           f"/submodel-elements/{id_short_path}/$value")
    resp = session.patch(url, json=str(value), timeout=REQUEST_TIMEOUT_S)
    if not resp.ok:
        # Not raise_for_status(): it drops the response body, which is where
        # Spring Boot names the exception.
        raise requests.exceptions.HTTPError(
            f"{resp.status_code} {resp.reason} for url: {resp.url} - body: {resp.text[:500]!r}",
            response=resp,
        )
    return resp


def main():
    total_messages_processed = 0
    total_write_success = 0
    total_write_failures = 0
    total_dropped = 0
    total_puts_sent = 0
    total_puts_ok = 0
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

    # requests.Session() reuses one urllib3 connection pool with HTTP
    # keep-alive across every PATCH - required: at
    # 200 Hz with a ~3.5x property fan-out this issues ~700 HTTP requests/sec,
    # and without it each PATCH would open/close its own TCP connection,
    # exhausting local ephemeral ports on the Pi within seconds. No further
    # tuning beyond a plain Session() is needed or applied: the loop below is
    # strictly sequential (each PATCH's response is awaited before the next is
    # sent), so at most one connection is ever checked out of the pool at a
    # time - Session()'s own default HTTPAdapter (pool_maxsize=10) already
    # exceeds that, so a custom adapter/pool size would be unused tuning, not
    # naive-but-necessary infrastructure.
    session = requests.Session()

    print(f"Waiting for BaSyx AAS Environment at {BASYX_BASE_URL}...")
    if not wait_for_basyx(session, BASYX_BASE_URL, BASYX_READY_TIMEOUT_S):
        print(f"BaSyx not reachable at {BASYX_BASE_URL} within {BASYX_READY_TIMEOUT_S}s, aborting.")
        return
    print("BaSyx connected!")

    last_seq = -1
    process = psutil.Process(os.getpid())
    process.cpu_percent()  # Prime it - the first real call always returns a meaningless 0.0
    t_prev_end = time.perf_counter()

    with open(CSV_FILENAME, mode='w', newline='', buffering=1) as file:
        writer = csv.writer(file)
        writer.writerow(["System_Time", "Message_Type", "MAVLink_Seq", "Dropped_Since_Last",
                          "Queue_Wait_ms", "Processing_Latency_ms", "CPU_Percent", "Memory_MB",
                          "Write_Success", "Error"])

        print("Starting instrumented read-write loop... (Press Ctrl+C to stop)")

        while True:
            msg = mav.recv_match(blocking=True, timeout=1.0)
            if not msg:
                continue

            current_seq = msg.get_header().seq
            dropped_this_cycle = 0
            if last_seq != -1:
                dropped_this_cycle = (current_seq - last_seq - 1) % 256
                total_dropped += dropped_this_cycle
            last_seq = current_seq

            msg_type = msg.get_type()
            if msg_type not in TRACKED_MESSAGE_TYPES:
                continue

            t_start = time.perf_counter()
            queue_wait_ms = (t_start - t_prev_end) * 1000

            # Wrapped per message, not per run - one transient HTTP failure
            # (a dropped connection, a slow GC pause past REQUEST_TIMEOUT_S)
            # shouldn't discard an entire 15-minute run's data. Writes stay
            # sequential and blocking; a mid-message failure still abandons
            # whatever properties of that message weren't yet PATCHed (no
            # per-field recovery), and there is no retry - a dead BaSyx
            # instance shows up as a run of consecutive failures. Same as
            # main.py's try/except.
            write_success = True
            write_error = ""
            message_puts = 0
            message_puts_ok = 0

            def do_patch(submodel_key, id_short_path, value):
                nonlocal message_puts, message_puts_ok
                message_puts += 1
                patch_value(session, BASYX_BASE_URL, submodel_key, id_short_path, value)
                message_puts_ok += 1

            try:
                if msg_type == 'HEARTBEAT':
                    do_patch('Status', 'VehicleType', int(msg.type))
                    do_patch('Status', 'AutopilotType', int(msg.autopilot))
                    do_patch('Status', 'SystemStatus', int(msg.system_status))

                elif msg_type == 'SYS_STATUS':
                    if msg.voltage_battery != BATTERY_VOLTAGE_UNKNOWN:
                        voltage = msg.voltage_battery / 1000.0
                        do_patch('Power', 'Voltage', voltage)

                    current = msg.current_battery * 0.01  # cA -> A
                    do_patch('Power', 'Current', current)

                    do_patch('Power', 'Remaining', int(msg.battery_remaining))

                    cpu_load = int(msg.load * 0.1)  # d% -> %, truncated
                    do_patch('Power', 'CPULoad', cpu_load)

                    drop_rate = int(msg.drop_rate_comm * 0.01)  # c% -> %, truncated
                    do_patch('Power', 'DropRateComm', drop_rate)

                elif msg_type == 'GLOBAL_POSITION_INT':
                    lat = msg.lat / 1e7
                    lon = msg.lon / 1e7
                    alt = msg.alt / 1000.0
                    hdg = msg.hdg * 0.01  # cdeg -> deg
                    do_patch('Navigation', 'GlobalPosition.Latitude', lat)
                    do_patch('Navigation', 'GlobalPosition.Longitude', lon)
                    do_patch('Navigation', 'GlobalPosition.AltitudeMSL', alt)
                    do_patch('Navigation', 'GlobalPosition.Heading', hdg)

                elif msg_type == 'LOCAL_POSITION_NED':
                    do_patch('Navigation', 'LocalPositionNED.X', msg.x)
                    do_patch('Navigation', 'LocalPositionNED.Y', msg.y)
                    do_patch('Navigation', 'LocalPositionNED.Z', msg.z)
                    do_patch('Navigation', 'LocalPositionNED.VX', msg.vx)
                    do_patch('Navigation', 'LocalPositionNED.VY', msg.vy)
                    do_patch('Navigation', 'LocalPositionNED.VZ', msg.vz)

                elif msg_type == 'GPS_RAW_INT':
                    do_patch('Navigation', 'GPS_Raw.FixType', int(msg.fix_type))
                    do_patch('Navigation', 'GPS_Raw.SatellitesVisible', int(msg.satellites_visible))
                    hdop = msg.eph * 0.01  # cm -> m
                    do_patch('Navigation', 'GPS_Raw.HDOP', hdop)

                elif msg_type == 'ALTITUDE':
                    do_patch('Navigation', 'TerrainData.TerrainAltitude', msg.altitude_terrain)
                    do_patch('Navigation', 'TerrainData.BottomClearance', msg.bottom_clearance)

                elif msg_type == 'ATTITUDE':
                    do_patch('Attitude', 'EulerAngles.Roll', msg.roll)
                    do_patch('Attitude', 'EulerAngles.Pitch', msg.pitch)
                    do_patch('Attitude', 'EulerAngles.Yaw', msg.yaw)
                    do_patch('Attitude', 'EulerAngles.RollSpeed', msg.rollspeed)
                    do_patch('Attitude', 'EulerAngles.PitchSpeed', msg.pitchspeed)
                    do_patch('Attitude', 'EulerAngles.YawSpeed', msg.yawspeed)

                elif msg_type == 'ATTITUDE_QUATERNION':
                    do_patch('Attitude', 'Quaternion.Q1', msg.q1)
                    do_patch('Attitude', 'Quaternion.Q2', msg.q2)
                    do_patch('Attitude', 'Quaternion.Q3', msg.q3)
                    do_patch('Attitude', 'Quaternion.Q4', msg.q4)

                elif msg_type == 'ATTITUDE_TARGET':
                    do_patch('Attitude', 'AttitudeTarget.BodyRollRate', msg.body_roll_rate)
                    do_patch('Attitude', 'AttitudeTarget.BodyPitchRate', msg.body_pitch_rate)
                    do_patch('Attitude', 'AttitudeTarget.BodyYawRate', msg.body_yaw_rate)
                    do_patch('Attitude', 'AttitudeTarget.Thrust', msg.thrust)

                elif msg_type == 'SERVO_OUTPUT_RAW':
                    do_patch('Actuation', 'ServoOutput.Servo1', int(msg.servo1_raw))
                    do_patch('Actuation', 'ServoOutput.Servo2', int(msg.servo2_raw))
                    do_patch('Actuation', 'ServoOutput.Servo3', int(msg.servo3_raw))
                    do_patch('Actuation', 'ServoOutput.Servo4', int(msg.servo4_raw))

                elif msg_type == 'VFR_HUD':
                    do_patch('HUD', 'Airspeed', msg.airspeed)
                    do_patch('HUD', 'Groundspeed', msg.groundspeed)
                    do_patch('HUD', 'Heading', int(msg.heading))
                    do_patch('HUD', 'Throttle', int(msg.throttle))
                    do_patch('HUD', 'Altitude', msg.alt)
                    do_patch('HUD', 'ClimbRate', msg.climb)

                elif msg_type == 'OPEN_DRONE_ID_SYSTEM':
                    do_patch('Identification', 'RemoteID_System.OperatorLocationType', int(msg.operator_location_type))
                    do_patch('Identification', 'RemoteID_System.ClassificationType', int(msg.classification_type))
                    # Note: mavlink_ops.yaml also maps a "UaType" field for this message, but
                    # OPEN_DRONE_ID_SYSTEM has no such field in the MAVLink spec (it belongs to
                    # OPEN_DRONE_ID_BASIC_ID) - intentionally left unmapped, matching main.py.

            except requests.exceptions.RequestException as e:
                write_success = False
                # 600, not 200: patch_value() now embeds the response BODY
                # (up to 500 chars) in the exception message on a non-2xx
                # status - truncating back down to 200 here would cut most
                # of that off before it reaches the CSV, defeating the point.
                write_error = str(e)[:600]
                print(f"Write failed for {msg_type} (seq={current_seq}): {write_error}")

            total_puts_sent += message_puts
            total_puts_ok += message_puts_ok

            t_end = time.perf_counter()
            latency_ms = (t_end - t_start) * 1000
            t_prev_end = t_end

            cpu_percent = process.cpu_percent()
            mem_info = process.memory_info().rss / (1024 * 1024)

            total_messages_processed += 1
            if write_success:
                total_write_success += 1
                arrived_counts[msg_type] += 1
            else:
                total_write_failures += 1

            writer.writerow([
                time.time(), msg_type, current_seq, dropped_this_cycle,
                round(queue_wait_ms, 3), round(latency_ms, 3),
                round(cpu_percent, 2), round(mem_info, 2),
                write_success, write_error,
            ])

            if total_messages_processed % 50 == 0:
                completeness_pct = (total_puts_ok / total_puts_sent * 100) if total_puts_sent else 0.0
                if expected_counts:
                    expected_total = sum(expected_counts.values())
                    delivery_pct = (total_write_success / expected_total * 100) if expected_total else 0.0
                    print(f"Processed: {total_messages_processed} (success: {total_write_success}, write failures: {total_write_failures}) | "
                          f"Delivery: {total_write_success}/{expected_total} ({delivery_pct:.1f}%) | "
                          f"HTTP completeness: {total_puts_ok}/{total_puts_sent} ({completeness_pct:.1f}%) | Last Latency: {latency_ms:.1f}ms")
                else:
                    print(f"Processed: {total_messages_processed} (success: {total_write_success}, write failures: {total_write_failures}) | "
                          f"HTTP completeness: {total_puts_ok}/{total_puts_sent} ({completeness_pct:.1f}%) | Last Latency: {latency_ms:.1f}ms")

    write_success_pct = (total_write_success / total_messages_processed * 100) if total_messages_processed else 0.0
    completeness_pct = (total_puts_ok / total_puts_sent * 100) if total_puts_sent else 0.0
    print(f"Final Stats: {total_messages_processed} processed "
          f"({total_write_success} write-confirmed [{write_success_pct:.2f}%], {total_write_failures} write failures), "
          f"{total_puts_ok}/{total_puts_sent} property PUTs confirmed (HTTP completeness {completeness_pct:.2f}%), "
          f"{total_dropped} dropped (raw wire-seq count - unreliable with the current replay/synth tooling, see python-baseline-conn/README.md).")

    summary = {}
    if expected_counts:
        print("Per-type delivery ratio (arrived / expected from source .tlog, tracked types only):")
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

    summary["_http_completeness"] = {
        "puts_sent": total_puts_sent,
        "puts_ok": total_puts_ok,
        "completeness_pct": round(completeness_pct, 2),
    }

    summary_path = os.path.splitext(CSV_FILENAME)[0] + "_delivery_summary.json"
    with open(summary_path, "w") as summary_file:
        json.dump(summary, summary_file, indent=2)
    print(f"Delivery summary written to {summary_path}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nBenchmark stopped by user.")
