import time
import csv
import json
import base64
import threading
import psutil
import os
from datetime import datetime, timezone
from pymavlink import mavutil
import paho.mqtt.client as mqtt

# ==========================================
# CONFIGURATION
# ==========================================
# Defaults match local dev usage (gateway and MQTT broker on the same
# machine). All are overridable via environment variables so the Docker image
# can be pointed at a different MAVLink source / broker per deployment (e.g.
# the Raspberry Pi) without editing this file or rebuilding the image.
MAVLINK_CONNECTION_STRING = os.environ.get('MAVLINK_CONNECTION_STRING', 'udp:0.0.0.0:14550')
MQTT_BROKER_URL = os.environ.get('MQTT_BROKER_URL', 'tcp://127.0.0.1:1883')
# Same topic prefix as the Go connector's config.yaml (mqtt.topicPrefix), on
# purpose: this lets the same external MQTT subscriber/logging tool used to
# validate the connector's runs (see uav-aas-tools/mqtt_receiver) be reused
# unmodified against this baseline, exactly as the OPC UA variant reuses the
# same quad_op.nodeset.xml node paths as the connector for the same reason.
MQTT_TOPIC_PREFIX = os.environ.get('MQTT_TOPIC_PREFIX', 'aas/uav-1').rstrip('/')
MQTT_CLIENT_ID = os.environ.get('MQTT_CLIENT_ID', 'python-baseline-mqtt-01')
CSV_FILENAME = os.environ.get('CSV_FILENAME', 'naive_baseline_mqtt_metrics.csv')
HEARTBEAT_TIMEOUT_S = 10
MQTT_CONNECT_TIMEOUT_S = 10
# Bounds how long a single publish call can block waiting for paho to hand
# the packet to the socket, mirroring the Go MQTT exporter's own
# token.WaitTimeout(2 * time.Second) for the same QoS 0 publish call
# (pkg/exporters/mqtt/mqtt.go, publish()).
MQTT_PUBLISH_WAIT_S = 2.0

# Optional: path to the exact .tlog file being fed to this gateway via
# mavlink-replay's replay.py (read-only reference, not a live connection).
# See main.py's identical comment for why the wire sequence number
# (Dropped_Since_Last below) can't be trusted with the current replay/synth
# tooling, and why this pre-scan-based ratio is used instead.
SOURCE_TLOG_PATH = os.environ.get('SOURCE_TLOG_PATH')

# MAVLink's documented sentinel for "field not populated" on voltage_battery.
BATTERY_VOLTAGE_UNKNOWN = 65535

# Message types this gateway translates - identical set to main.py (the OPC
# UA baseline), which mirrors every sourceMessage in
# config/mappings/mavlink_ops.yaml except ESC_STATUS (not emitted by the
# synthetic replay data, so nothing to receive and translate either way).
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


def parse_broker_url(url):
    """Splits a 'tcp://host:port' style URL into (host, port) for paho's
    connect(), which takes them as separate arguments rather than a URL (the
    Go connector's mqtt.host config value uses the same 'tcp://host:port'
    shape via paho.mqtt.golang, which does parse the scheme itself)."""
    stripped = url.split('://', 1)[-1]
    host, _, port = stripped.partition(':')
    return host, int(port) if port else 1883


def scan_expected_counts(tlog_path, tracked_types):
    # Identical to main.py's scan_expected_counts: reads the file directly -
    # independent of the live UDP connection and of whatever replay.py is
    # doing with the same file over the network.
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


def b64_submodel(id_short):
    """Base64URL-encodes an AAS Submodel idShort, matching the Go MQTT
    exporter's topic construction exactly:
    base64.URLEncoding.EncodeToString([]byte(event.SubmodelID))
    in pkg/exporters/mqtt/mqtt.go's publish(). Go's base64.URLEncoding pads
    with '=', same as Python's urlsafe_b64encode - both produce identical
    output for the same input."""
    return base64.urlsafe_b64encode(id_short.encode()).decode()


# Precomputed once, matching each Submodel idShort used below (Section III's
# seven Submodels; Identification/Status/Power/Navigation/Attitude/Actuation/
# HUD all appear in the message set this baseline tracks).
SUBMODEL_B64 = {name: b64_submodel(name) for name in
                ['Identification', 'Status', 'Power', 'Navigation', 'Attitude', 'Actuation', 'HUD']}


def topic_for(submodel_id_short, property_path):
    """<prefix>/submodels/<base64(idShort)>/elements/<path>, identical shape
    to the Go connector's topic (mqtt.go's publish()); property_path here is
    already written with '/' separators rather than '.', so no replacement
    step is needed (unlike the Go side, which stores paths dot-separated
    internally and replaces '.' with '/' only at publish time)."""
    return f"{MQTT_TOPIC_PREFIX}/submodels/{SUBMODEL_B64[submodel_id_short]}/elements/{property_path}"


def build_payload(value, timestamp):
    """{"value":<v>,"timestamp":"<iso8601>"}, the same two fields and shape
    as the Go connector's encodePayload (mqtt.go), built with plain
    json.dumps rather than the Go side's hand-rolled, reflection-free
    encoder - intentionally: this baseline is meant to represent the
    generic-serialization path a naive implementation would reach for, which
    is exactly the cost the Go connector's zero-reflection encoding (Section
    IV-E) is contrasted against."""
    return json.dumps({"value": value, "timestamp": timestamp.isoformat().replace("+00:00", "Z")})


def main():
    total_messages_processed = 0
    total_write_success = 0
    total_write_failures = 0
    total_dropped = 0
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

    broker_host, broker_port = parse_broker_url(MQTT_BROKER_URL)
    print(f"Connecting to MQTT broker at {broker_host}:{broker_port}...")

    connected_event = threading.Event()
    connect_error = {}

    def on_connect(client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            connected_event.set()
        else:
            connect_error['reason_code'] = reason_code
            connected_event.set()

    def on_disconnect(client, userdata, flags, reason_code, properties=None):
        print(f"MQTT disconnected (reason_code={reason_code})")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MQTT_CLIENT_ID)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    client.connect(broker_host, broker_port, keepalive=60)
    # loop_start() runs paho's network I/O on its own background thread -
    # required for any use of this library (there is no purely single-thread
    # way to drive it while also doing blocking pymavlink reads below) - but
    # message translation and the publish() calls themselves stay entirely
    # sequential on the main thread, one message at a time, with no worker
    # pool, batching, or coalescing. That sequential, blocking-per-message
    # path - not the literal OS thread count - is the "naive" property this
    # baseline is built to demonstrate, matching the OPC UA variant's use of
    # asyncua's own internally-threaded/event-loop-driven I/O for the same
    # reason.
    client.loop_start()

    if not connected_event.wait(timeout=MQTT_CONNECT_TIMEOUT_S):
        print(f"MQTT CONNACK not received within {MQTT_CONNECT_TIMEOUT_S}s, aborting.")
        client.loop_stop()
        return
    if connect_error:
        print(f"MQTT connection refused (reason_code={connect_error['reason_code']}), aborting.")
        client.loop_stop()
        return
    print("MQTT connected!")

    try:
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

            writer.writerow(["System_Time", "Message_Type", "MAVLink_Seq", "Dropped_Since_Last", "Queue_Wait_ms", "Processing_Latency_ms", "CPU_Percent", "Memory_MB", "Write_Success", "Error"])

            print("Starting instrumented read-publish loop... (Press Ctrl+C to stop)")

            while True:
                # 1. Read MAVLink
                msg = mav.recv_match(blocking=True, timeout=1.0)
                if not msg:
                    continue

                # 2. Sequence & Drop Tracking - see main.py's identical block
                # for why this is evaluated for every message received (not
                # just tracked types), and README.md for why the resulting
                # count is currently unreliable with mavlink-replay/synth.py.
                current_seq = msg.get_header().seq
                dropped_this_cycle = 0

                if last_seq != -1:
                    dropped_this_cycle = (current_seq - last_seq - 1) % 256
                    total_dropped += dropped_this_cycle

                last_seq = current_seq

                msg_type = msg.get_type()

                if msg_type not in TRACKED_MESSAGE_TYPES:
                    continue

                # Time since the previous tracked message finished: captures
                # queueing/backlog delay from the naive blocking loop, on top
                # of the pure translation+publish cost measured below.
                t_start = time.perf_counter()
                queue_wait_ms = (t_start - t_prev_end) * 1000

                # 3. Hardcoded Translation & Blocking Publishes.
                # Scaling and target Submodel/property paths below mirror
                # config/mappings/mavlink_ops.yaml exactly, field for field
                # identical to main.py's OPC UA node writes - only the sink
                # (an MQTT publish + bounded wait, instead of an OPC UA
                # set_value) differs. Like main.py, MAVLink "unknown"
                # sentinel values (e.g. eph/hdg/satellites_visible at their
                # UINT16_MAX default) are NOT filtered out here - they are
                # scaled and published through as-is, except voltage_battery,
                # which is special-cased below exactly as in main.py.
                now = None

                def pub(submodel, path, value):
                    nonlocal now
                    if now is None:
                        now = msg._timestamp if getattr(msg, '_timestamp', None) else time.time()
                    ts = datetime.fromtimestamp(now, tz=timezone.utc)
                    info = client.publish(topic_for(submodel, path), build_payload(value, ts), qos=0)
                    info.wait_for_publish(timeout=MQTT_PUBLISH_WAIT_S)
                    if not info.is_published():
                        # QoS 0 has no broker acknowledgment, so wait_for_publish()
                        # times out silently rather than raising - without this
                        # check, a stalled publish would be logged as an ordinary,
                        # if slow, success. Raising here instead routes it through
                        # the same per-message failure handling as an outright
                        # paho exception (queue full, send error, etc.) below.
                        raise RuntimeError(f"publish not confirmed within {MQTT_PUBLISH_WAIT_S}s (mid={info.mid})")

                # Wrapped per message, not per run: without this, a single
                # publish failure - a stalled/unconfirmed QoS 0 send (see pub()
                # above), a queue-full error, or anything else paho raises -
                # would propagate out of the whole loop and end a 15-minute
                # benchmark on one bad publish. This does not make the gateway
                # any less naive: publishes remain sequential, blocking, and
                # unbatched (no coalescing, no worker pool); a mid-message
                # failure still abandons whatever fields of that message were
                # not yet published (no per-field recovery); and there is still
                # no application-level reconnection logic beyond paho's own
                # automatic TCP reconnect, so a broker outage still shows up
                # honestly as a run of consecutive failures. It only stops one
                # bad publish from discarding an otherwise-good run, and gives
                # Write_Success below a real per-message signal instead of
                # every logged row being unconditionally "successful" by
                # construction - see main.py's identical comment for the OPC
                # UA side of the same fix.
                write_success = True
                write_error = ""
                try:
                    if msg_type == 'HEARTBEAT':
                        pub('Status', 'VehicleType', int(msg.type))
                        pub('Status', 'AutopilotType', int(msg.autopilot))
                        pub('Status', 'SystemStatus', int(msg.system_status))

                    elif msg_type == 'SYS_STATUS':
                        if msg.voltage_battery != BATTERY_VOLTAGE_UNKNOWN:
                            pub('Power', 'Voltage', msg.voltage_battery / 1000.0)
                        pub('Power', 'Current', msg.current_battery * 0.01)  # cA -> A
                        pub('Power', 'Remaining', int(msg.battery_remaining))
                        pub('Power', 'CPULoad', int(msg.load * 0.1))  # d% -> %, truncated
                        pub('Power', 'DropRateComm', int(msg.drop_rate_comm * 0.01))  # c% -> %, truncated

                    elif msg_type == 'GLOBAL_POSITION_INT':
                        pub('Navigation', 'GlobalPosition/Latitude', msg.lat / 1e7)
                        pub('Navigation', 'GlobalPosition/Longitude', msg.lon / 1e7)
                        pub('Navigation', 'GlobalPosition/AltitudeMSL', msg.alt / 1000.0)
                        pub('Navigation', 'GlobalPosition/Heading', msg.hdg * 0.01)  # cdeg -> deg

                    elif msg_type == 'LOCAL_POSITION_NED':
                        pub('Navigation', 'LocalPositionNED/X', msg.x)
                        pub('Navigation', 'LocalPositionNED/Y', msg.y)
                        pub('Navigation', 'LocalPositionNED/Z', msg.z)
                        pub('Navigation', 'LocalPositionNED/VX', msg.vx)
                        pub('Navigation', 'LocalPositionNED/VY', msg.vy)
                        pub('Navigation', 'LocalPositionNED/VZ', msg.vz)

                    elif msg_type == 'GPS_RAW_INT':
                        pub('Navigation', 'GPS_Raw/FixType', int(msg.fix_type))
                        pub('Navigation', 'GPS_Raw/SatellitesVisible', int(msg.satellites_visible))
                        pub('Navigation', 'GPS_Raw/HDOP', msg.eph * 0.01)  # cm -> m

                    elif msg_type == 'ALTITUDE':
                        pub('Navigation', 'TerrainData/TerrainAltitude', msg.altitude_terrain)
                        pub('Navigation', 'TerrainData/BottomClearance', msg.bottom_clearance)

                    elif msg_type == 'ATTITUDE':
                        pub('Attitude', 'EulerAngles/Roll', msg.roll)
                        pub('Attitude', 'EulerAngles/Pitch', msg.pitch)
                        pub('Attitude', 'EulerAngles/Yaw', msg.yaw)
                        pub('Attitude', 'EulerAngles/RollSpeed', msg.rollspeed)
                        pub('Attitude', 'EulerAngles/PitchSpeed', msg.pitchspeed)
                        pub('Attitude', 'EulerAngles/YawSpeed', msg.yawspeed)

                    elif msg_type == 'ATTITUDE_QUATERNION':
                        pub('Attitude', 'Quaternion/Q1', msg.q1)
                        pub('Attitude', 'Quaternion/Q2', msg.q2)
                        pub('Attitude', 'Quaternion/Q3', msg.q3)
                        pub('Attitude', 'Quaternion/Q4', msg.q4)

                    elif msg_type == 'ATTITUDE_TARGET':
                        pub('Attitude', 'AttitudeTarget/BodyRollRate', msg.body_roll_rate)
                        pub('Attitude', 'AttitudeTarget/BodyPitchRate', msg.body_pitch_rate)
                        pub('Attitude', 'AttitudeTarget/BodyYawRate', msg.body_yaw_rate)
                        pub('Attitude', 'AttitudeTarget/Thrust', msg.thrust)

                    elif msg_type == 'SERVO_OUTPUT_RAW':
                        pub('Actuation', 'ServoOutput/Servo1', int(msg.servo1_raw))
                        pub('Actuation', 'ServoOutput/Servo2', int(msg.servo2_raw))
                        pub('Actuation', 'ServoOutput/Servo3', int(msg.servo3_raw))
                        pub('Actuation', 'ServoOutput/Servo4', int(msg.servo4_raw))

                    elif msg_type == 'VFR_HUD':
                        pub('HUD', 'Airspeed', msg.airspeed)
                        pub('HUD', 'Groundspeed', msg.groundspeed)
                        pub('HUD', 'Heading', int(msg.heading))
                        pub('HUD', 'Throttle', int(msg.throttle))
                        pub('HUD', 'Altitude', msg.alt)
                        pub('HUD', 'ClimbRate', msg.climb)

                    elif msg_type == 'OPEN_DRONE_ID_SYSTEM':
                        pub('Identification', 'RemoteID_System/OperatorLocationType', int(msg.operator_location_type))
                        pub('Identification', 'RemoteID_System/ClassificationType', int(msg.classification_type))
                except Exception as e:
                    write_success = False
                    write_error = str(e)[:200]
                    print(f"Publish failed for {msg_type} (seq={current_seq}): {write_error}")

                # End latency timer
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
                if total_messages_processed % 50 == 0:
                    if expected_counts:
                        expected_total = sum(expected_counts.values())
                        delivery_pct = (total_write_success / expected_total * 100) if expected_total else 0.0
                        print(f"Processed: {total_messages_processed} (success: {total_write_success}, publish failures: {total_write_failures}) | "
                              f"Delivery: {total_write_success}/{expected_total} ({delivery_pct:.1f}%) | Last Latency: {latency_ms:.1f}ms")
                    else:
                        print(f"Processed: {total_messages_processed} (success: {total_write_success}, publish failures: {total_write_failures}) | "
                              f"Total Dropped (raw wire-seq, unreliable - see README): {total_dropped} | Last Latency: {latency_ms:.1f}ms")

    except KeyboardInterrupt:
        print("\nBenchmark stopped by user.")
    except Exception as e:
        print(f"\nGateway crashed: {e}")
    finally:
        write_success_pct = (total_write_success / total_messages_processed * 100) if total_messages_processed else 0.0
        print(f"Final Stats: {total_messages_processed} processed "
              f"({total_write_success} publish-confirmed [{write_success_pct:.2f}%], {total_write_failures} publish failures), "
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

        print("Disconnecting MQTT...")
        client.disconnect()
        client.loop_stop()


if __name__ == "__main__":
    main()
