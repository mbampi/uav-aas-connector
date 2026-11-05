"""Batched, pipelined MAVLink-to-BaSyx bridge.

Not the naive baseline (see bridge.py). Instead of one blocking PATCH per
property, this keeps an in-memory mirror of every submodel's values, updates
it as MAVLink messages arrive (no I/O), and flushes each changed submodel as
one PATCH, with different submodels flushed concurrently.

Submodel-level PATCH .../$value, tested against BaSyx 2.0.0-milestone-15:

    body: {"GlobalPosition": {"Latitude": "1.0", ...}}  -> 400 Bad Request
    body: array of full SubmodelElement definitions     -> 204 No Content

The array replaces the submodel's element list: sending only "GlobalPosition"
deletes LocalPositionNED, GPS_Raw and TerrainData. So every flush has to send
the complete submodel, which is why SubmodelStateStore keeps a full mirror
instead of a queue of recent changes.

Each submodel has its own worker thread (run_submodel_flush_worker). It blocks
until the submodel has pending changes, flushes, and checks again as soon as
the flush returns. Up to 7 PATCHes can be in flight, hence the connection pool
sized to the number of submodels. There is no flush timer, so the pacing comes
from BaSyx's response time, like WaitDrain in pkg/exporters/coalesce.

Staging a value and flushing a submodel happen on different cadences, so they
are logged to two CSVs instead of bridge.py's one row per message. See
../README.md for the columns.
"""

import base64
import csv
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import psutil
import requests
from pymavlink import mavutil

# ==========================================
# CONFIGURATION - same env-var pattern as bridge.py
# ==========================================
MAVLINK_CONNECTION_STRING = os.environ.get('MAVLINK_CONNECTION_STRING', 'udp:0.0.0.0:14550')
BASYX_BASE_URL = os.environ.get('BASYX_BASE_URL', 'http://127.0.0.1:8081').rstrip('/')
INGESTION_CSV_FILENAME = os.environ.get('INGESTION_CSV_FILENAME', 'basyx_batched_ingestion_metrics.csv')
FLUSH_CSV_FILENAME = os.environ.get('FLUSH_CSV_FILENAME', 'basyx_batched_flush_metrics.csv')
HEARTBEAT_TIMEOUT_S = 10
BASYX_READY_TIMEOUT_S = 60
REQUEST_TIMEOUT_S = 5.0

# How often the metrics sampler thread refreshes the shared CPU%/memory
# snapshot flush workers attach to their CSV rows - NOT a flush pacing
# knob (there is none - see module docstring). Purely about not calling
# psutil from 7 threads concurrently (its cpu_percent() tracks a
# last-call delta internally per Process object and isn't meant to be
# read from multiple threads at once) and not over-sampling it either.
METRICS_SAMPLE_INTERVAL_S = 0.1

SOURCE_TLOG_PATH = os.environ.get('SOURCE_TLOG_PATH')

BATTERY_VOLTAGE_UNKNOWN = 65535

# Identical to bridge.py's list and identically justified - see that file.
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
    return base64.urlsafe_b64encode(raw.encode('utf-8')).rstrip(b'=').decode('ascii')


SUBMODEL_IDS_B64 = {k: b64url(v) for k, v in SUBMODEL_IDS.items()}

# Same file BaSyx loads at startup, so the mirror matches its schema. An incomplete
# schema would delete elements on the first flush. In Docker it's mounted as a
# volume, since the build context is just ./bridge.
AAS_ENVIRONMENT_PATH = os.environ.get(
    'AAS_ENVIRONMENT_PATH',
    os.path.join(os.path.dirname(__file__), '..', 'aas_definitions', 'uav_environment.json'),
)


def _walk_elements(elements):
    """AAS-JSON submodelElements list -> {idShort: element-state-dict}, recursively."""
    out = {}
    for el in elements:
        if el['modelType'] == 'Property':
            out[el['idShort']] = {'modelType': 'Property', 'valueType': el['valueType'], 'value': el['value']}
        elif el['modelType'] == 'SubmodelElementCollection':
            out[el['idShort']] = {'modelType': 'SubmodelElementCollection', 'value': _walk_elements(el['value'])}
        # No other modelType appears in config/aas/quad_op.json's submodels - see that
        # file / aas_definitions/generate_environment.py.
    return out


def _serialize_elements(elements_state):
    """Inverse of _walk_elements: {idShort: element-state-dict} -> AAS-JSON submodelElements list."""
    out = []
    for idshort, el in elements_state.items():
        if el['modelType'] == 'Property':
            out.append({'idShort': idshort, 'modelType': 'Property', 'valueType': el['valueType'], 'value': el['value']})
        elif el['modelType'] == 'SubmodelElementCollection':
            out.append({'idShort': idshort, 'modelType': 'SubmodelElementCollection', 'value': _serialize_elements(el['value'])})
    return out


def load_initial_state(environment_path):
    with open(environment_path) as f:
        env = json.load(f)
    state = {}
    for sm in env['submodels']:
        state[sm['idShort']] = _walk_elements(sm['submodelElements'])
    return state


class SubmodelStateStore:
    """In-memory mirror of every submodel's current property values, with
    reactive per-submodel signaling - no polling, no fixed interval (see
    module docstring for why a timer-based design was rejected).

    Required by BaSyx's full-replace PATCH .../$value semantics: every
    flush must serialize the COMPLETE current state of a submodel, not just
    what changed since the last flush, or the omitted properties are
    deleted server-side. One lock guards the value mirror and the
    dirty/timing bookkeeping so a flush always sees a consistent snapshot
    relative to the ingestion thread's updates; one threading.Event per
    submodel lets that submodel's dedicated worker block until there's
    actually something to do, instead of waking on a clock to check.
    """

    def __init__(self, initial_state):
        self._state = initial_state
        self._dirty = {k: False for k in initial_state}
        self._dirty_since = {k: None for k in initial_state}
        self._events = {k: threading.Event() for k in initial_state}
        self._lock = threading.Lock()

    def stage(self, submodel_key, id_short_path, value):
        """Update one property's value in the mirror and wake that
        submodel's flush worker if it wasn't already dirty. No I/O - just a
        dict write plus (at most) an Event.set()."""
        parts = id_short_path.split('.')
        with self._lock:
            node = self._state[submodel_key]
            for p in parts[:-1]:
                node = node[p]['value']
            node[parts[-1]]['value'] = str(value)
            if not self._dirty[submodel_key]:
                self._dirty[submodel_key] = True
                self._dirty_since[submodel_key] = time.perf_counter()
                self._events[submodel_key].set()

    def wait_and_take(self, submodel_key, timeout):
        """Block until this submodel is dirty (or timeout, so the caller
        can periodically check for shutdown), then atomically take and
        clear its full current element list. Returns (elements_list,
        dirty_since_perf_counter) or None if the wait timed out with
        nothing pending."""
        if not self._events[submodel_key].wait(timeout=timeout):
            return None
        with self._lock:
            if not self._dirty[submodel_key]:
                # Woken but another thread already consumed it - shouldn't
                # happen with one worker per submodel, but stay correct if
                # that ever changes.
                self._events[submodel_key].clear()
                return None
            elements = _serialize_elements(self._state[submodel_key])
            dirty_since = self._dirty_since[submodel_key]
            self._dirty[submodel_key] = False
            self._dirty_since[submodel_key] = None
            self._events[submodel_key].clear()
            return elements, dirty_since

    def mark_dirty_again(self, submodel_key):
        """Re-flag a submodel dirty after a failed flush, so its worker
        retries immediately with the current state.
        """
        with self._lock:
            if not self._dirty[submodel_key]:
                self._dirty[submodel_key] = True
                self._dirty_since[submodel_key] = time.perf_counter()
            self._events[submodel_key].set()

    def snapshot_all(self):
        """Every submodel's current full element list, dirty or not - used
        for the final flush on shutdown so nothing buffered is lost."""
        with self._lock:
            return {k: _serialize_elements(v) for k, v in self._state.items()}


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


def flush_submodel(session, base_url, submodel_key, elements):
    """One whole-submodel PATCH .../$value - the batching unit. elements
    must be the submodel's COMPLETE current element list (see module
    docstring: this is a full replace, not a merge).

    Returns the round-trip latency in ms. It is timed here, in the worker
    thread, because timing Future.result() from the caller only measures
    retrieving a value that is already computed.
    """
    url = f"{base_url}/submodels/{SUBMODEL_IDS_B64[submodel_key]}/$value"
    t_start = time.perf_counter()
    resp = session.patch(url, json=elements, timeout=REQUEST_TIMEOUT_S)
    latency_ms = (time.perf_counter() - t_start) * 1000
    if not resp.ok:
        raise requests.exceptions.HTTPError(
            f"{resp.status_code} {resp.reason} for url: {resp.url} - body: {resp.text[:500]!r}",
            response=resp,
        )
    return latency_ms


class MetricsSampler:
    """Background thread sampling process CPU%/memory at a fixed low-rate
    interval, shared (read-only) by every submodel flush worker.

    Exists only so 7 concurrent flush workers don't each call
    psutil.Process.cpu_percent() directly - that method tracks a
    last-call-time delta internally per Process object, and concurrent
    calls from multiple threads would race on that internal state. This is
    plumbing for the CPU_Percent/Memory_MB columns (an explicitly secondary,
    in-process signal - see ../README.md - the docker-stats sidecar
    is the number that matters for cross-system comparison), not a pacing
    mechanism - it has nothing to do with when flushes happen.
    """

    def __init__(self, process, interval_s):
        self._process = process
        self._interval_s = interval_s
        self._cpu_percent = 0.0
        self._mem_mb = 0.0
        self._lock = threading.Lock()

    def run(self, stop_event):
        while not stop_event.is_set():
            cpu = self._process.cpu_percent()
            mem = self._process.memory_info().rss / (1024 * 1024)
            with self._lock:
                self._cpu_percent = cpu
                self._mem_mb = mem
            stop_event.wait(self._interval_s)

    def get(self):
        with self._lock:
            return self._cpu_percent, self._mem_mb


def run_submodel_flush_worker(submodel_key, session, store, flush_writer, flush_writer_lock,
                               metrics_sampler, stop_event, stats):
    """One dedicated thread per submodel. Blocks until this submodel has
    pending changes, flushes them IMMEDIATELY, then checks again without
    waiting - if more changes arrived while flushing, it flushes again
    right away; if not, it blocks again. No fixed interval anywhere (see
    module docstring for why) - this submodel's own flush cadence is
    entirely a function of how fast MAVLink produces changes for it and how
    fast BaSyx responds, nothing else.
    """
    while not stop_event.is_set():
        result = store.wait_and_take(submodel_key, timeout=1.0)  # timeout only so shutdown is checked periodically
        if result is None:
            continue
        elements, dirty_since = result

        props_count = sum(1 for _ in _iter_leaf_count(elements))
        cpu_percent, mem_mb = metrics_sampler.get()
        write_success = True
        write_error = ''
        latency_ms = None
        try:
            latency_ms = flush_submodel(session, BASYX_BASE_URL, submodel_key, elements)
            coalescing_lag_ms = (time.perf_counter() - dirty_since) * 1000
            stats['flush_success'] += 1
        except requests.exceptions.RequestException as e:
            coalescing_lag_ms = (time.perf_counter() - dirty_since) * 1000
            write_success = False
            write_error = str(e)[:600]
            store.mark_dirty_again(submodel_key)
            stats['flush_failure'] += 1
            print(f"Flush failed for {submodel_key}: {write_error}")

        with flush_writer_lock:
            flush_writer.writerow([
                time.time(), submodel_key, props_count,
                round(latency_ms, 3) if latency_ms is not None else '',
                round(coalescing_lag_ms, 3),
                round(cpu_percent, 2), round(mem_mb, 2),
                write_success, write_error,
            ])


def _iter_leaf_count(elements_list):
    for el in elements_list:
        if el['modelType'] == 'Property':
            yield 1
        elif el['modelType'] == 'SubmodelElementCollection':
            for _ in _iter_leaf_count(el['value']):
                yield 1


def main():
    total_messages_processed = 0
    total_dropped = 0
    arrived_counts = {t: 0 for t in TRACKED_MESSAGE_TYPES}
    expected_counts = {}
    stats = {'flush_success': 0, 'flush_failure': 0}

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

    session = requests.Session()
    # Unlike bridge.py, concurrency here is real (up to 7 submodels flushed
    # at once) - pool_maxsize actually needs to cover that, not just default
    # to Session()'s own pool.
    adapter = requests.adapters.HTTPAdapter(pool_connections=len(SUBMODEL_IDS), pool_maxsize=len(SUBMODEL_IDS))
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    print(f"Waiting for BaSyx AAS Environment at {BASYX_BASE_URL}...")
    if not wait_for_basyx(session, BASYX_BASE_URL, BASYX_READY_TIMEOUT_S):
        print(f"BaSyx not reachable at {BASYX_BASE_URL} within {BASYX_READY_TIMEOUT_S}s, aborting.")
        return
    print("BaSyx connected!")

    print(f"Loading initial submodel state mirror from {AAS_ENVIRONMENT_PATH}...")
    store = SubmodelStateStore(load_initial_state(AAS_ENVIRONMENT_PATH))

    process = psutil.Process(os.getpid())
    process.cpu_percent()

    ingestion_file = open(INGESTION_CSV_FILENAME, mode='w', newline='', buffering=1)
    ingestion_writer = csv.writer(ingestion_file)
    ingestion_writer.writerow(["System_Time", "Message_Type", "MAVLink_Seq", "Dropped_Since_Last",
                                "Submodel", "Properties_Staged", "Stage_Latency_ms"])

    flush_file = open(FLUSH_CSV_FILENAME, mode='w', newline='', buffering=1)
    flush_writer = csv.writer(flush_file)
    flush_writer.writerow(["System_Time", "Submodel", "Properties_In_Payload", "Flush_Latency_ms",
                            "Coalescing_Lag_ms", "CPU_Percent", "Memory_MB", "Write_Success", "Error"])
    flush_writer_lock = threading.Lock()

    stop_event = threading.Event()

    metrics_sampler = MetricsSampler(process, METRICS_SAMPLE_INTERVAL_S)
    sampler_thread = threading.Thread(target=metrics_sampler.run, args=(stop_event,), daemon=True)
    sampler_thread.start()

    # One dedicated worker per submodel - see run_submodel_flush_worker's
    # docstring and the module docstring's "no flush-interval knob" section
    # for why this is 7 independent reactive workers, not one loop on a timer.
    flush_threads = []
    for submodel_key in SUBMODEL_IDS:
        t = threading.Thread(
            target=run_submodel_flush_worker,
            args=(submodel_key, session, store, flush_writer, flush_writer_lock,
                  metrics_sampler, stop_event, stats),
            daemon=True,
            name=f"flush-{submodel_key}",
        )
        t.start()
        flush_threads.append(t)

    last_seq = -1

    print(f"Starting instrumented ingest loop (non-blocking stage, {len(flush_threads)} reactive "
          f"per-submodel flush workers, no fixed interval)... (Press Ctrl+C to stop)")

    try:
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
            submodel_key = None
            properties_staged = 0

            # Same translation math as bridge.py/main.py, cA->A, cdeg->deg,
            # voltage_battery sentinel, etc. - see mavlink_ops.yaml and
            # bridge.py's own comment for the verified 1:1 correspondence.
            # Every message maps to exactly one submodel (see
            # config/mappings/mavlink_ops.yaml: no sourceMessage appears
            # under two different targetAASSubmodel values), so staging is
            # always against a single store entry per message.
            if msg_type == 'HEARTBEAT':
                submodel_key = 'Status'
                store.stage('Status', 'VehicleType', int(msg.type)); properties_staged += 1
                store.stage('Status', 'AutopilotType', int(msg.autopilot)); properties_staged += 1
                store.stage('Status', 'SystemStatus', int(msg.system_status)); properties_staged += 1

            elif msg_type == 'SYS_STATUS':
                submodel_key = 'Power'
                if msg.voltage_battery != BATTERY_VOLTAGE_UNKNOWN:
                    store.stage('Power', 'Voltage', msg.voltage_battery / 1000.0); properties_staged += 1
                store.stage('Power', 'Current', msg.current_battery * 0.01); properties_staged += 1
                store.stage('Power', 'Remaining', int(msg.battery_remaining)); properties_staged += 1
                store.stage('Power', 'CPULoad', int(msg.load * 0.1)); properties_staged += 1
                store.stage('Power', 'DropRateComm', int(msg.drop_rate_comm * 0.01)); properties_staged += 1

            elif msg_type == 'GLOBAL_POSITION_INT':
                submodel_key = 'Navigation'
                store.stage('Navigation', 'GlobalPosition.Latitude', msg.lat / 1e7); properties_staged += 1
                store.stage('Navigation', 'GlobalPosition.Longitude', msg.lon / 1e7); properties_staged += 1
                store.stage('Navigation', 'GlobalPosition.AltitudeMSL', msg.alt / 1000.0); properties_staged += 1
                store.stage('Navigation', 'GlobalPosition.Heading', msg.hdg * 0.01); properties_staged += 1

            elif msg_type == 'LOCAL_POSITION_NED':
                submodel_key = 'Navigation'
                store.stage('Navigation', 'LocalPositionNED.X', msg.x); properties_staged += 1
                store.stage('Navigation', 'LocalPositionNED.Y', msg.y); properties_staged += 1
                store.stage('Navigation', 'LocalPositionNED.Z', msg.z); properties_staged += 1
                store.stage('Navigation', 'LocalPositionNED.VX', msg.vx); properties_staged += 1
                store.stage('Navigation', 'LocalPositionNED.VY', msg.vy); properties_staged += 1
                store.stage('Navigation', 'LocalPositionNED.VZ', msg.vz); properties_staged += 1

            elif msg_type == 'GPS_RAW_INT':
                submodel_key = 'Navigation'
                store.stage('Navigation', 'GPS_Raw.FixType', int(msg.fix_type)); properties_staged += 1
                store.stage('Navigation', 'GPS_Raw.SatellitesVisible', int(msg.satellites_visible)); properties_staged += 1
                store.stage('Navigation', 'GPS_Raw.HDOP', msg.eph * 0.01); properties_staged += 1

            elif msg_type == 'ALTITUDE':
                submodel_key = 'Navigation'
                store.stage('Navigation', 'TerrainData.TerrainAltitude', msg.altitude_terrain); properties_staged += 1
                store.stage('Navigation', 'TerrainData.BottomClearance', msg.bottom_clearance); properties_staged += 1

            elif msg_type == 'ATTITUDE':
                submodel_key = 'Attitude'
                store.stage('Attitude', 'EulerAngles.Roll', msg.roll); properties_staged += 1
                store.stage('Attitude', 'EulerAngles.Pitch', msg.pitch); properties_staged += 1
                store.stage('Attitude', 'EulerAngles.Yaw', msg.yaw); properties_staged += 1
                store.stage('Attitude', 'EulerAngles.RollSpeed', msg.rollspeed); properties_staged += 1
                store.stage('Attitude', 'EulerAngles.PitchSpeed', msg.pitchspeed); properties_staged += 1
                store.stage('Attitude', 'EulerAngles.YawSpeed', msg.yawspeed); properties_staged += 1

            elif msg_type == 'ATTITUDE_QUATERNION':
                submodel_key = 'Attitude'
                store.stage('Attitude', 'Quaternion.Q1', msg.q1); properties_staged += 1
                store.stage('Attitude', 'Quaternion.Q2', msg.q2); properties_staged += 1
                store.stage('Attitude', 'Quaternion.Q3', msg.q3); properties_staged += 1
                store.stage('Attitude', 'Quaternion.Q4', msg.q4); properties_staged += 1

            elif msg_type == 'ATTITUDE_TARGET':
                submodel_key = 'Attitude'
                store.stage('Attitude', 'AttitudeTarget.BodyRollRate', msg.body_roll_rate); properties_staged += 1
                store.stage('Attitude', 'AttitudeTarget.BodyPitchRate', msg.body_pitch_rate); properties_staged += 1
                store.stage('Attitude', 'AttitudeTarget.BodyYawRate', msg.body_yaw_rate); properties_staged += 1
                store.stage('Attitude', 'AttitudeTarget.Thrust', msg.thrust); properties_staged += 1

            elif msg_type == 'SERVO_OUTPUT_RAW':
                submodel_key = 'Actuation'
                store.stage('Actuation', 'ServoOutput.Servo1', int(msg.servo1_raw)); properties_staged += 1
                store.stage('Actuation', 'ServoOutput.Servo2', int(msg.servo2_raw)); properties_staged += 1
                store.stage('Actuation', 'ServoOutput.Servo3', int(msg.servo3_raw)); properties_staged += 1
                store.stage('Actuation', 'ServoOutput.Servo4', int(msg.servo4_raw)); properties_staged += 1

            elif msg_type == 'VFR_HUD':
                submodel_key = 'HUD'
                store.stage('HUD', 'Airspeed', msg.airspeed); properties_staged += 1
                store.stage('HUD', 'Groundspeed', msg.groundspeed); properties_staged += 1
                store.stage('HUD', 'Heading', int(msg.heading)); properties_staged += 1
                store.stage('HUD', 'Throttle', int(msg.throttle)); properties_staged += 1
                store.stage('HUD', 'Altitude', msg.alt); properties_staged += 1
                store.stage('HUD', 'ClimbRate', msg.climb); properties_staged += 1

            elif msg_type == 'OPEN_DRONE_ID_SYSTEM':
                submodel_key = 'Identification'
                store.stage('Identification', 'RemoteID_System.OperatorLocationType', int(msg.operator_location_type)); properties_staged += 1
                store.stage('Identification', 'RemoteID_System.ClassificationType', int(msg.classification_type)); properties_staged += 1
                # UaType intentionally unmapped - see bridge.py's SUBMODEL_IDS comment.

            stage_latency_ms = (time.perf_counter() - t_start) * 1000

            total_messages_processed += 1
            arrived_counts[msg_type] += 1

            ingestion_writer.writerow([
                time.time(), msg_type, current_seq, dropped_this_cycle,
                submodel_key or '', properties_staged, round(stage_latency_ms, 4),
            ])

            if total_messages_processed % 200 == 0:
                print(f"Ingested: {total_messages_processed} messages | "
                      f"Flushes: {stats['flush_success']} ok / {stats['flush_failure']} failed | "
                      f"Last stage latency: {stage_latency_ms:.4f}ms")

    except KeyboardInterrupt:
        print("\nBenchmark stopped by user.")
    finally:
        print(f"Stopping {len(flush_threads)} flush workers and the metrics sampler, then performing "
              f"a final full flush of every submodel...")
        stop_event.set()
        for t in flush_threads:
            t.join(timeout=5)
        sampler_thread.join(timeout=5)

        # Final flush: send every submodel's current state once more,
        # dirty or not, so nothing staged-but-not-yet-flushed is lost when
        # the process exits - each worker only flushes when signaled, so
        # whatever was staged in the brief window between its last flush
        # and this shutdown hasn't gone out yet.
        final_state = store.snapshot_all()
        with ThreadPoolExecutor(max_workers=len(SUBMODEL_IDS)) as executor:
            futures = {executor.submit(flush_submodel, session, BASYX_BASE_URL, k, els): k for k, els in final_state.items()}
            for fut in as_completed(futures):
                k = futures[fut]
                try:
                    fut.result()
                    print(f"Final flush of {k}: ok")
                except requests.exceptions.RequestException as e:
                    print(f"Final flush of {k} FAILED: {str(e)[:300]}")

        print(f"Final Stats: {total_messages_processed} messages ingested, "
              f"{stats['flush_success']} submodel flushes ok, {stats['flush_failure']} failed, "
              f"{total_dropped} dropped (raw wire-seq count - unreliable with the current replay/synth "
              f"tooling, see python-baseline-conn/README.md).")

        summary = {}
        if expected_counts:
            print("Per-type ingestion ratio (arrived / expected from source .tlog, tracked types only):")
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

        summary["_flush_completeness"] = {
            "flush_success": stats['flush_success'],
            "flush_failure": stats['flush_failure'],
            "flush_success_pct": round(
                stats['flush_success'] / (stats['flush_success'] + stats['flush_failure']) * 100, 2
            ) if (stats['flush_success'] + stats['flush_failure']) else 0.0,
        }
        summary_path = os.path.splitext(FLUSH_CSV_FILENAME)[0] + "_delivery_summary.json"
        with open(summary_path, "w") as summary_file:
            json.dump(summary, summary_file, indent=2)
        print(f"Delivery summary written to {summary_path}")

        ingestion_file.close()
        flush_file.close()


if __name__ == "__main__":
    main()
