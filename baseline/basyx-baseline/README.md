# Eclipse BaSyx Baseline

Off-the-shelf Eclipse BaSyx V2 AAS Environment, fed the same MAVLink telemetry
as the connector through a small REST bridge (BaSyx has no MAVLink ingestion).
Methodology, asymmetries and run validity are in `../README.md`.

- BaSyx runs with defaults only: in-memory backend, no MongoDB, no registries,
  no auth, default JVM flags.
- `bridge/bridge.py` - naive bridge. Single-threaded, one blocking PATCH per
  property, no retry.
- `bridge/bridge_batched.py` - batched/pipelined bridge. One PATCH per changed
  submodel, submodels flushed concurrently. Measures what BaSyx's API allows
  when used efficiently; it is not a naive baseline.

Run one bridge at a time: both use container name `basyx-bridge` and UDP
port 14550.

## Hardware

Same topology as the connector experiments:

```
MacBook Pro M1                          Raspberry Pi 5 (4GB)
- MAVLink replay (replay.py)   <--LAN-->  - BaSyx AAS Environment (Docker)
- MQTT broker (Mosquitto)                 - MAVLink-to-BaSyx bridge (Docker)
- basyx_mqtt_receiver.py                  - 2x Docker-stats sidecars
```

## Prerequisites

- Docker Engine + Compose plugin on both machines.
- On the Mac: `pip install paho-mqtt`, plus `uav-aas-tools/mavlink-replay`.
- This directory copied to the Pi.

## Phase 1: Deploy on the Pi

1. Set `mqtt.hostname` in `application.properties` to the Mac's LAN IP.
2. The AAS environment (`aas_definitions/uav_environment.json`) is loaded at
   startup. It is generated from `../../config/aas/quad_op.json`; regenerate
   after changing that file:

   ```bash
   python3 aas_definitions/generate_environment.py
   ```

3. Start BaSyx, one bridge and both resource collectors:

   ```bash
   # naive bridge
   docker compose up -d --build basyx-aas-env basyx-bridge \
       metrics-collector-basyx metrics-collector-bridge

   # batched bridge
   docker compose up -d --build basyx-aas-env basyx-bridge-batched \
       metrics-collector-basyx metrics-collector-bridge
   ```

   The bridge waits up to 60 s (`BASYX_READY_TIMEOUT_S`) for BaSyx, so both
   can start together.

4. Check it's up:

   ```bash
   docker compose logs -f basyx-aas-env basyx-bridge
   curl http://<pi-ip>:8081/submodels   # should list 7 submodels
   ```

## Phase 2: Naive bridge (`bridge/bridge.py`)

Listens for MAVLink on UDP, applies the same scaling as
`../python-baseline-conn/main.py`, and PATCHes each property:

```
PATCH http://<basyx>:8081/submodels/<base64url(id)>/submodel-elements/<idShortPath>/$value
body: "82.487"   (value as a JSON string)
```

See `../README.md` ("BaSyx API notes") for the request format details.

- Uses one `requests.Session()` (needed at ~700 req/s, see `../README.md`).
- Per-message try/except, no retry or reconnection.
- `SOURCE_TLOG_PATH`: same as in `python-baseline-conn`; gives a per-type
  delivery ratio in `<CSV base>_delivery_summary.json`.

Check the mapping against `mavlink_ops.yaml` after changing either side:

```bash
python3 verify_mapping.py
```

Output CSV: `basyx_baseline_bridge_metrics.csv`, same columns as
`python-baseline-conn` (`System_Time, Message_Type, MAVLink_Seq,
Dropped_Since_Last, Queue_Wait_ms, Processing_Latency_ms, CPU_Percent,
Memory_MB, Write_Success, Error`), so the same analyzer works:

```bash
python3 ../../analysis/baseline_latency_analyzer.py metrics/basyx_baseline_bridge_metrics.csv
```

`Processing_Latency_ms` is a full HTTP round trip per property. The delivery
summary also has `_http_completeness` (`puts_sent`, `puts_ok`,
`completeness_pct`).

## Phase 2b: Batched bridge (`bridge/bridge_batched.py`)

BaSyx's submodel-level `PATCH .../$value` replaces the whole element list, so
the bridge keeps a full in-memory copy of every submodel
(`SubmodelStateStore`, seeded from `uav_environment.json`) and always sends
the complete submodel.

- Ingestion only updates the in-memory copy (no I/O).
- Each submodel has its own worker thread that flushes as soon as there are
  pending changes, with no timer. Up to 7 PATCHes in flight at once.
- A failed flush leaves the submodel dirty, so the next flush retries with
  the current state.
- Connection pool sized to the number of submodels.

Output CSVs:

- `basyx_batched_ingestion_metrics.csv`, one row per MAVLink message:
  `System_Time, Message_Type, MAVLink_Seq, Dropped_Since_Last, Submodel,
  Properties_Staged, Stage_Latency_ms`
- `basyx_batched_flush_metrics.csv`, one row per flush:
  `System_Time, Submodel, Properties_In_Payload, Flush_Latency_ms,
  Coalescing_Lag_ms, CPU_Percent, Memory_MB, Write_Success, Error`.
  `Coalescing_Lag_ms` is how long the submodel waited dirty before the flush.

The delivery summary has `_flush_completeness` (`flush_success`,
`flush_failure`, `flush_success_pct`). `verify_mapping.py` checks this bridge
too.

## Phase 3: Mac side

1. MQTT broker, same config as the connector's MQTT runs:

   ```bash
   cd ../../../uav-aas-tools/mqtt_receiver
   mosquitto -c mosquitto.conf
   # or: docker run -d --name mosquitto -p 1883:1883 eclipse-mosquitto:2
   ```

2. Receiver. Use `basyx_mqtt_receiver.py`, not `mqtt_receiver.py` (BaSyx uses
   a different topic scheme). It writes the same CSV schema
   (`Arrival_Timestamp_Ms, Node_ID, Value_Raw`) so `latency-calculate.py`
   works unchanged:

   ```bash
   python3 basyx_mqtt_receiver.py --host localhost --port 1883 --log basyx_mqtt_received_log.csv
   ```

3. Replay:

   ```bash
   cd ../../../uav-aas-tools/mavlink-replay
   python3 replay.py ./data/synthetic/synth_50hz_15min.tlog --ip <pi-ip> --port 14550 --log dispatch_log.csv
   ```

## Phase 4: Test protocol

10 Hz, 50 Hz and 200 Hz, 900 s each, traces from `../synthetic/`. Run every
rate once with each bridge.

Before starting: time sync (chrony/ntp) between Mac and Pi, since dispatch
and arrival timestamps come from different hosts here. Restart
`basyx-aas-env` after editing `application.properties`.

Per rate and bridge:

1. Mac: start the broker, then `basyx_mqtt_receiver.py`.
2. Pi: `docker compose up` as in Phase 1.
3. Check the bridge log (`MAVLink connected!` / `BaSyx connected!` for the
   naive bridge).
4. Mac: start `replay.py` with the trace for this rate.
5. After 900 s stop `replay.py` and the receiver, then `docker compose down`
   on the Pi (restarting BaSyx clears the in-memory state between passes).
6. Collect `./metrics/*.csv` from the Pi and the receiver/dispatch logs from
   the Mac, and move them out of `./metrics/` before the next pass.

## Phase 5: Metrics

| Metric | How | Tooling |
| --- | --- | --- |
| End-to-end latency | MQTT arrival on Mac minus UDP dispatch from Mac, matched by sequence counter | `latency-calculate.py dispatch_log.csv basyx_mqtt_received_log.csv --seq-node-suffix LocalPositionNED.X` (dot, not slash) |
| Ingestion completeness | 2xx responses vs PATCHes sent | `_http_completeness` / `_flush_completeness` in the delivery summary |
| Dropped traffic | Expected per-type counts (`SOURCE_TLOG_PATH`) vs received | bridge delivery summary, same as `python-baseline-conn` |
| Resources | Mean/p99 CPU and memory, per component | `metrics/basyx-aas-env_<ts>.csv` and `metrics/basyx-bridge_<ts>.csv` |

For the batched bridge, also report:

- ingestion completeness (should be close to 100% at every rate, since
  staging doesn't block)
- p50/p99/max of `Flush_Latency_ms` (grows with `Properties_In_Payload`)
- `Coalescing_Lag_ms` over the whole 900 s. Flat lag means BaSyx keeps up;
  lag that keeps growing means BaSyx itself is the limit at that rate.

## Local smoke test

```bash
# BaSyx
docker compose up -d --build basyx-aas-env

# naive bridge against loopback
cd bridge && pip install -r requirements.txt
MAVLINK_CONNECTION_STRING=udp:0.0.0.0:14550 BASYX_BASE_URL=http://127.0.0.1:8081 \
    CSV_FILENAME=/tmp/basyx_bridge_smoketest.csv python3 bridge.py

# or the batched bridge
MAVLINK_CONNECTION_STRING=udp:0.0.0.0:14550 BASYX_BASE_URL=http://127.0.0.1:8081 \
    INGESTION_CSV_FILENAME=/tmp/basyx_batched_ingestion_smoketest.csv \
    FLUSH_CSV_FILENAME=/tmp/basyx_batched_flush_smoketest.csv \
    python3 bridge_batched.py

# short replay
cd ../../../uav-aas-tools/mavlink-replay
python3 replay.py ./data/synthetic/synth_10hz_15min.tlog --ip 127.0.0.1 --port 14550 --log /tmp/dispatch_smoketest.csv

# check values landed (drop the /submodel-elements/... part for the batched bridge)
SM=$(python3 -c "import base64;print(base64.urlsafe_b64encode(b'urn:submodel:navigation:001').rstrip(b'=').decode())")
curl "http://127.0.0.1:8081/submodels/$SM/submodel-elements/LocalPositionNED.X/\$value"
```

## Things to watch for

Naive bridge:

- JVM GC pauses showing up as p99 latency spikes.
- Whether frequent `Attitude` updates slow down `Navigation`/`Power` writes.
- A "Connection pool is full" warning would mean something broke the
  sequential loop; it should never appear.
- UDP drops on the Pi when the blocking PATCHes can't keep up at 200 Hz.
  This is an expected result for a naive bridge, not a bug.

Batched bridge:

- JVM overhead shows up as growing `Coalescing_Lag_ms` instead of
  per-property spikes.
- If BaSyx serializes writes internally, flush latency should rise with the
  number of submodels being flushed at the same moment.
