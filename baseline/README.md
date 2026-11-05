# Baselines

Comparison points for the Go `uav-aas-connector` in the benchmarks:

- `python-baseline-conn/` - naive Python gateways: `main.py` (MAVLink to OPC UA)
  and `main_mqtt.py` (MAVLink to MQTT). Usage in `python-baseline-conn/README.md`.
- `basyx-baseline/` - off-the-shelf Eclipse BaSyx V2 AAS Environment fed by a
  MAVLink-to-REST bridge. Two bridges: `bridge.py` (naive) and
  `bridge_batched.py` (batched/pipelined). Usage in `basyx-baseline/README.md`.

This file covers the methodology: what "naive" means here, why the comparison
is fair, and what has to be disclosed when reporting results.

## What "naive" means

The baselines are slower because of structural choices, not injected delays or
deliberately bad code:

- single-threaded, sequential translation
- one blocking write/publish/PATCH per property, before the next message
- no batching, no coalescing, no worker pool, no reconnection logic

The connector, by contrast (`pkg/core/core.go`, `pkg/exporters/coalesce`):

- broadcasts non-blockingly to per-exporter channels, so a slow exporter can't
  stall ingestion
- coalesces to latest-value-per-property under load (bounded memory, no
  convergence on stale data)
- avoids reflection when serializing on the hot path (the MQTT baseline uses
  plain `json.dumps`)

## Why the comparison is fair

- **Same translation task.** Same MAVLink message set, same scaling (cA->A,
  cdeg->deg, `voltage_battery` sentinel), same OPC UA node paths and MQTT
  topic/payload shape as the connector's exporters.
  `basyx-baseline/aas_definitions/uav_environment.json` is generated from
  `config/aas/quad_op.json` (same 7 submodels, ids and properties).
- **Same resource measurement.** Every baseline uses the same Docker-stats
  sidecar as the connector (`resource_metrics/resource_metrics.py`). The
  Docker-stats CSV is the CPU/memory number to report; the in-process `psutil`
  columns are a secondary, differently-sampled signal.
- **Per-message try/except.** It only keeps a single transient failure from
  ending a 15-minute run. Writes stay sequential and unbatched, with no retry.
- **Same security mode.** The connector defaults to `SecurityMode=None` and
  anonymous auth (`config/config.yaml`), like all baselines. If `tlsEnabled`
  is turned on for the connector in a run, this no longer holds.
- **`requests.Session()` in the BaSyx bridge is required, not an
  optimization.** At 200 Hz the naive bridge sends ~700 PATCHes/s; without
  keep-alive it runs out of ephemeral ports. The bridge is sequential, so the
  default pool size is enough and is not tuned.

## Known asymmetries

- **Reconnection.** `main.py` (OPC UA) and `bridge.py` (BaSyx) have none.
  `main_mqtt.py` gets auto-reconnect from paho-mqtt's defaults. When comparing
  resilience, group them as "OPC UA + BaSyx: none" vs "MQTT: library default",
  not as an OPC UA vs MQTT finding.
- **BaSyx runs two components on the Pi** (the JVM and the bridge), so it
  gets two resource CSVs (`basyx-aas-env_*.csv`, `basyx-bridge_*.csv`).
  Report both, not only their sum.
- **BaSyx per-property cost.** Each value is an HTTP round trip with JSON on
  both ends; the AAS REST API has no scalar-write fast path like OPC UA's
  node write. This is a protocol cost, not a bridge implementation issue.
- **BaSyx MQTT payloads are larger** (whole `SubmodelElement` JSON vs the
  connector's `{"value":...,"timestamp":...}`) and carry no timestamp, so
  BaSyx latency is computed with the sequence-number method
  (`latency-calculate.py`).

## Threats to validity

1. **Language/runtime confound.** Go vs Python (and the JVM, for BaSyx) sits
   on top of the architectural difference. The framing used is "what a
   practitioner would reach for to prototype this", which should be stated
   explicitly. A naive single-threaded Go ablation would separate the two if
   a reviewer asks; it is not implemented.
2. **BaSyx resource numbers are a lower bound.** `basyx.backend=InMemory`, no
   MongoDB, no registries. That is BaSyx's default and its cheapest
   deployment; a production setup would cost more.
3. **BaSyx MQTT eventing is always on**, because it is needed for latency
   measurement. Reported CPU/memory includes it.
4. **The naive BaSyx throughput number measures the bridge**, not BaSyx.
   `bridge_batched.py` exists to measure what BaSyx's API allows when
   requests are batched per submodel and pipelined; always say which bridge
   produced a number.

## BaSyx API notes

Behavior confirmed against BaSyx 2.0.0-milestone-15:

- Property write: `PATCH /submodels/<id>/submodel-elements/<idShortPath>/$value`.
  There is no `PUT` mapping (`405`).
- The body is the value JSON-encoded as a string (`"82.487"`), even for
  numbers. A bare number gives `500`.
- `idShortPath` uses `.` as separator (`GlobalPosition.Latitude`), unlike the
  `/` used in the other baselines' paths.
- Identifiers in URLs are unpadded Base64URL.
- Submodel-level `PATCH /submodels/<id>/$value` takes an array of full
  `SubmodelElement` definitions and **replaces** the submodel's element list
  (elements left out are deleted). A `{idShort: value}` object gives `400`.
  So the batched bridge keeps a full mirror of each submodel and always sends
  all of it.
- That submodel-level PATCH publishes on
  `sm-repository/<repo>/submodels/<id>/submodelElements/patched` with an
  array payload, not on `.../<idShortPath>/value/updated`. The receiver
  (`uav-aas-tools/mqtt_receiver/basyx_mqtt_receiver.py`) handles both.

## Run validity

- Naive BaSyx bridge: discard runs before 2026-08-11 (PUT, then bare-number
  bodies; 100% write failures).
- Batched BaSyx bridge: discard runs before 2026-08-13. Runs from 2026-08-13
  used a fixed 20 ms flush interval; use 2026-08-14 or later (reactive flush).
- Batched BaSyx MQTT latency: missing for runs before the 2026-08-14 receiver
  fix (the receiver ignored `.../patched` messages). Flush, ingestion and
  resource data from those runs are fine. 50 Hz and 200 Hz were re-run; 10 Hz
  still needs a re-run.
- Before using a BaSyx run, check the bridge's delivery summary:
  `_http_completeness` (naive) or `_flush_completeness.flush_success_pct`
  (batched) must be nonzero.

## Mapping checks

The scale factors and paths in `main.py`, `main_mqtt.py`, `bridge.py` and
`bridge_batched.py` are written by hand from `config/mappings/mavlink_ops.yaml`.
`basyx-baseline/verify_mapping.py` checks both BaSyx bridges against the YAML:
all 49 mappable entries match. The 4 unmapped entries are
`OPEN_DRONE_ID_SYSTEM.UaType` (not a real field on that message) and the 3
`ESC_STATUS.*` fields (not in the installed pymavlink dialect), same as
`main.py`. The same check for `main.py`/`main_mqtt.py` is not done yet.

## Open items

- Check that BaSyx only answers `GET /submodels` after the environment is
  fully loaded. `wait_for_basyx()` treats any non-5xx response as ready.
- Extend `verify_mapping.py` to `main.py`/`main_mqtt.py`.
