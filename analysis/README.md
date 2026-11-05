# Analysis Tools

Six command-line scripts for analyzing the latency and resource-usage logs
produced by the Go connector (`pkg/instrumentation`), the naive Python
baseline (`baseline/python-baseline-conn`), and the Eclipse BaSyx baseline
(`baseline/basyx-baseline`), plus a batch runner for the message-loss one.
Some of them read the same input file, so check what one row means before
picking one.

## Quick reference

| Tool | Reads | One row = | Use it for |
|---|---|---|---|
| `latency_analyzer.py` | connector `latency_*.log` | one **AAS property** update | understanding where time goes *inside* the connector's own pipeline stages |
| `message_latency_analyzer.py` | connector `latency_*.log` (same file) | one **MAVLink message** (reconciled) | comparing the connector against the baseline |
| `message_loss_analyzer.py` | connector `latency_*.log` + that run's `dispatch_connector_*.csv` + a mapping YAML | one **property update, classified** | splitting "missing trace" into coalesced vs candidate real loss |
| `baseline_latency_analyzer.py` | baseline `naive_baseline_*.csv` or BaSyx's `basyx_baseline_bridge_metrics.csv` (same schema) | one **MAVLink message** (native) | the baseline-side numbers for that same comparison |
| `resource_analyzer.py` | any Docker-stats sidecar CSV (`resource_metrics/resource_metrics.py`'s output) | one **sample in time** | CPU/Memory usage, for either system |
| `dropped_traffic_analyzer.py` | that run's `dispatch_basyx_mqtt_*.csv` + `basyx_baseline_bridge_metrics.csv` | one **MAVLink message type** | BaSyx-baseline UDP-to-bridge loss, per type, when no `SOURCE_TLOG_PATH` delivery summary was captured |

All but `resource_analyzer.py` are stdlib-only; `resource_analyzer.py` needs
`pandas` and `matplotlib` (`latency_analyzer.py` also imports `matplotlib`
for its chart, but nothing else does).

## Why two tools read the connector's `latency_*.log`?

The connector's instrumentation logs one line per pipeline stage
(`ADAPTER`/`CORE`/`{PROTOCOL}_SENDING`/`{PROTOCOL}_SENT`) per **AAS
property**. A single `ATTITUDE` message fans out into 6 properties (Roll,
Pitch, Yaw, RollSpeed, PitchSpeed, YawSpeed), each with its own trace through
those stages.

The naive baseline (`main.py`/`main_mqtt.py`) doesn't work that way - it's a
single-threaded loop that translates and writes/publishes every property
from one message, sequentially and blocking, before moving to the next
message, and logs **one** latency number per message covering all of it.

Comparing the connector's per-property numbers directly against that would
flatter the connector: a message that costs the baseline 6 sequential writes
would only ever contribute 6 *independent* samples to the connector's
per-property distribution, never a "cost of all 6" figure. So:

- **`latency_analyzer.py`** reports the per-property view. Use it to
  understand the connector's *own* pipeline (e.g. why OPC UA's
  `Sending→Sent` stage has a heavy tail, Section IV-D's batching behavior).
  This is what produced Table I / the per-property numbers in the paper's
  Section V.
- **`message_latency_analyzer.py`** reads the exact same log but groups by
  `TraceID_ns` alone (shared by every property one message fans out into)
  and takes `max(SENT timestamp across that message's properties) −
  TraceID_ns` - "time until every property this message produced was fully
  sent." **This is the number that's actually comparable to the baseline's
  `Processing_Latency_ms`.** It also separates messages into fully
  completed / partially completed / fully lost, since a message can have
  some of its properties coalesced (superseded by a newer value) without
  that being data loss - see its docstring and `EXPERIMENTS_SUMMARY.md`
  Section 5/7 for the full reasoning.

**Rule of thumb: if you're about to write a sentence comparing a connector
number to a baseline number, the connector side of that sentence should come
from `message_latency_analyzer.py`, never from `latency_analyzer.py`.**

## Usage

### `latency_analyzer.py` - connector, per-property

```bash
python latency_analyzer.py <path-to-latency_*.log> --protocol {mqtt|opcua}
```

Example:
```bash
python latency_analyzer.py \
  ../experiments/dissertation_exps/connector/mqtt_50hz_connector/latency_20260803_005123.log \
  --protocol mqtt
```

Prints per-stage (`Adapter→Core`, `Core→Sending`, `Sending→Sent`, `Total`)
count/avg/p90/p95/p99, and saves `latency_chart.png` (latency over time with
a moving average) to the current directory.

### `message_latency_analyzer.py` - connector, per-message (reconciled)

Same arguments as `latency_analyzer.py`, same input file:

```bash
python message_latency_analyzer.py <path-to-latency_*.log> --protocol {mqtt|opcua}
```

Prints total distinct message traces, the fully-completed / partial / fully
lost breakdown, mean properties-per-message (fan-out), and the message-level
latency distribution for "fully completed" messages and for "fully
completed + partial" combined. No chart (numbers only).

### `message_loss_analyzer.py` - connector, coalesced vs. real loss

```bash
python message_loss_analyzer.py <path-to-latency_*.log> --protocol {mqtt|opcua} \
  [--dispatch-csv <path-to-dispatch_connector_*.csv> --mapping <path-to-mapping.yaml>]
```

Example:
```bash
python message_loss_analyzer.py \
  ../experiments/dissertation_exps/connector/mqtt_200hz_connector/latency_20260803_003249.log \
  --protocol mqtt \
  --dispatch-csv ../experiments/dissertation_exps/connector/mqtt_200hz_connector/dispatch_connector_mqtt_200hz.csv \
  --mapping ../config/mappings/mavlink_ops.yaml
```

`EXPERIMENTS_SUMMARY.md` Section 5 identified a gap - dispatched messages
that never produced a `{PROTOCOL}_SENT` line - but couldn't split it into
"coalesced" (the documented latest-value-wins behavior) vs. real loss,
because these runs didn't capture the runtime `instrumentation.DropCounter`/
`CoalescedCounter` values. This script closes that gap after the fact, using
only artifacts these experiments already produced:

- **Tier 0 (pre-adapter)**: compares the run's `dispatch_connector_*.csv`
  (ground truth of what was actually replayed, filtered to message types the
  given mapping YAML can produce an `ADAPTER` line for) against the count of
  distinct MAVLink messages that actually reached the adapter. Requires
  `--dispatch-csv` and `--mapping`; skipped if either is omitted.
- **Tier 1 (adapter → core)**: exact, not inferred.
  `instrumentation.Record("ADAPTER", ...)` always fires before the adapter's
  (non-blocking) send into core's inbound channel, so any `(TraceID,
  PropertyID)` with an `ADAPTER` row but no `CORE` row is a real drop.
- **Tier 2 (core → sent)**: per property (`SubmodelID.PropertyID`), every
  update strictly before that property's *last* update in the run is either
  sent or was superseded by a later one (or a chain of them) - i.e.
  provably coalesced. Only a property's last update in the run, if it also
  never reached `{PROTOCOL}_SENT`, is a candidate for real loss; it's
  further split against this run's own worst-case observed
  Core→Sent latency to separate "plausibly still in flight when the log
  window closed" from "unexplained by that."

Prints stage counts, the three tiers, and an estimated real-loss total. See
the script's module docstring for the full reasoning and the known blind
spot (a non-final update dropped by a full `core.broadcast.<name>` channel
is indistinguishable from coalescing unless it happens to be that
property's last update).

### `baseline_latency_analyzer.py` - baseline, per-message

```bash
python baseline_latency_analyzer.py <path-to-naive_baseline_metrics.csv>
```

Example:
```bash
python baseline_latency_analyzer.py \
  ../experiments/dissertation_exps/baseline/mqtt_50hz_baseline/naive_baseline_mqtt_metrics.csv
```

Prints throughput, message-type counts, write-success/failure counts, and
stats for `Queue_Wait_ms`, `Processing_Latency_ms`, and their sum (`Total`)
- directly comparable to `message_latency_analyzer.py`'s "fully-completed
messages only" output. Latency stats are computed **only over
`Write_Success=True` rows**: both baseline gateways catch a per-message
write/publish failure rather than crashing the whole run (see
`baseline/python-baseline-conn/README.md`'s Metrics Output section), and a
failed row's latency is time-until-failure, not a completed operation - mixing
it into the distribution would corrupt it, mirroring exactly how
`message_latency_analyzer.py` excludes coalesced/lost connector traces from
its own stats. Failure counts are reported separately, broken down by
message type. (Older-format CSVs without a `Write_Success` column are still
readable - every row is just treated as successful, matching the pre-fix
crash-on-any-failure behavior.)

If a `<CSV base>_delivery_summary.json` sits next to the CSV (written when
the baseline is run with `SOURCE_TLOG_PATH` set), its overall delivery ratio
is printed too, labeled as self-reported - not the externally cross-checked
kind described below.

### `resource_analyzer.py` - CPU/Memory, either system

```bash
python resource_analyzer.py <path-to-resource-csv> \
  [--align-to <path-to-latency_*.log-or-naive_baseline_*.csv>] \
  [--no-zero-filter]
```

Works on any CSV from the shared Docker-stats sidecar
(`resource_metrics/resource_metrics.py`), whether it was monitoring the
connector or either baseline gateway - same schema either way.

- **By default**, rows with `Memory_Bytes == 0` are excluded before
  computing statistics. A live process never legitimately reports exactly 0
  bytes resident; these rows come from the sidecar continuing to poll after
  the container under test has already been stopped for teardown between
  runs (see `EXPERIMENTS_SUMMARY.md` Section 6 for the full root-cause
  writeup). Pass `--no-zero-filter` to see the raw, unfiltered numbers.
- **`--align-to`** tightens this further: point it at the `latency_*.log` or
  `naive_baseline_*.csv` from the *same run*, and only resource samples
  falling inside that file's own timestamp range are kept. This catches the
  few genuinely-valid, near-idle samples in the gap between dispatch ending
  and the container actually being stopped, which the zero-filter alone
  doesn't. Format (connector log vs. baseline CSV) is auto-detected from the
  companion file's header - no extra flag needed.

Example, connector:
```bash
python resource_analyzer.py \
  ../experiments/dissertation_exps/connector/mqtt_200hz_connector/uav-aas-connector_20260803_003249.csv \
  --align-to ../experiments/dissertation_exps/connector/mqtt_200hz_connector/latency_20260803_003249.log
```

Example, baseline:
```bash
python resource_analyzer.py \
  ../experiments/dissertation_exps/baseline/mqtt_50hz_baseline/python-baseline-conn_<timestamp>.csv \
  --align-to ../experiments/dissertation_exps/baseline/mqtt_50hz_baseline/naive_baseline_mqtt_metrics.csv
```

Prints mean/std/min/max/p95/p99 for CPU%, Memory (MB), and Memory%, and how
many samples were excluded and why. Saves `resource_usage_chart.png` to the
current directory - excluded points are plotted as red X's over the full
raw timeline rather than silently dropped, so an artifact like this one
stays visible if it happens again.

For the BaSyx baseline specifically, run it once per component
(`basyx-aas-env_*.csv` and `basyx-bridge_*.csv`, both aligned to that run's
`basyx_baseline_bridge_metrics.csv`) and rename the chart between runs, since
both would otherwise overwrite the same `resource_usage_chart.png` - see
`experiments/dissertation_exps/basyx/mqtt_50hz_basyx/README.md` for a worked
example.

### `dropped_traffic_analyzer.py` - BaSyx baseline, UDP-to-bridge loss

```bash
python dropped_traffic_analyzer.py <path-to-dispatch_basyx_mqtt_*.csv> <path-to-basyx_baseline_bridge_metrics.csv>
```

`baseline/basyx-baseline/README.md`'s Phase 5 table computes
"Dropped traffic" from the bridge's self-reported
`_delivery_summary.json` (written only when `SOURCE_TLOG_PATH` was set for
that run). When it wasn't, this script gets the same number a different way:
per-type dispatched count (`dispatch_basyx_mqtt_*.csv`, restricted to the
MAVLink message types `bridge.py` actually implements, i.e.
`config/mappings/mavlink_ops.yaml`'s `sourceMessage` set) against the count
of rows the bridge logged for that type in `basyx_baseline_bridge_metrics.csv`
(one row per attempted PATCH, regardless of its outcome). The gap is loss
between the Pi's UDP socket and the bridge's `recv_match()` loop - i.e.
before any HTTP is even attempted - not to be confused with PATCH failures,
which `baseline_latency_analyzer.py`'s `Write_Success` column already covers
separately.

## Putting it together: comparing connector vs. baseline at one rate

```bash
# Connector side (message-level, the fair basis)
python message_latency_analyzer.py \
  ../experiments/dissertation_exps/connector/mqtt_50hz_connector/latency_20260803_005123.log \
  --protocol mqtt

# Baseline side
python baseline_latency_analyzer.py \
  ../experiments/dissertation_exps/baseline/mqtt_50hz_baseline/naive_baseline_mqtt_metrics.csv

# Resource usage, both sides
python resource_analyzer.py \
  ../experiments/dissertation_exps/connector/mqtt_50hz_connector/uav-aas-connector_20260803_005124.csv \
  --align-to ../experiments/dissertation_exps/connector/mqtt_50hz_connector/latency_20260803_005123.log

python resource_analyzer.py \
  ../experiments/dissertation_exps/baseline/mqtt_50hz_baseline/python-baseline-conn_<timestamp>.csv \
  --align-to ../experiments/dissertation_exps/baseline/mqtt_50hz_baseline/naive_baseline_mqtt_metrics.csv
```

The "fully-completed messages only" row from `message_latency_analyzer.py`
and the "Processing_Latency_ms" / "Total" rows from
`baseline_latency_analyzer.py` are the two numbers that belong in the same
table cell.

See `experiments/dissertation_exps/connector/EXPERIMENTS_SUMMARY.md` for the
full connector-side results these tools already produced, and
`baseline/python-baseline-conn/README.md` for how to run the baseline
gateways and their resource-monitoring sidecar in the first place.

## Running `message_loss_analyzer.py` across every connector experiment

`run_message_loss_for_all_experiments.sh` finds every experiment folder
under `experiments/dissertation_exps/connector/`, runs
`message_loss_analyzer.py` against it (once per protocol - twice for
`both_*` folders), and appends the command plus its output to the end of
that folder's `README.md`/`readme.md`, matching the existing "command then
verbatim output" style already in those files. It's a no-op on a folder
whose README already has a message-loss section, so it's safe to re-run.

```bash
bash analysis/run_message_loss_for_all_experiments.sh
```
