# Analysis Tools

Scripts for the latency and resource logs produced by the Go connector
(`pkg/instrumentation`), the Python baseline (`baseline/python-baseline-conn`)
and the BaSyx baseline (`baseline/basyx-baseline`).

| Tool | Reads | One row is | Use |
|---|---|---|---|
| `latency_analyzer.py` | connector `latency_*.log` | one AAS property update | time spent in each connector stage |
| `message_latency_analyzer.py` | connector `latency_*.log` | one MAVLink message | comparing the connector with a baseline |
| `message_loss_analyzer.py` | connector `latency_*.log`, `dispatch_connector_*.csv`, mapping YAML | one classified property update | coalesced updates vs possible real loss |
| `baseline_latency_analyzer.py` | `naive_baseline_*.csv` or `basyx_baseline_bridge_metrics.csv` | one MAVLink message | baseline latency |
| `resource_analyzer.py` | Docker-stats CSV from `resource_metrics.py` | one sample in time | CPU and memory for any system |
| `dropped_traffic_analyzer.py` | `dispatch_basyx_mqtt_*.csv`, `basyx_baseline_bridge_metrics.csv` | one message type | BaSyx UDP-to-bridge loss |

`resource_analyzer.py` needs `pandas` and `matplotlib`, and
`latency_analyzer.py` needs `matplotlib`. The others use only the standard
library.

## Per-property vs per-message latency

The connector logs one line per stage (`ADAPTER`, `CORE`, `{PROTOCOL}_SENDING`,
`{PROTOCOL}_SENT`) for each AAS property. One `ATTITUDE` message becomes 6
properties, each with its own trace. The baselines log one number per message,
which covers writing all of its properties.

- `latency_analyzer.py` shows the per-property view, for looking at the
  connector's own stages.
- `message_latency_analyzer.py` groups the same log by `TraceID_ns` and uses
  `max(SENT across the message's properties) - TraceID_ns`. This is comparable
  with the baseline's `Processing_Latency_ms`.

When comparing the connector with a baseline, use
`message_latency_analyzer.py` for the connector.

## Usage

Examples use log paths from a local `experiments/` folder, which isn't in the
repository.

### `latency_analyzer.py`

```bash
python latency_analyzer.py <latency_*.log> --protocol {mqtt|opcua}
```

Prints count/avg/p90/p95/p99 per stage (`Adapter→Core`, `Core→Sending`,
`Sending→Sent`, `Total`) and saves `latency_chart.png` to the current directory.

### `message_latency_analyzer.py`

```bash
python message_latency_analyzer.py <latency_*.log> --protocol {mqtt|opcua}
```

Prints the number of traces, the completed/partial/lost breakdown, properties
per message, and the latency distribution for completed messages and for
completed + partial.

### `message_loss_analyzer.py`

```bash
python message_loss_analyzer.py <latency_*.log> --protocol {mqtt|opcua} \
  [--dispatch-csv <dispatch_connector_*.csv> --mapping <mapping.yaml>]
```

Example:

```bash
python message_loss_analyzer.py \
  ../experiments/dissertation_exps/connector/mqtt_200hz_connector/latency_20260803_003249.log \
  --protocol mqtt \
  --dispatch-csv ../experiments/dissertation_exps/connector/mqtt_200hz_connector/dispatch_connector_mqtt_200hz.csv \
  --mapping ../config/mappings/mavlink_ops.yaml
```

Splits updates that never reached `{PROTOCOL}_SENT` into three tiers:

- Tier 0 (before the adapter): compares the dispatch CSV with the messages that
  reached the adapter. Needs `--dispatch-csv` and `--mapping`.
- Tier 1 (adapter to core): an `ADAPTER` row with no `CORE` row is a real drop.
- Tier 2 (core to sent): any update before a property's last one was either
  sent or replaced by a newer value. Only a last update with no `SENT` line can
  be real loss. Those are split by whether they may still have been in flight
  when the log ended.

See the script's docstring for its known blind spot.

To run it for every folder under `experiments/dissertation_exps/connector/` and
append the output to each folder's README:

```bash
bash analysis/run_message_loss_for_all_experiments.sh
```

Folders that already have a message-loss section are skipped.

### `baseline_latency_analyzer.py`

```bash
python baseline_latency_analyzer.py <naive_baseline_metrics.csv>
```

Prints throughput, message counts, write success/failure, and stats for
`Queue_Wait_ms`, `Processing_Latency_ms` and their sum. Rows with
`Write_Success=False` are left out of the latency stats and counted separately.
CSVs without that column are treated as all successful. If a
`<CSV base>_delivery_summary.json` is next to the CSV, its delivery ratio is
printed too.

### `resource_analyzer.py`

```bash
python resource_analyzer.py <resource.csv> \
  [--align-to <latency_*.log or naive_baseline_*.csv>] \
  [--no-zero-filter]
```

- Rows with `Memory_Bytes == 0` are excluded by default. They come from the
  sidecar polling after the container has stopped. `--no-zero-filter` keeps them.
- `--align-to` keeps only samples inside the time range of a log or CSV from the
  same run. The format is detected from its header.

Prints mean/std/min/max/p95/p99 for CPU%, memory (MB) and memory%, and saves
`resource_usage_chart.png` to the current directory, with excluded points marked
in red.

For BaSyx, run it once for `basyx-aas-env_*.csv` and once for
`basyx-bridge_*.csv`, and rename the chart between runs.

### `dropped_traffic_analyzer.py`

```bash
python dropped_traffic_analyzer.py <dispatch_basyx_mqtt_*.csv> <basyx_baseline_bridge_metrics.csv>
```

Compares dispatched counts per message type with the rows the bridge logged.
The gap is loss between the Pi's UDP socket and the bridge, before any HTTP
request. Use it when a run has no `SOURCE_TLOG_PATH` delivery summary. Failed
PATCHes are a separate number (`Write_Success`).

## Comparing the connector and a baseline at one rate

```bash
# Connector
python message_latency_analyzer.py <connector latency_*.log> --protocol mqtt

# Baseline
python baseline_latency_analyzer.py <naive_baseline_mqtt_metrics.csv>

# Resources, both sides
python resource_analyzer.py <uav-aas-connector_*.csv> --align-to <connector latency_*.log>
python resource_analyzer.py <python-baseline-conn_*.csv> --align-to <naive_baseline_mqtt_metrics.csv>
```

Compare the "fully-completed messages only" row from
`message_latency_analyzer.py` with the `Processing_Latency_ms` and `Total` rows
from `baseline_latency_analyzer.py`.
