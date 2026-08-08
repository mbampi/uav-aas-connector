# Baselines

`python-baseline-conn/` holds the naive Python gateways used as a comparison
point for the Go `uav-aas-connector`: `main.py` (MAVLink to OPC UA) and
`main_mqtt.py` (MAVLink to MQTT). Usage in `python-baseline-conn/README.md`.

This file covers the methodology: what "naive" means here, why the comparison
is fair, and what has to be disclosed when reporting results.

## What "naive" means

The baselines are slower because of structural choices, not injected delays or
deliberately bad code:

- single-threaded, sequential translation
- one blocking write/publish per property, before the next message
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
- **Same resource measurement.** The baselines use the same Docker-stats
  sidecar as the connector (`resource_metrics/resource_metrics.py`). The
  Docker-stats CSV is the CPU/memory number to report; the in-process `psutil`
  columns are a secondary, differently-sampled signal.
- **Per-message try/except.** It only keeps a single transient failure from
  ending a 15-minute run. Writes stay sequential and unbatched, with no retry.
- **Same security mode.** The connector defaults to `SecurityMode=None` and
  anonymous auth (`config/config.yaml`), like the baselines. If `tlsEnabled`
  is turned on for the connector in a run, this no longer holds.

## Known asymmetries

`main.py` (OPC UA) has no reconnection. `main_mqtt.py` gets auto-reconnect from
paho-mqtt's defaults. This is a library default, not an OPC UA vs MQTT finding,
so disclose it when comparing resilience.

## Threats to validity

Go vs Python is a language/runtime confound on top of the architectural
difference. The framing used is "what a practitioner would reach for to
prototype this", which should be stated explicitly. A naive single-threaded Go
ablation would separate the two if a reviewer asks; it is not implemented.

## Mapping checks

The scale factors and paths in `main.py` and `main_mqtt.py` are written by hand
from `config/mappings/mavlink_ops.yaml`. A diff between the two is still to do.
