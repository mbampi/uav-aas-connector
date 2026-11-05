#!/usr/bin/env python3
"""Diffs both bridge implementations' property mappings against
config/mappings/mavlink_ops.yaml, the source of truth both the Go connector
and every naive baseline (this one included) are supposed to translate
identically:

  - bridge/bridge.py's do_patch() calls (the naive, per-property baseline)
  - bridge/bridge_batched.py's store.stage() calls (the batched/pipelined
    client - same submodel/idShortPath targets, different transport)

Both hand-duplicate this mapping rather than generating it from the YAML. Run
this after any change to config/mappings/mavlink_ops.yaml or either bridge
file's mapping section.

    python3 verify_mapping.py

Exit code is non-zero if either bridge maps a (submodel, idShortPath) that
isn't in the YAML, or misses one that both (a) is in the YAML and (b) is
technically writable - i.e. excluding the two known, documented gaps this
baseline inherits from python-baseline-conn/main.py:
  - OPEN_DRONE_ID_SYSTEM.UaType: not a real field on this MAVLink message
    (it belongs to OPEN_DRONE_ID_BASIC_ID) - the Go connector's own
    reflection-based lookup silently no-ops on it too.
  - ESC_STATUS.*: not in this project's installed pymavlink dialect, and
    not emitted by the synthetic replay traces (see main.py's comment on
    TRACKED_MESSAGE_TYPES).
"""

import os
import re
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
YAML_PATH = os.path.join(REPO_ROOT, "config", "mappings", "mavlink_ops.yaml")

# Each bridge file paired with the regex that extracts its
# (submodel, idShortPath) call targets - both call shapes put the submodel
# key and idShortPath as the first two positional string arguments, so one
# pattern per file covers it.
BRIDGES = [
    ("bridge.py", os.path.join(os.path.dirname(__file__), "bridge", "bridge.py"),
     re.compile(r"do_patch\(\s*'([^']+)'\s*,\s*'([^']+)'")),
    ("bridge_batched.py", os.path.join(os.path.dirname(__file__), "bridge", "bridge_batched.py"),
     re.compile(r"store\.stage\(\s*'([^']+)'\s*,\s*'([^']+)'")),
]

# (sourceMessage, sourceField) pairs known and disclosed to be unimplementable
# for the reasons in the module docstring above - not a mapping bug.
KNOWN_GAPS = {
    ("OPEN_DRONE_ID_SYSTEM", "UaType"),
    ("ESC_STATUS", "Rpm[0]"),
    ("ESC_STATUS", "Voltage[0]"),
    ("ESC_STATUS", "Current[0]"),
}

ENTRY_RE = re.compile(
    r'sourceMessage:\s*"([^"]+)"[^\n]*\n'
    r'\s*sourceField:\s*"([^"]+)"[^\n]*\n'
    r'\s*targetAASSubmodel:\s*"([^"]+)"[^\n]*'
    r'(?:\n\s*targetAASCollection:\s*"([^"]+)")?[^\n]*\n'
    r'\s*targetAASProperty:\s*"([^"]+)"'
)


def load_yaml_mappings():
    text = open(YAML_PATH).read()
    mappings = {}
    for msg, field, submodel, collection, prop in ENTRY_RE.findall(text):
        path = f"{collection}.{prop}" if collection else prop
        mappings[(msg, field)] = (submodel, path)
    return mappings


def load_bridge_targets(path, pattern):
    text = open(path).read()
    return {(submodel, path_) for submodel, path_ in pattern.findall(text)}


def check_bridge(label, path, pattern, yaml_mappings):
    bridge_targets = load_bridge_targets(path, pattern)

    expected_targets = {
        target for key, target in yaml_mappings.items() if key not in KNOWN_GAPS
    }
    unexpected_gaps = {
        key: target for key, target in yaml_mappings.items()
        if key in KNOWN_GAPS and target in bridge_targets
    }

    missing = sorted(expected_targets - bridge_targets)
    extra = sorted(bridge_targets - expected_targets)

    ok = True
    print(f"--- {label} ---")
    print(f"{len(bridge_targets)} mapping call targets found")

    if missing:
        ok = False
        print("MISSING (mapped in YAML, not written by this bridge):")
        for submodel, path_ in missing:
            print(f"  {submodel}.{path_}")

    if extra:
        ok = False
        print("EXTRA (written by this bridge, not mapped in YAML):")
        for submodel, path_ in extra:
            print(f"  {submodel}.{path_}")

    if unexpected_gaps:
        print("NOTE: these known gaps are now implemented - consider removing from KNOWN_GAPS:")
        for (msg, field), target in unexpected_gaps.items():
            print(f"  {msg}.{field} -> {target[0]}.{target[1]}")

    if ok:
        print("OK: matches mavlink_ops.yaml exactly, modulo the disclosed gaps.")
    print()
    return ok


def main():
    yaml_mappings = load_yaml_mappings()
    expected_count = sum(1 for k in yaml_mappings if k not in KNOWN_GAPS)
    print(f"mavlink_ops.yaml: {len(yaml_mappings)} entries "
          f"({len(KNOWN_GAPS)} known/disclosed gaps, {expected_count} expected per bridge)\n")

    ok = True
    for label, path, pattern in BRIDGES:
        ok = check_bridge(label, path, pattern, yaml_mappings) and ok

    if ok:
        print("ALL BRIDGES OK.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
