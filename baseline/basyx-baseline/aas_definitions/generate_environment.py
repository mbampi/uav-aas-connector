#!/usr/bin/env python3
"""Regenerates uav_environment.json from ../../../config/aas/quad_op.json.

Wraps the same submodels in the AAS V3 "Environment" JSON shape that BaSyx's
aas-environment component expects, and adds one AssetAdministrationShell
referencing all 7. Run it after changing quad_op.json, and don't edit
uav_environment.json's "submodels" by hand:

    python3 generate_environment.py
    git diff --stat uav_environment.json   # should be empty if quad_op.json didn't change
"""

import json
import os

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
SOURCE = os.path.join(REPO_ROOT, "config", "aas", "quad_op.json")
OUTPUT = os.path.join(os.path.dirname(__file__), "uav_environment.json")


def main():
    with open(SOURCE) as f:
        source = json.load(f)

    submodels = source["submodels"]

    submodel_refs = [
        {"type": "ModelReference", "keys": [{"type": "Submodel", "value": sm["id"]}]}
        for sm in submodels
    ]

    environment = {
        "assetAdministrationShells": [
            {
                "id": "urn:aas:uav-1:001",
                "idShort": "UAV1",
                "modelType": "AssetAdministrationShell",
                "description": [
                    {
                        "language": "en",
                        "text": "BaSyx naive-baseline AAS shell for the synthetic UAV telemetry "
                                "stream (same 7 submodels as the Go connector's config/aas/quad_op.json).",
                    }
                ],
                "assetInformation": {
                    "assetKind": "Instance",
                    "globalAssetId": "urn:asset:uav-1:001",
                },
                "submodels": submodel_refs,
            }
        ],
        "submodels": submodels,
        "conceptDescriptions": [],
    }

    with open(OUTPUT, "w") as f:
        json.dump(environment, f, indent=4)
        f.write("\n")

    print(f"Wrote {len(submodels)} submodels from {SOURCE} to {OUTPUT}")


if __name__ == "__main__":
    main()
