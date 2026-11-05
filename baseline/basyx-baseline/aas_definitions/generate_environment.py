#!/usr/bin/env python3
"""Regenerates uav_environment.json from the connector's own AAS definition.

uav_environment.json is a mechanical wrapper around
../../../config/aas/quad_op.json - the SAME submodels (same ids, idShorts,
elements, values) the Go connector and python-baseline-conn both load, just
wrapped in the official AAS V3 "Environment" JSON shape
(assetAdministrationShells / submodels / conceptDescriptions) that BaSyx's
aas-environment component expects (see ../README.md Phase 1). It adds one
AssetAdministrationShell referencing all 7 submodels; it does not add,
remove, rename, or restructure a single submodel element.

Run this after any change to config/aas/quad_op.json to keep this baseline's
AAS model in sync - do NOT hand-edit uav_environment.json's "submodels" key
directly, since that guarantee is exactly what this script exists to keep:

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
