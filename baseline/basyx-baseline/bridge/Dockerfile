FROM python:3.12-slim

WORKDIR /app

# build-essential covers cases where a dependency has no prebuilt wheel for
# the target architecture (e.g. arm64 on a Raspberry Pi) and pip falls back
# to compiling from source.
RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Both the naive (bridge.py) and batched/pipelined (bridge_batched.py)
# bridges live in the same image - which one runs is selected by the
# compose service's `entrypoint:` override below, same pattern as
# ../../python-baseline-conn's OPC UA/MQTT variants.
COPY bridge.py bridge_batched.py ./

# 14550/udp: MAVLink Input
EXPOSE 14550/udp

# Default entrypoint runs the naive bridge; docker-compose.yml overrides
# this to ["python", "bridge_batched.py"] for the batched service.
ENTRYPOINT ["python", "bridge.py"]
