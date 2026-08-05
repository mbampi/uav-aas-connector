FROM python:3.12-slim

WORKDIR /app

# build-essential covers cases where a dependency has no prebuilt wheel for
# the target architecture (e.g. arm64 on a Raspberry Pi) and pip falls back
# to compiling from source.
RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker layer caching
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application source - both the OPC UA (main.py) and MQTT
# (main_mqtt.py) naive gateways live in the same image; which one runs is
# selected by ENTRYPOINT/the compose service's `entrypoint:` override below,
# not by a separate image per protocol.
COPY main.py main_mqtt.py ./

# 14550/udp: MAVLink Input
EXPOSE 14550/udp

# Default entrypoint runs the OPC UA variant; docker-compose.yml overrides
# this to ["python", "main_mqtt.py"] for the MQTT service.
ENTRYPOINT ["python", "main.py"]
