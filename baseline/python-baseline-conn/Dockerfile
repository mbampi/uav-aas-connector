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

# Copy the application source
COPY main.py ./

# 14550/udp: MAVLink Input
EXPOSE 14550/udp

ENTRYPOINT ["python", "main.py"]
