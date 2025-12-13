# --- Configuration ---
PI_USER := pi
PI_HOST := raspberrypi.local
PI_RESOURCES_METRICS := ~/uav-aas-connector/pi_metrics
PI_LATENCY_METRICS := ~/uav-aas-connector/pi_metrics
LOCAL_PATH  := ./metrics_received

# SSH Target (combines user and host)
REMOTE := $(PI_USER)@$(PI_HOST)

.PHONY: all sync clean help

# Default target: Syncs files then cleans them from remote
all: sync clean

# 1. SCP the metrics from Raspberry Pi to local machine
sync:
	@echo "--------------------------------------"
	@echo "📡 Fetching metrics from $(PI_HOST)..."
	@echo "--------------------------------------"
	@mkdir -p $(LOCAL_PATH)
	scp -r $(REMOTE):$(PI_RESOURCES_METRICS)/* $(LOCAL_PATH)/
	scp -r $(REMOTE):$(PI_LATENCY_METRICS)/* $(LOCAL_PATH)/
	@echo "✅ Transfer complete. Files saved to $(LOCAL_PATH)"

# 2. Delete the old metric files on the Raspberry Pi
clean:
	@echo "--------------------------------------"
	@echo "🧹 Cleaning up remote files on $(PI_HOST)..."
	@echo "--------------------------------------"
	ssh $(REMOTE) "rm -f $(PI_RESOURCES_METRICS)/*"
	ssh $(REMOTE) "rm -f $(PI_LATENCY_METRICS)/*"
	@echo "✅ Remote metrics deleted."

upload:
	@echo "--------------------------------------"
	@echo "📤 Uploading code to $(PI_HOST)..."
	@echo "--------------------------------------"
	scp -r ./cmd ./config ./pkg ./resource_metrics .dockerignore docker-compose.yml Dockerfile go.mod go.sum $(REMOTE):~/uav-aas-connector/
	@echo "✅ Upload complete."

# Help command to list available targets
help:
	@echo "Available commands:"
	@echo "  make sync   - Download metrics from Pi (keeps remote files)"
	@echo "  make clean  - Delete metrics on Pi (does not download)"
	@echo "  make all    - Download metrics AND delete them from Pi"
	@echo "  make upload - Upload code to Pi"