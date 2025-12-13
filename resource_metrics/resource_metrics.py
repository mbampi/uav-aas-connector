import docker
import csv
import time
import os
import sys
from datetime import datetime

# --- Configuration ---
# Container to monitor
CONTAINER_NAME = os.getenv("TARGET_CONTAINER", "uav-aas-connector")
DURATION_SEC = int(os.getenv("DURATION_SEC", 660))
INTERVAL_SEC = float(os.getenv("INTERVAL_SEC", 0.5))

# Output setup
OUTPUT_DIR = "/output"
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

print(f"--- Native Metrics Collector ---")
print(f"Target: {CONTAINER_NAME}")
print(f"Duration: {DURATION_SEC}s")
print(f"Connecting to Docker Socket...")

# Connect to Docker Socket (requires /var/run/docker.sock mounted)
try:
    client = docker.from_env()
    container = client.containers.get(CONTAINER_NAME)
    print(f"Success: Found container '{container.name}' ({container.id[:12]})")
except docker.errors.NotFound:
    print(f"FATAL: Container '{CONTAINER_NAME}' not found. Is it running?")
    sys.exit(1)
except Exception as e:
    print(f"FATAL: Could not connect to Docker Daemon. {e}")
    sys.exit(1)

strTime = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = os.path.join(OUTPUT_DIR, f'{CONTAINER_NAME}_{strTime}.csv')

def calculate_cpu_percent(d):
    """Calculates CPU % exactly like 'docker stats'"""
    try:
        cpu_stats = d.get("cpu_stats", {})
        precpu_stats = d.get("precpu_stats", {})
        
        # CPU Usage
        cpu_usage = cpu_stats.get("cpu_usage", {})
        precpu_usage = precpu_stats.get("cpu_usage", {})
        
        cpu_count = len(cpu_usage.get("percpu_usage", []) or [])
        
        # Fallback for cpu count if percpu_usage is missing (common in cgroups v2)
        if cpu_count == 0:
             cpu_count = cpu_stats.get("online_cpus", 1)

        cpu_delta = cpu_usage.get("total_usage", 0) - precpu_usage.get("total_usage", 0)
        system_delta = cpu_stats.get("system_cpu_usage", 0) - precpu_stats.get("system_cpu_usage", 0)
        
        if system_delta > 0.0 and cpu_delta > 0.0:
            cpu_percent = (cpu_delta / system_delta) * cpu_count * 100.0
            return cpu_percent
    except Exception:
        pass
    return 0.0

# Open CSV
with open(filename, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["Timestamp", "CPU_Percent", "Memory_Bytes", "Memory_Percent"])
    
    start_time = time.time()
    next_call = start_time
    
    print(f"Starting collection... Writing to {filename}")
    
    while (time.time() - start_time) < DURATION_SEC:
        try:
            # Fetch real-time stats
            stats = container.stats(stream=False)
            
            # 1. CPU
            cpu_pct = calculate_cpu_percent(stats)
            
            # 2. Memory
            mem_stats = stats.get("memory_stats", {})
            
            # Check if 'usage' key exists, handle missing key gracefully
            if "usage" in mem_stats:
                mem_current = mem_stats["usage"]
                mem_limit = mem_stats.get("limit", 1) # avoid divide by zero
            else:
                # Fallback or zero if cgroups disabled
                mem_current = 0
                mem_limit = 1
                
            mem_pct = (mem_current / mem_limit) * 100.0
            
            # 3. Timestamp
            now = time.time()
            
            writer.writerow([now, f"{cpu_pct:.2f}", mem_current, f"{mem_pct:.2f}"])
            csvfile.flush()
            
            print(f"T: {datetime.fromtimestamp(now).strftime('%H:%M:%S')} | CPU: {cpu_pct:.5f}% | MEM: {mem_pct:.2f}%")
            
        except Exception as e:
            print(f"Error collecting metrics: {e}")
            try:
                container.reload()
                if container.status != 'running':
                    print("Target container stopped. Exiting.")
                    break
            except:
                break

        next_call = next_call + INTERVAL_SEC
        time.sleep(max(0, next_call - time.time()))

print("Collection Complete.")