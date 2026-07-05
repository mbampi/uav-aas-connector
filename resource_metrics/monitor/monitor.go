package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/shirou/gopsutil/v3/process"
)

// Config
const (
	TargetProcessName = "uav-aas-connector"
	Interval          = 1 * time.Second
	OutputDir         = "resource_metrics"
)

func main() {
	// 1. Find the process ID (PID)
	proc, err := findProcessByName(TargetProcessName)
	if err != nil {
		log.Fatalf("Error: %v. Make sure the binary is running.", err)
	}
	pid := proc.Pid
	fmt.Printf("Found %s with PID: %d\n", TargetProcessName, pid)

	// 2. Ensure Output Directory Exists
	// 0755 means read/write/execute for owner, read/execute for others
	if err := os.MkdirAll(OutputDir, 0755); err != nil {
		log.Fatalf("Failed to create directory '%s': %v", OutputDir, err)
	}

	// 3. Create CSV file with timestamp inside the directory
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("resources_%s.csv", timestamp)
	fullPath := filepath.Join(OutputDir, filename)

	file, err := os.Create(fullPath)
	if err != nil {
		log.Fatalf("Cannot create file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write CSV Header
	// RSS = Resident Set Size (Physical RAM used)
	// VMS = Virtual Memory Size
	header := []string{"Timestamp", "CPU_Percent", "Memory_RSS_Bytes", "Memory_VMS_Bytes"}
	if err := writer.Write(header); err != nil {
		log.Fatalf("Cannot write header: %v", err)
	}
	writer.Flush()

	fmt.Printf("Logging to %s (Press Ctrl+C to stop)...\n", fullPath)

	// 4. Setup graceful shutdown (Ctrl+C)
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	// 5. Monitoring Loop
	ticker := time.NewTicker(Interval)
	defer ticker.Stop()

	for {
		select {
		case <-stopChan:
			fmt.Println("\nStopping monitor...")
			return
		case t := <-ticker.C:
			// Get CPU percent
			// Passing 0 uses the time since last call to calculate delta automatically
			cpuPercent, err := proc.Percent(0)
			if err != nil {
				log.Printf("Process likely stopped: %v", err)
				return
			}

			// Get Memory Info
			memInfo, err := proc.MemoryInfo()
			if err != nil {
				log.Printf("Process likely stopped: %v", err)
				return
			}

			// Prepare record
			timeStr := t.Format("15:04:05")
			record := []string{
				timeStr,
				fmt.Sprintf("%.2f", cpuPercent),
				fmt.Sprintf("%d", memInfo.RSS),
				fmt.Sprintf("%d", memInfo.VMS),
			}

			if err := writer.Write(record); err != nil {
				log.Printf("Error writing record: %v", err)
			}
			writer.Flush()
		}
	}
}

func findProcessByName(name string) (*process.Process, error) {
	procs, err := process.Processes()
	if err != nil {
		return nil, err
	}

	for _, p := range procs {
		n, err := p.Name()
		if err != nil {
			continue
		}
		// Matches exact name
		if n == name {
			return p, nil
		}
	}
	return nil, fmt.Errorf("process '%s' not found", name)
}
