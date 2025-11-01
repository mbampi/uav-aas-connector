// 1. Load config (using pkg/config).
// 2. Create channels (rawChan, processedChan).
// 3. Start Adapters, Core and Exporters as goroutines.
// 4. Handle graceful shutdown on interrupt signal.

package main
