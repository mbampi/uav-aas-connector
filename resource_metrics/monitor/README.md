



## Compile

// For Raspberry Pi 4 (64-bit OS)
```bash
GOOS=linux GOARCH=arm64 go build -o monitor_uav monitor.go
```

## Transfer to Raspberry Pi

```bash
scp monitor_uav agriwing@10.0.0.50:/home/agriwing/uav-aas-connector/monitor_uav
```



## Run
In the Raspberry Pi, run:

```bash
chmod +x monitor_uav
./monitor_uav
```