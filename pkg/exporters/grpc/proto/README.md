# Protocol Buffers for AAS gRPC Exporter

This directory contains the Protocol Buffers (protobuf) definitions used by the gRPC exporter in the UAV AAS Connector project. The protobuf files define the structure of the messages exchanged between the UAV AAS Connector and gRPC clients or servers.

If any changes are made to the `.proto` files, the corresponding Go code must be regenerated to reflect those changes.
```bash
protoc --go_out=. --go-grpc_out=. aas.proto
```