# Stage 1: Builder
FROM golang:1.24-alpine AS builder

# Install git for fetching dependencies
RUN apk add --no-cache git

WORKDIR /app

# Copy go.mod and go.sum first to leverage Docker layer caching
# This makes subsequent builds much faster if dependencies haven't changed
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Build the application
# CGO_ENABLED=0: Creates a static binary (no dependency on C libraries)
# -ldflags="-s -w": Strips debug information to reduce binary size
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o uav-aas-connector ./cmd/main.go

# Stage 2: Runtime
# We use a minimal Alpine image for the final container
FROM alpine:latest

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/uav-aas-connector .

# Copy default configuration files (can be overridden by volumes)
COPY config/ ./config/

# 14550/udp: MAVLink Input
# 8080/tcp:  HTTP Exporter
# 4840/tcp:  OPC UA Exporter
EXPOSE 14550/udp 8080 4840

# Run the binary
ENTRYPOINT ["./uav-aas-connector"]