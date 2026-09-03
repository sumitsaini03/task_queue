# ==========================================
# Stage 1: Build static binary
# ==========================================
FROM golang:1.24-alpine AS builder

WORKDIR /src

# Install CA certs and tzdata for minimal runtime
RUN apk add --no-cache ca-certificates tzdata

# Copy go.mod
COPY go.mod ./
RUN go mod download

# Copy application source
COPY . .

# Build statically linked binary with stripped symbols
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -extldflags '-static'" \
    -o /taskqueue ./cmd/server

# ==========================================
# Stage 2: Minimal distroless runtime
# ==========================================
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /

# Copy CA certificates and timezone data
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

# Copy static binary
COPY --from=builder /taskqueue /taskqueue

# Expose HTTP API port
EXPOSE 8080

# Run as nonroot user (UID 65532)
USER nonroot:nonroot

# Persistent volume for WAL and snapshots
VOLUME ["/data/wal"]

ENTRYPOINT ["/taskqueue"]
