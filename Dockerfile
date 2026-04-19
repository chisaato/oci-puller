# Build stage
FROM golang:alpine AS builder

# Set architecture for static build
ENV CGO_ENABLED=0
ENV GOOS=linux

WORKDIR /app

# Copy dependency files first
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build static binary
# -ldflags="-s -w" removes symbol table and debug information
# -extldflags "-static" ensures all libraries are statically linked
RUN go build -a -installsuffix cgo -ldflags="-s -w -extldflags '-static'" -o oci-puller .

# Final stage
FROM gcr.io/distroless/static-debian12

WORKDIR /

# Copy binary from builder
COPY --from=builder /app/oci-puller /oci-puller

# Copy default config (if needed)
# COPY config.yaml /etc/oci-puller/config.yaml

# Expose port (adjust based on your config)
EXPOSE 9800


ENTRYPOINT ["/oci-puller", "server"]
