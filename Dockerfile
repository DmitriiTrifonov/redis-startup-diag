# Small image with all three tools. Build and push to your registry:
#   docker build -t <registry>/redis-startup-diag:latest .
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

# busybox instead of distroless: handy `sh`, `cat /etc/resolv.conf` etc. in the pod
FROM busybox:1.36
COPY --from=build /out/ /usr/local/bin/
ENTRYPOINT ["/bin/sh", "-c"]
CMD ["netprobe && startup -duration 30s"]
