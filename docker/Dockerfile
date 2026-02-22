FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /ojs-server ./cmd/ojs-server

FROM alpine:3.21
RUN apk --no-cache add ca-certificates tzdata && \
    adduser -D -u 1001 ojs

LABEL org.opencontainers.image.source="https://github.com/openjobspec/ojs-backend-kafka" \
      org.opencontainers.image.description="OJS Kafka Backend" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --from=builder /ojs-server /usr/local/bin/ojs-server
USER ojs
EXPOSE 8080 9090

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -qO- http://localhost:8080/ojs/v1/health || exit 1

ENV OTEL_EXPORTER_OTLP_ENDPOINT=""

ENTRYPOINT ["ojs-server"]
