FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /ojs-server ./cmd/ojs-server

FROM alpine:3.19
RUN apk --no-cache add ca-certificates && \
    adduser -D -u 1001 ojs
COPY --from=builder /ojs-server /ojs-server
USER ojs
EXPOSE 8080

# Optional: OpenTelemetry OTLP exporter endpoint (e.g., http://localhost:4317)
ENV OTEL_EXPORTER_OTLP_ENDPOINT=""

ENTRYPOINT ["/ojs-server"]
