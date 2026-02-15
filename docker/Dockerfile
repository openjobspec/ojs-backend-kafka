FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /ojs-server ./cmd/ojs-server

FROM alpine:3.19
RUN apk --no-cache add ca-certificates
COPY --from=builder /ojs-server /ojs-server

EXPOSE 8080
ENTRYPOINT ["/ojs-server"]
