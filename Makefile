.PHONY: build run test lint clean conformance conformance-all docker-up docker-down dev docker-dev

CONFORMANCE_RUNNER = ../ojs-conformance/runner/http
CONFORMANCE_SUITES = ../../suites
OJS_URL ?= http://localhost:8080
KAFKA_BROKERS ?= localhost:9092
REDIS_URL ?= redis://localhost:6379

build:
	go build -o bin/ojs-server ./cmd/ojs-server

run: build
	OJS_ALLOW_INSECURE_NO_AUTH=true KAFKA_BROKERS=$(KAFKA_BROKERS) REDIS_URL=$(REDIS_URL) ./bin/ojs-server

test:
	go test ./... -race -cover

lint:
	go vet ./...

clean:
	rm -rf bin/

# Docker
docker-up:
	docker compose -f docker/docker-compose.yml up --build -d

docker-down:
	docker compose -f docker/docker-compose.yml down

# Conformance tests (require running server + Kafka + Redis)
conformance: conformance-all

conformance-all:
	cd $(CONFORMANCE_RUNNER) && go run . -url $(OJS_URL) -suites $(CONFORMANCE_SUITES) -redis $(REDIS_URL)

conformance-level-0:
	cd $(CONFORMANCE_RUNNER) && go run . -url $(OJS_URL) -suites $(CONFORMANCE_SUITES) -level 0 -redis $(REDIS_URL)

conformance-level-1:
	cd $(CONFORMANCE_RUNNER) && go run . -url $(OJS_URL) -suites $(CONFORMANCE_SUITES) -level 1 -redis $(REDIS_URL)

conformance-level-2:
	cd $(CONFORMANCE_RUNNER) && go run . -url $(OJS_URL) -suites $(CONFORMANCE_SUITES) -level 2 -redis $(REDIS_URL)

conformance-level-3:
	cd $(CONFORMANCE_RUNNER) && go run . -url $(OJS_URL) -suites $(CONFORMANCE_SUITES) -level 3 -redis $(REDIS_URL)

conformance-level-4:
	cd $(CONFORMANCE_RUNNER) && go run . -url $(OJS_URL) -suites $(CONFORMANCE_SUITES) -level 4 -redis $(REDIS_URL)

# Development with hot reload
dev:
	air -c .air.toml

docker-dev:
	docker compose -f docker/docker-compose.yml --profile dev up --build
