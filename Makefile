VERSION ?= dev
DSN ?= clickhouse://default@127.0.0.1:9000/default
COMPOSE ?= docker compose -f deploy/docker-compose.yml

.PHONY: build test integration vet fmt lint vuln helm-lint ci run docker demo down clean

build:
	go build -trimpath -ldflags="-s -w -X main.version=$(VERSION)" -o bin/clickhouse-exporter ./cmd/clickhouse-exporter

test:
	go test -race ./...

integration:
	CLICKHOUSE_DSN='$(DSN)' go test -tags=integration -v ./internal/collector

vet:
	go vet ./...

lint:
	staticcheck ./...
	golangci-lint run --timeout=5m

vuln:
	govulncheck ./...

helm-lint:
	docker run --rm -v "$(PWD)/deploy/helm:/charts" alpine/helm:latest lint --strict /charts/clickhouse-exporter \
		--set clickhouse.dsn='clickhouse://exporter@clickhouse:9000/default'

ci: test vet lint vuln helm-lint

fmt:
	gofmt -w .

run:
	go run ./cmd/clickhouse-exporter --clickhouse.dsn '$(DSN)'

docker:
	docker build --build-arg VERSION=$(VERSION) -t clickhouse-exporter:$(VERSION) .

demo:
	$(COMPOSE) up --build -d
	@echo "Grafana:    http://localhost:13000 (admin/admin)"
	@echo "Prometheus: http://localhost:9090"
	@echo "Exporter:   http://localhost:9116/metrics"

down:
	$(COMPOSE) down -v

clean:
	rm -rf bin
