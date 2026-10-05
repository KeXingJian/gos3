BINARY   := gos3
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT   ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILDTIME ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
PKG      := github.com/kxj/gos3/internal/version
LDFLAGS  := -X $(PKG).Version=$(VERSION) -X $(PKG).Commit=$(COMMIT) -X $(PKG).BuildTime=$(BUILDTIME)

.PHONY: all build test fmt vet tidy run clean verify-docker verify-dist proto

all: build

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o $(BINARY) .

test:
	go test ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

tidy:
	go mod tidy

run: build
	./$(BINARY) server /tmp/gos3-data

verify-docker:
	docker compose down -v
	docker compose build
	docker compose up -d --wait gos3
	docker compose run --rm verify
	@docker compose logs gos3 2>&1 | grep -q 'HTTP GET' && echo "PASS otel: HTTP span exported" || (echo "FAIL otel: no HTTP span"; docker compose logs gos3 2>&1 | tail -30; exit 1)
	docker compose down -v

verify-dist:
	docker compose -f docker-compose.dist.yml down -v
	docker compose -f docker-compose.dist.yml build
	docker compose -f docker-compose.dist.yml up -d --wait node1 node2
	docker compose -f docker-compose.dist.yml run --rm --no-deps -e PHASE=write verify
	@docker compose -f docker-compose.dist.yml logs node1 node2 2>&1 | grep -q 'DiskService' && echo "PASS otel: gRPC span exported" || (echo "FAIL otel: no gRPC span"; exit 1)
	docker compose -f docker-compose.dist.yml stop node1
	docker compose -f docker-compose.dist.yml run --rm --no-deps -e PHASE=read -e S3_ENDPOINT=http://node2:9000 verify
	docker compose -f docker-compose.dist.yml down -v

proto:
	docker run --rm -v $(PWD):/work -w /work -e GOSUMDB=off -e GOFLAGS=-mod=mod -e GOPROXY=https://goproxy.cn,direct golang:1.24-alpine sh -c '\
		apk add --no-cache protobuf protobuf-dev >/dev/null 2>&1 && \
		go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.5 && \
		go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1 && \
		export PATH=$$PATH:/root/go/bin && \
		protoc -I. --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative internal/disk/disk.proto'

clean:
	rm -f $(BINARY)
