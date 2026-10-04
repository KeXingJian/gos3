BINARY   := gos3
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT   ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILDTIME ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
PKG      := github.com/kxj/gos3/internal/version
LDFLAGS  := -X $(PKG).Version=$(VERSION) -X $(PKG).Commit=$(COMMIT) -X $(PKG).BuildTime=$(BUILDTIME)

.PHONY: all build test fmt vet tidy run clean verify-docker

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
	docker compose down -v

clean:
	rm -f $(BINARY)
