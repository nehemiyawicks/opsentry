BIN     := bin/opsentry
GO      ?= go
VERSION ?= dev
COMMIT  := $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
LDFLAGS := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT)

.PHONY: all build run test race vet fmt tidy lint clean docker compose-up compose-down

all: build

build:
	$(GO) build -trimpath -ldflags="$(LDFLAGS)" -o $(BIN) ./cmd/opsentry

run: build
	./$(BIN) -config=config.example.yaml

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

tidy:
	$(GO) mod tidy

lint:
	golangci-lint run

clean:
	rm -rf bin dist

docker:
	docker build --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) -t opsentry:$(VERSION) .

compose-up:
	docker compose -f deploy/docker-compose.yaml up --build -d

compose-down:
	docker compose -f deploy/docker-compose.yaml down -v
