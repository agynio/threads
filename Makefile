SHELL := /bin/bash

PROTO_PATHS := agynio/api/threads/v1 agynio/api/notifications/v1 agynio/api/identity/v1 agynio/api/authorization/v1

.PHONY: all proto build test lint fmt clean

all: build

proto:
	buf generate buf.build/agynio/api $(foreach p,$(PROTO_PATHS),--path $(p)) --include-imports

build:
	GOFLAGS=-mod=mod go build ./...

test:
	GOFLAGS=-mod=mod go test ./...

lint:
	GOFLAGS=-mod=mod go vet ./...

fmt:
	gofmt -w $(shell find . -type f -name '*.go')

clean:
	rm -rf gen
