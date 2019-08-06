VERSION := $(shell git describe --always --tags --dirty)
ldflags := "-X extendo.Version=${VERSION}"

.PHONY: build install lint test check clean

all: build

install:
	go install -ldflags ${ldflags}

build:
	go build -ldflags ${ldflags}

lint:
	golangci-lint run ./...

check: test

test:
	go test -coverprofile=coverage.out -race -v ./...
#	go tool cover -func=coverage.out

clean:
	go clean
