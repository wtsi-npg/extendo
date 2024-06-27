VERSION := $(shell git describe --always --tags --dirty)
ldflags := "-X extendo.Version=${VERSION}"

.PHONY: build install lint test check clean

all: build

install:
	go install -ldflags ${ldflags}

build: install
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -ldflags ${ldflags}

lint:
	golangci-lint run ./...

check: test

test: build
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 ginkgo -r --race

coverage:
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 ginkgo -r -cover -coverprofile=coverage.out

clean:
	go clean
