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
	ginkgo -r -race

coverage:
	ginkgo -r -cover -coverprofile=coverage.out

clean:
	go clean
