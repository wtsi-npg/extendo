VERSION := $(shell git describe --always --tags --dirty)
ldflags := "-X extendo.Version=${VERSION}"
build_args := -a -v -ldflags ${ldflags}

CGO_ENABLED?=${CGO_ENABLED}

.PHONY: build install lint test check clean

all: build

install:
	go install ${build_args}

build: install
	GOOS=linux GOARCH=amd64 go build ${build_args}

lint:
	golangci-lint run ./...

check: test

test: build
	GOOS=linux GOARCH=amd64 ginkgo -r --race

coverage:
	GOOS=linux GOARCH=amd64 ginkgo -r --cover -coverprofile=coverage.out

clean:
	go clean
