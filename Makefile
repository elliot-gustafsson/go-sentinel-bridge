.PHONY:	build
SHELL := /bin/bash
MAKEFLAGS += --no-print-directory

build:
	CGO_ENABLED=0 GOOS=linux go build

localrun:
	go run .

test:
	go test ./... -count=1 -cover

container:
ifndef TAG
	$(error TAG is required. Use: make container TAG=<tag>)
endif
	$(shell command -v podman &>/dev/null && echo "podman" || echo "docker") build -t $(TAG) -f Containerfile .
