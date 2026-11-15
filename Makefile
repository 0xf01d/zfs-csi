GO ?= go
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
IMAGE ?= ghcr.io/0xf01d/zfs-csi
TAG ?= $(VERSION)
GOFLAGS ?=

.PHONY: all build test vet fmt image csi-sanity e2e measure

all: fmt vet test build

build:
	mkdir -p bin
	CGO_ENABLED=0 $(GO) build $(GOFLAGS) -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o bin/zfs-csi ./cmd/zfs-csi

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './vendor/*')

image:
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE):$(TAG) -t $(IMAGE):latest .

# Requires a real ZFS node. See README.
csi-sanity: build
	./hack/csi-sanity.sh

# Requires ZFS and either root (temporary file pools) or ZFS_CSI_E2E_POOL_A/B.
e2e:
	ZFS_CSI_E2E=1 $(GO) test -v ./test/e2e

measure: build
	./hack/measure-idle.sh
