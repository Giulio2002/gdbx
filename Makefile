.PHONY: build test lint lint-install clean

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOVET=$(GOCMD) vet
GOLANGCI_LINT=golangci-lint

# Fix GOPATH if it equals GOROOT
GOPATH_FIX := $(shell if [ "$$(go env GOPATH)" = "$$(go env GOROOT)" ]; then echo "GOPATH=/tmp/gopath"; fi)

# Build the library
build:
	$(GOBUILD) -v ./...

# Run tests
test:
	$(GOTEST) -v ./tests/... -timeout 120s

# Run tests with race detector
test-race:
	$(GOTEST) -v -race ./tests/... -timeout 300s

# Run short tests only
test-short:
	$(GOTEST) -v -short ./tests/... -timeout 60s

# Install golangci-lint if not present
lint-install:
	@which $(GOLANGCI_LINT) > /dev/null || (echo "Installing golangci-lint..." && \
		curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $$(go env GOPATH)/bin v1.61.0)

# Run linter
lint: lint-install
	$(GOPATH_FIX) $(GOLANGCI_LINT) run ./...

# Run linter with auto-fix
lint-fix: lint-install
	$(GOPATH_FIX) $(GOLANGCI_LINT) run --fix ./...

# Run go vet
vet:
	$(GOVET) ./...

# Clean build artifacts
clean:
	$(GOCMD) clean
	rm -f coverage.out

# Run benchmarks
bench:
	$(GOTEST) -bench=. -benchmem ./tests/... -timeout 600s

# Generate coverage report
coverage:
	$(GOTEST) -coverprofile=coverage.out ./tests/...
	$(GOCMD) tool cover -html=coverage.out

# Run all checks (build, vet, lint, test)
all: build vet lint test
