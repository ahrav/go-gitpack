# Makefile for go-gitpack.
#
# The default build uses the pure-Go flate backend. The libdeflate backend is
# gated behind the `gitpack_libdeflate` build tag and is NOT exercised by a
# plain `go test`, so `test-libdeflate` exists to run the suite against it (see
# zlib_cgo.go / zlib_cgo_test.go). Run `make check` for the full local gate.

GO      ?= go
PKG     ?= .
# libdeflate-dev installs the header and shared library into the default search
# paths on Debian/Ubuntu, so -ldeflate is sufficient. Override for a custom
# build, e.g. CGO_LDFLAGS="/path/libdeflate.a".
CGO_LDFLAGS ?= -ldeflate

.PHONY: all check test test-race test-libdeflate cover vet fmt fmt-check mutate tidy

all: check

## check: fmt-check + vet + tests + race (the local pre-merge gate).
check: fmt-check vet test test-race

## test: run the full suite against the default pure-Go backend.
test:
	$(GO) test -count=1 $(PKG)/...

## test-race: run the suite under the data-race detector (the standing gate for
## the concurrent read path and the parallel commit walk).
test-race:
	$(GO) test -race -count=1 $(PKG)/...

## test-libdeflate: run the suite against the cgo libdeflate backend. Requires
## libdeflate headers + library (Debian/Ubuntu: `apt-get install libdeflate-dev`).
test-libdeflate:
	CGO_ENABLED=1 CGO_LDFLAGS="$(CGO_LDFLAGS)" \
		$(GO) test -tags gitpack_libdeflate -count=1 $(PKG)/...

## cover: write a coverage profile and print the per-function summary.
cover:
	$(GO) test -coverprofile=coverage.out -count=1 $(PKG) >/dev/null
	$(GO) tool cover -func=coverage.out | tail -1
	@echo "open coverage: $(GO) tool cover -html=coverage.out"

## vet: run go vet over all packages.
vet:
	$(GO) vet $(PKG)/...

## fmt: format all Go sources in place.
fmt:
	gofmt -w .

## fmt-check: fail if any Go source is not gofmt-clean.
fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

## mutate: mutation-test the optimization-critical files with gremlins to
## confirm the differential/fuzz oracles actually kill behavioral changes.
## Install once: go install github.com/go-gremlins/gremlins/cmd/gremlins@latest
mutate:
	gremlins unleash --tags="" $(PKG)

## tidy: verify go.mod/go.sum are tidy.
tidy:
	$(GO) mod tidy
