# Makefile for go-gitpack.
#
# The default build uses the pure-Go flate backend. The libdeflate backend is
# gated behind the `gitpack_libdeflate` build tag and is NOT exercised by a
# plain `go test`, so `test-libdeflate` exists to run the suite against it (see
# zlib_cgo.go / zlib_cgo_test.go). Run `make check` for the full local gate.
#
# The test suite needs git fixture repositories under testdata/repos (gitignored,
# built by ./generate_testdata.sh). The test targets depend on `fixtures`, which
# generates them once if absent, so a fresh checkout (local or CI) is self-sufficient.

GO      ?= go
PKG     ?= .
TESTDATA := testdata/repos

.PHONY: all check build build-cross fixtures test test-race test-libdeflate cover vet fmt fmt-check mutate tidy

all: check

## check: fmt-check + vet + cross-compile + tests + race (the local pre-merge gate).
check: fmt-check vet build-cross test test-race

## fixtures: generate the git fixture repos the suite needs (idempotent; only
## runs generate_testdata.sh when testdata/repos is absent).
fixtures:
	@test -d $(TESTDATA) || ./generate_testdata.sh

## build: compile all packages.
build:
	$(GO) build $(PKG)/...

## build-cross: cross-compile smoke matrix pinning the DEFLATE build-tag algebra.
##
## The ARM64 assembly fast loop is gated on
## `arm64 && !purego && (!cgo || !gitpack_libdeflate)` (inflate_fast_arm64.go)
## with the complementary generic gate in inflate_fast_generic.go, so tag drift
## only surfaces on targets no single native runner compiles. The legs pin
## every arm of that expression reachable without cgo; all use CGO_ENABLED=0
## because cross-compilation disables cgo anyway, and the explicit setting
## keeps the legs reproducible on a native runner too (the linux/arm64 leg
## exercises the `!cgo` arm on the machine that would otherwise default to
## cgo). The one arm cross-compilation cannot reach — arm64 with cgo AND
## gitpack_libdeflate, where the tag alone must deselect the asm file — is
## pinned by the CI libdeflate job's ARM64 leg.
##
## `go build ./...` deliberately includes examples/ — they are plain Go with no
## cgo or platform constraints, so they must cross-compile as well.
##
## Cross-vet runs only for darwin/arm64 (asm-active leg: vets the wrapper and
## its unsafe conversions against a non-linux target) and linux/amd64
## (generic-path leg). Vetting every leg would roughly double the cost of the
## target for no additional tag coverage; the remaining legs share one of those
## two code paths.
build-cross:
	CGO_ENABLED=0 GOOS=darwin  GOARCH=arm64 $(GO) build $(PKG)/...
	CGO_ENABLED=0 GOOS=windows GOARCH=arm64 $(GO) build $(PKG)/...
	CGO_ENABLED=0 GOOS=linux   GOARCH=amd64 $(GO) build $(PKG)/...
	CGO_ENABLED=0 GOOS=linux   GOARCH=arm64 $(GO) build -tags purego $(PKG)/...
	CGO_ENABLED=0 GOOS=linux   GOARCH=arm64 $(GO) build $(PKG)/...
	CGO_ENABLED=0 GOOS=darwin  GOARCH=arm64 $(GO) vet $(PKG)/...
	CGO_ENABLED=0 GOOS=linux   GOARCH=amd64 $(GO) vet $(PKG)/...

## test: run the full suite against the default pure-Go backend.
test: fixtures
	$(GO) test -count=1 $(PKG)/...

## test-race: run the suite under the data-race detector (the standing gate for
## the concurrent read path and the parallel commit walk).
test-race: fixtures
	$(GO) test -race -count=1 $(PKG)/...

## test-libdeflate: run the suite against the cgo libdeflate backend. Requires
## libdeflate headers + library (Debian/Ubuntu: `apt-get install libdeflate-dev`).
## The cgo preamble in zlib_cgo.go declares `-ldeflate`; override CGO_CFLAGS /
## CGO_LDFLAGS in the environment for a libdeflate in a non-standard location.
test-libdeflate: fixtures
	CGO_ENABLED=1 $(GO) test -tags gitpack_libdeflate -count=1 $(PKG)/...

## cover: write a coverage profile and print the per-function summary.
cover: fixtures
	$(GO) test -coverprofile=coverage.out -count=1 $(PKG)/... >/dev/null
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

## linkaudit-external: prove the amd64 inflate kernel survives internal vs
## external linking with byte-identical instructions (the fast-loop PR claim;
## the per-instruction register/ISA audit lives in
## inflate_fast_amd64_linkaudit_test.go, which cannot afford two full builds).
## Builds the test binary under both link modes and diffs `go tool objdump`
## of the kernel symbol on the (file:line, opcode-bytes) columns — absolute
## addresses shift between link modes, but relative displacements live in the
## opcode bytes, so equal bytes mean identical instructions. Skips when the
## effective GOARCH is not amd64 or when no C toolchain is available
## (-linkmode=external needs one).
.PHONY: linkaudit-external
linkaudit-external:
	@set -e; \
	if [ "$$($(GO) env GOARCH)" != "amd64" ]; then \
		echo "linkaudit-external: skipped (GOARCH=$$($(GO) env GOARCH), needs amd64)"; exit 0; \
	fi; \
	cc="$$($(GO) env CC)"; \
	if ! command -v "$$cc" >/dev/null 2>&1; then \
		echo "linkaudit-external: skipped (no C toolchain '$$cc' for -linkmode=external)"; exit 0; \
	fi; \
	tmp="$$(mktemp -d)"; trap 'rm -rf "$$tmp"' EXIT; \
	$(GO) test -c -vet=off -o "$$tmp/int.test" $(PKG); \
	CGO_ENABLED=1 $(GO) test -c -vet=off -ldflags '-linkmode=external' -o "$$tmp/ext.test" $(PKG); \
	for m in int ext; do \
		$(GO) tool objdump -s 'inflateHuffmanFastAMD64(\.abi0)?$$' "$$tmp/$$m.test" \
		| awk -F'\t' 'NR>1 { n=0; loc=""; hex=""; \
			for (i=1; i<=NF; i++) if ($$i != "") { n++; if (n==1) loc=$$i; if (n==3) hex=$$i }; \
			if (loc == "" || hex !~ /^[0-9a-fA-F]+$$/) { \
				printf "linkaudit-external: unexpected objdump row (no hex-bytes column): %s\n", $$0 > "/dev/stderr"; \
				exit 1 }; \
			print loc "\t" hex }' > "$$tmp/$$m.norm" \
		|| { echo "linkaudit-external: objdump output format changed; update the awk parser"; exit 1; }; \
	done; \
	[ -s "$$tmp/int.norm" ] || { echo "linkaudit-external: kernel symbol missing from internal-link binary"; exit 1; }; \
	diff -u "$$tmp/int.norm" "$$tmp/ext.norm"; \
	echo "linkaudit-external: kernel byte-identical across link modes ($$(wc -l < "$$tmp/int.norm") instructions)"
