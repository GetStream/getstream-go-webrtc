.PHONY: build
build:
	@go build ./...

.PHONY: generate
generate:
	./generate.sh

.PHONY: lint
lint:
	./lint.sh

.PHONY: fmt
fmt:
	@go run mvdan.cc/gofumpt@latest -l -w .

# Same check the lint workflow runs: report, do not rewrite.
.PHONY: fmt-check
fmt-check:
	@out=$$(go run mvdan.cc/gofumpt@latest -l .); \
		if [ -n "$$out" ]; then echo 'ERROR: not gofumpt-formatted:'; echo "$$out"; exit 1; fi

.PHONY: vet
vet:
	@go vet ./...

.PHONY: tidy
tidy:
	@go mod tidy

.PHONY: test
test:
	@go test ./...

.PHONY: test-race
test-race:
	@go test -race -timeout 10m ./...

# go test -cover shells out to the covdata tool for packages with no test files.
# When the local go command is older than the go line in go.mod it switches to a
# downloaded toolchain module, and those ship no covdata binary while the older
# go command does not know how to build one, so the run fails with
# `no such tool "covdata"` (golang/go#75031, fixed in later go1.25 patches).
# Beginning the decision at the go.mod toolchain avoids it; override this if you
# have a newer one installed.
COVER_GOTOOLCHAIN ?= go1.27.1+auto

.PHONY: cover
cover:
	@GOTOOLCHAIN=$(COVER_GOTOOLCHAIN) go test -coverprofile cover.out ./...
	@go tool cover -func=cover.out

# This module must never pull in GetStream/kit or GetStream/video-sfu.
.PHONY: check-no-private-deps
check-no-private-deps:
	@! go list -deps ./... | grep -E 'GetStream/(kit|video-sfu)' || \
		{ echo 'ERROR: private GetStream dependency reached from this module'; exit 1; }

.PHONY: ci
ci: build vet fmt-check test-race check-no-private-deps
