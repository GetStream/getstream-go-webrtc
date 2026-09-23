#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

go fmt ./...
go vet ./...
go run mvdan.cc/gofumpt@latest -l -w .
