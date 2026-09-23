#!/usr/bin/env bash
#
# Regenerates coordinator/models from the public GetStream/protocol OpenAPI
# clientside spec. The Go mirror of stream-video-js's generate-openapi.sh:
# models only, no API client -- coordinator/client.go is hand-written.
#
# Usage:
#   ./generate.sh                    # download the spec from GitHub
#   ./generate.sh path/to/spec.yaml  # use a local spec
set -euo pipefail

cd "$(dirname "$0")"

SPEC_URL="https://raw.githubusercontent.com/GetStream/protocol/main/openapi/video-openapi-clientside.yaml"
SPEC_FILE="${1:-}"

if [ -z "$SPEC_FILE" ]; then
  SPEC_FILE=$(mktemp -t video-clientside.XXXX.yaml)
  trap 'rm -f "$SPEC_FILE"' EXIT
  curl -fsSL "$SPEC_URL" -o "$SPEC_FILE"
fi

mkdir -p coordinator/models

go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.8.0 \
  -config oapi-codegen.yaml \
  "$SPEC_FILE"

# The spec's date-time fields become time.Time, which cannot decode the epoch
# nanoseconds the coordinator actually sends. models.Time (coordinator/models/time.go)
# reads both that and RFC 3339; the spec has no x-go-type to say so, so it is swapped in
# here. Nothing else in the generated file needs the time package.
sed -i '' \
  -e 's/time\.Time/Time/g' \
  -e '/^\t"time"$/d' \
  coordinator/models/models.gen.go

# oapi-codegen renders websocket events as the opaque VideoEvent union and emits
# no methods, so the WebsocketEvent implementations, the event handler and the
# call-event constraints are derived from it here. The same pass derives
# signal.Events and the HandleCallEvent dispatch from the SfuEvent protobuf
# oneof, so no switch over either event family is hand-maintained.
go run ./internal/cmd/genwsevent

# Test doubles for the coordinator client. moq is pinned so the output is
# reproducible.
go generate ./coordinator/...

./lint.sh
