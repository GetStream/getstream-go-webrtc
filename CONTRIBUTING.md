# Contributing

Contributions are welcome. Please make sure your changes are tested and follow Go best practices.

## Getting started

You need Go 1.25 or later. Everything else is fetched by the toolchain:

```bash
make build
make ci      # what to run before pushing: build, vet, gofumpt check, race tests, dep guard
```

```bash
make build                    # go build ./...
make test                     # go test ./...
make test-race                # go test -race -timeout 10m ./...
make lint                     # go fmt, go vet, gofumpt -w (rewrites files)
make fmt-check                # gofumpt -l, report only — what CI runs
make vet                      # go vet ./...
make tidy                     # go mod tidy
make generate                 # the three-stage codegen pipeline
make check-no-private-deps    # the dependency guard
make ci                       # build + vet + fmt-check + test-race + dep guard
```

The whole suite runs with **no credentials and no network**: peer-connection tests use loopback
connections, coordinator tests use `httptest`, and no test reads an environment variable. If you add
a test that needs a live Stream app, gate it on an environment variable and `t.Skip` when it is
unset — and say in the skip message what would unblock it.

`make cover` needs a Go toolchain that ships the prebuilt `covdata` tool. If you see
`go: no such tool "covdata"`, your toolchain install is incomplete (it happens with toolchains
fetched by `go`'s automatic version switching); run `go test -coverprofile` against the packages that
have tests instead of `./...`.

Every code sample in the README is also a function in `internal/readmecheck`, so `make build` fails
if the docs drift from the API. Change a sample and change it there too.

The module builds with `CGO_ENABLED=0`.

## Package map

| Path | What it is |
| --- | --- |
| `.` (`rtc`) | `Client`, `Call`, publisher, subscriber, media engine, location discovery, stats, event fan-out. `call.go` is the most important file. |
| `coordinator/` | Coordinator REST + event websocket. Exactly one endpoint is called: `POST /api/v2/video/call/{type}/{id}/join`. |
| `coordinator/models/` | Generated from the public OpenAPI spec. Do not hand-edit. |
| `signal/` | The SFU signalling client: the protobuf websocket plus the twirp `SignalServer` RPCs. |
| `pc/` | `pc.Transport`, the peer-connection wrapper. |
| `track/` | Local tracks driven by a `SampleProvider`, simulcast layers, RED transcoding for audio. |
| `audio/` | `audio.PCM`, resampling, chunking, WAV, G.711. |
| `audio/opus/` | Pure-Go Opus encode and decode. |
| `audio/rtc/` | `TrackWriter` publishes PCM; `TrackReader` turns a remote track into PCM. |
| `websocket/` | Generic typed websocket connection, used by both `coordinator` and `signal`. |
| `event/` | Small pub/sub store plus a one-shot awaiter. |
| `rtcstats/` | Trace buffer for the client stats the SFU ingests. Its output format is wire protocol. |
| `interceptor/` | RTX prober for the SFU's RTX SSRC mapping. |
| `logger/` | `ILogger` interface, `Noop` default, logrus adapter, pion `LeveledLogger` bridge. |
| `internal/` | Internal helpers, `FakeSFU`, the `cmd/genwsevent` generator, and `readmecheck`. |

```bash
go doc .                    # Client, Call, Option, JoinOption, the event helpers
go doc ./pc ./signal ./track
go doc ./audio ./audio/opus ./audio/rtc
go doc Call.Join
```

## Dependency guard

This module must build for anyone with a stock Go toolchain and no extra credentials, so it may
never depend on private GetStream packages (`GetStream/kit`, `GetStream/video-sfu`). This is
enforced by CI and by `TestNoPrivateDependencies`:

```bash
make check-no-private-deps
```

If you need something from one of those modules, port the used subset into `internal/` rather than
adding the dependency.

## Generated code

Five files are generated. Do not hand-edit any of them; they all carry a `DO NOT EDIT` header.

| File | Generator |
| --- | --- |
| `coordinator/models/models.gen.go` | `oapi-codegen`, config in `oapi-codegen.yaml` |
| `coordinator/models/wsevent_types.gen.go` | `internal/cmd/genwsevent` |
| `coordinator/events.gen.go` | `internal/cmd/genwsevent` |
| `coordinator/handler.gen.go` | `internal/cmd/genwsevent` |
| `coordinator/mocks/mock_coordinator_client.go` | `moq`, via `go generate ./coordinator/...` |

To change the models, change the upstream spec in
[`GetStream/protocol`](https://github.com/GetStream/protocol) or the settings in
`oapi-codegen.yaml`, then:

```bash
make generate                    # fetches the spec from GitHub
./generate.sh path/to/spec.yaml  # or use a local spec
```

The pipeline is reproducible: running it on a clean tree must leave the tree clean. If it does not,
either the spec moved or you changed a generator — say which in the commit message.

`oapi-codegen.yaml` has two settings that look removable and are not:

- **`skip-prune: true`.** Four schemas the SDK needs — `WSAuthMessage`,
  `ConnectUserDetailsRequest`, `ConnectedEvent`, `OwnUserResponse` — are reachable only from the
  websocket, not from any HTTP path, so the default pruning pass silently drops all four.
- **`name-normalizer: ToCamelCaseWithInitialisms`.** Produces `ID`, `UserID`, `SessionID`, `APIKey`
  instead of `Id`, `UserId`, `SessionId`, `ApiKey`.

Hand-written code that complements the generated models — `coordinator/client.go`,
`coordinator/websocket.go`, `coordinator/models/wsevent.go` — is ordinary code and fair game to edit.

## Code formatting & linter

Formatting is enforced with [`gofumpt`](https://github.com/mvdan/gofumpt), a stricter `gofmt`. Run
`make lint` before pushing (it rewrites files) or `make fmt-check` to only report. For VS Code:

```json
{
    "editor.formatOnSave": true,
    "gopls": {
        "formatting.gofumpt": true
    }
}
```

## Commit message convention

We follow [conventional commits](https://www.conventionalcommits.org/), e.g.
`feat: add fast reconnect to the SFU signalling client` or `fix: drop stale ICE candidates on migrate`.

## Release (for Stream developers)

Open a pull request from a branch named `release-<version>` (for example `release-0.2.0`). Merging it
tags the commit and creates the GitHub release automatically.
