# proceed

Proceed is a standalone tool: a single binary that compiles
typed graph definitions (YAML), executes them durably, persists an append-only
event log with materialized runtime/evidence graphs, and answers causal queries
about what ran and why.

## Build

```sh
CGO_ENABLED=0 go build -o proceed ./cmd/proceed
```

## Usage

```sh
./proceed --help
```

## Development checks

```sh
go test ./...
gofmt -l .
go vet ./...
```

All three must pass before submitting changes.
