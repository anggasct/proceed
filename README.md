# proceed

A single binary that compiles typed graph definitions (YAML), runs them
durably, and records every execution decision to an append-only event log.

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
