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

Shell execution requires Linux and the `bubblewrap` command. Proceed refuses isolated shell runs when the sandbox is unavailable.

## Development checks

```sh
go test ./...
gofmt -l .
go vet ./...
```

All three must pass before submitting changes.
