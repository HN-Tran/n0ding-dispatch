# Contributing

n0ding Dispatch accepts focused fixes, tests, documentation, and reproducible
adapter evidence for the public preview. Open an issue before investing in a
large interface or scope change.

Run the complete local gate before opening a pull request:

```sh
go test ./...
go test -race ./...
go vet ./...
go build -trimpath ./cmd/n0ding-dispatch
mkdir -p bin
go build -trimpath -o bin/n0ding-dispatch ./cmd/n0ding-dispatch
sh scripts/package-smoke.sh
```

Do not commit runtime credentials, private task data, databases, or binaries.
Report vulnerabilities through [SECURITY.md](SECURITY.md), not a public issue.
Contributions are submitted under the repository's
[Apache-2.0 license](LICENSE).
