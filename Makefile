.PHONY: test vet build run

test:
	go test ./...

build:
	go build -o bin/n0ding-dispatch ./cmd/n0ding-dispatch

vet:
	go vet ./...

run:
	go run ./cmd/n0ding-dispatch serve --db dispatch.db
