all: test

test:
	go test -mod=mod -v ./...
