all: build

build:
	export GOBIN=./bin && \
	godep go install ./...

test: build
	export GOBIN=./bin && \
	godep go test ./...

clean:
	godep go clean -i ./...
	@rm -rf ./bin
