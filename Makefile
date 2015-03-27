all: build

build:
	export GOBIN=./bin && \
	godep go install ./...

clean:
	godep go clean -i ./...
	@rm -rf ./bin