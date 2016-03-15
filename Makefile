ROOT_PATH=$(shell pwd)

all: build

build:
	export GOBIN=${ROOT_PATH}/bin && \
	godep go install ./...

test: build
	export GOBIN=${ROOT_PATH}/bin && \
	godep go test ./...

clean:
	godep go clean -i ./...
	@rm -rf ./bin
