ROOT_PATH=$(shell pwd)

all: build

build:
	export GOBIN=${ROOT_PATH}/bin && \
	go install ./...

test: build
	export GOBIN=${ROOT_PATH}/bin && \
	go test --race -timeout 2m ./...

clean:
	go clean -i ./...
	@rm -rf ./bin
