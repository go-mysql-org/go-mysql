ROOT_PATH=$(shell pwd)

all: build

build:
	export GOBIN=${ROOT_PATH}/bin && \
	go install ./...

test:
	export GOBIN=${ROOT_PATH}/bin && \
	go test --race -timeout 2m ./...

clean:
	go clean -i ./...
	@rm -rf ./bin

update_vendor:
	which glide >/dev/null || curl https://glide.sh/get | sh
	which glide-vc || go get -v -u github.com/sgotti/glide-vc
	glide --verbose update --strip-vendor --skip-test
	@echo "removing test files"
	glide vc --only-code --no-tests
