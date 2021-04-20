all: build

GO111MODULE=on

build:
	go build -o bin/go-mysqlbinlog cmd/go-mysqlbinlog/main.go
	go build -o bin/go-mysqldump cmd/go-mysqldump/main.go
	go build -o bin/go-canal cmd/go-canal/main.go
	go build -o bin/go-binlogparser cmd/go-binlogparser/main.go

test:
	go test --race -timeout 2m ./...

fmt:
	golangci-lint run --fix

clean:
	go clean -i ./...
	@rm -rf ./bin