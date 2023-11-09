all: build

GO111MODULE=on

build:
	go build -o bin/go-mysqlbinlog cmd/go-mysqlbinlog/main.go
	go build -o bin/go-mysqldump cmd/go-mysqldump/main.go
	go build -o bin/go-canal cmd/go-canal/main.go
	go build -o bin/go-binlogparser cmd/go-binlogparser/main.go

test:
	go test --race -timeout 2m ./...

MYSQL_VERSION ?= 8.0
test-local:
	docker run --rm -d --network=host --name go-mysql-server \
		-e MYSQL_ALLOW_EMPTY_PASSWORD=true \
		-e MYSQL_DATABASE=test \
		-v $${PWD}/docker/resources/replication.cnf:/etc/mysql/conf.d/replication.cnf \
		mysql:$(MYSQL_VERSION)
	docker/resources/waitfor.sh 127.0.0.1 3306 \
		&& go test -race -v -timeout 2m ./... -gocheck.v
	docker stop go-mysql-server

fmt:
	golangci-lint run --fix

clean:
	go clean -i ./...
	@rm -rf ./bin
