all: build

GO111MODULE=on
MYSQL_VERSION ?= 8.0
GO ?= go

build:
	${GO} build -o bin/go-mysqlbinlog cmd/go-mysqlbinlog/main.go
	${GO} build -o bin/go-mysqldump cmd/go-mysqldump/main.go
	${GO} build -o bin/go-canal cmd/go-canal/main.go
	${GO} build -o bin/go-binlogparser cmd/go-binlogparser/main.go
	${GO} build -o bin/go-mysqlserver cmd/go-mysqlserver/main.go

test:
	${GO} test --race -timeout 2m ./...

test-local:
	docker run --rm -d --network=host --name go-mysql-server \
		-e MYSQL_ALLOW_EMPTY_PASSWORD=true \
		-e MYSQL_DATABASE=test \
		-v $${PWD}/docker/resources/replication.cnf:/etc/mysql/conf.d/replication.cnf \
		mysql:$(MYSQL_VERSION)
	docker/resources/waitfor.sh 127.0.0.1 3306 \
		&& ${GO} test -race -v -timeout 2m ./...
	docker stop go-mysql-server

fmt:
	golangci-lint run --fix

clean:
	${GO} clean -i ./...
	@rm -rf ./bin
