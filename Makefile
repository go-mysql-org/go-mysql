all: build

GO111MODULE=on
MYSQL_VERSION ?= 8.0
MARIADB_VERSION ?= 11.8.8
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

MARIADB_CONTAINER ?= go-mariadb-server

mariadb-start:
	@if docker inspect $(MARIADB_CONTAINER) >/dev/null 2>&1; then \
		if [ "$$(docker inspect --format '{{.State.Running}}' $(MARIADB_CONTAINER))" = "true" ]; then \
			echo "MariaDB container $(MARIADB_CONTAINER) is already running"; \
		else \
			docker start $(MARIADB_CONTAINER) >/dev/null; \
		fi; \
	else \
		docker run --rm -d -p 3306:3306 --name $(MARIADB_CONTAINER) \
		-e MARIADB_ALLOW_EMPTY_ROOT_PASSWORD=1 \
		-e MARIADB_ROOT_HOST=% \
		-e MARIADB_DATABASE=test \
		-v "$${PWD}/docker/resources/ca.pem:/etc/mysql/ca.pem:ro" \
		-v "$${PWD}/docker/resources/server-cert.pem:/etc/mysql/server-cert.pem:ro" \
		-v "$${PWD}/docker/resources/server-key.pem:/etc/mysql/server-key.pem:ro" \
		mariadb:$(MARIADB_VERSION) \
			--server-id=1 --log-bin=mysql --binlog-format=row \
			--ssl-ca=/etc/mysql/ca.pem \
			--ssl-cert=/etc/mysql/server-cert.pem \
			--ssl-key=/etc/mysql/server-key.pem >/dev/null; \
	fi; \
	for attempt in $$(seq 1 60); do \
		if docker exec $(MARIADB_CONTAINER) mariadb-admin ping \
			-h127.0.0.1 -P3306 -uroot --silent >/dev/null 2>&1; then \
			echo "MariaDB is ready on 127.0.0.1:3306"; \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	docker logs $(MARIADB_CONTAINER); \
	exit 1

test-local-mariadb:
	@docker exec $(MARIADB_CONTAINER) mariadb-admin ping \
		-h127.0.0.1 -P3306 -uroot --silent >/dev/null 2>&1 || \
		{ echo "MariaDB is not running; run 'make mariadb-start' first"; exit 1; }
	MYSQL_FLAVOR=mariadb ${GO} test -race -v -timeout 2m ./...

mariadb-stop:
	@if docker inspect $(MARIADB_CONTAINER) >/dev/null 2>&1; then \
		docker stop $(MARIADB_CONTAINER) >/dev/null; \
	else \
		echo "MariaDB container $(MARIADB_CONTAINER) does not exist"; \
	fi

fmt:
	golangci-lint run --fix

clean:
	${GO} clean -i ./...
	@rm -rf ./bin
