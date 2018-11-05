FROM golang:alpine

MAINTAINER atohutchful

RUN apk add --no-cache tini mariadb-client

ADD . /go/src/github.com/siddontang/go-mysql

RUN apk add --no-cache mariadb-client

RUN cd /go/src/github.com/siddontang/go-mysql/ && \
    go build -o bin/go-mysqldump ./cmd/go-mysqldump && \
    cp -f ./bin/go-mysqldump /go/bin/go-mysqldump

ENTRYPOINT ["/sbin/tini","--","go-mysqldump"]