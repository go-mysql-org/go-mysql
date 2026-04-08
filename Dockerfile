FROM golang:1.24

USER root

COPY . /go-mysql/

WORKDIR /go-mysql/cmd/go-mysqlserver/

EXPOSE 4000

ENTRYPOINT ["go", "run", "main.go"]