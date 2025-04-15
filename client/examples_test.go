package client_test

import (
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
)

var (
	conn *client.Conn
)

func ExampleConn_ExecuteMultiple() {
	queries := "SELECT 1; SELECT NOW();"
	conn.ExecuteMultiple(queries, func(result *mysql.Result, err error) {
		// Use the result as you want
	})
}

func ExampleConn_ExecuteSelectStreaming() {
	var result mysql.Result
	conn.ExecuteSelectStreaming(`SELECT ... LIMIT 100500`, &result, func(row []mysql.FieldValue) error {
		// Use the row as you want.
		// You must not save FieldValue.AsString() value after this callback is done. Copy it if you need.
		return nil
	}, nil)
}
