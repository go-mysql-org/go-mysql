// This package implements database/sql/driver interface,
// so we can use go-mysql with database/sql
package driver

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
)

type Driver struct {
}

// DSN user:password@addr[?db]
func (d Driver) Open(dsn string) (driver.Conn, error) {
	seps := strings.Split(dsn, "@")
	if len(seps) != 2 {
		return nil, fmt.Errorf("invalid dsn, must user:password@addr[?db]")
	}

	var user string
	var password string
	var addr string
	var db string

	if ss := strings.Split(seps[0], ":"); len(ss) == 2 {
		user, password = ss[0], ss[1]
	} else if len(ss) == 1 {
		user = ss[0]
	} else {
		return nil, fmt.Errorf("invalid dsn, must user:password@addr[?db]")
	}

	if ss := strings.Split(seps[1], "?"); len(ss) == 2 {
		addr, db = ss[0], ss[1]
	} else if len(ss) == 1 {
		addr = ss[0]
	} else {
		return nil, fmt.Errorf("invalid dsn, must user:password@addr[?db]")
	}

	conn, err := client.Connect(addr, user, password, db)
	if err != nil {
		return nil, err
	}

	return &Conn{conn}, nil
}

type Conn struct {
	*client.Conn
}

func (c *Conn) Prepare(stmt string) (driver.Stmt, error) {
	return nil, nil
}

func (c *Conn) Close() error {
	return nil
}

func (c *Conn) Begin() (driver.Tx, error) {

}

func (c *Conn) Exec(args []driver.Value) (driver.Result, error) {
	return nil, nil
}

func (c *Conn) Query(args []driver.Value) (driver.Rows, error) {
	return nil, nil
}

type Stmt struct {
	*client.Stmt
}

func (s *Stmt) Close() error {
	return nil
}

func (s *Stmt) NumInput() int {
	return 0
}

func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, nil
}

func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, nil
}

type Tx struct {
	*client.Conn
}

func (t *Tx) Commit() error {
	return nil
}

func (t *Tx) Rollback() error {
	return nil
}

type Result struct {
	*mysql.Result
}

func (r *Result) LastInsertId() (int64, error) {
	return 0, nil
}

func (r *Result) RowsAffected() (int64, error) {
	return 0, nil
}

type Rows struct {
	*mysql.Resultset
}

func (r *Rows) Columns() []string {
	return nil
}

func (r *Rows) Close() error {
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	return nil
}

func init() {
	sql.Register("mysql", Driver{})
}
