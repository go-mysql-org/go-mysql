package mysql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
)

type mysqlDriver struct {
}

//dsn: <username>:<password>@<host>:<port>/<database>
func (d mysqlDriver) Open(dsn string) (driver.Conn, error) {
	user, password, addr, db, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}

	co := new(conn)

	if err := co.Connect(addr, user, password, db); err != nil {
		return nil, err
	}

	mc := &mysqlConn{c: co}

	return mc, nil
}

type mysqlConn struct {
	c *conn
}

func (c *mysqlConn) Prepare(query string) (driver.Stmt, error) {
	s, err := c.c.Prepare(query)
	if err != nil {
		return nil, err
	}

	st := &mysqlStmt{s: s}
	return st, nil
}

func (c *mysqlConn) Close() error {
	return c.c.Close()
}

func (c *mysqlConn) Begin() (driver.Tx, error) {
	if err := c.c.Begin(); err != nil {
		return nil, err
	}

	tx := &mysqlTx{c: c.c}

	return tx, nil
}

func (c *mysqlConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	as := make([]interface{}, len(args))

	for i := range as {
		as[i] = interface{}(args[i])
	}

	return c.c.Exec(query, as...)
}

func (c *mysqlConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	as := make([]interface{}, len(args))

	for i := range as {
		as[i] = interface{}(args[i])
	}
	r, err := c.c.Query(query, as...)
	if err != nil {
		return nil, err
	}

	rows := &mysqlRows{r: r}
	return rows, nil
}

type mysqlStmt struct {
	s *stmt
}

func (s *mysqlStmt) Close() error {
	return s.s.Close()
}

func (s *mysqlStmt) NumInput() int {
	return len(s.s.params)
}

func (s *mysqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	as := make([]interface{}, len(args))

	for i := range as {
		as[i] = interface{}(args[i])
	}

	return s.s.Exec(as...)
}

func (s *mysqlStmt) Query(args []driver.Value) (driver.Rows, error) {
	as := make([]interface{}, len(args))

	for i := range as {
		as[i] = interface{}(args[i])
	}

	r, err := s.s.Query(as...)
	if err != nil {
		return nil, err
	}

	rows := &mysqlRows{r: r, iter: 0}
	return rows, nil
}

type mysqlTx struct {
	c *conn
}

func (tx *mysqlTx) Commit() error {
	return tx.c.Commit()
}

func (tx *mysqlTx) Rollback() error {
	return tx.c.Rollback()
}

type mysqlRows struct {
	r    *Resultset
	iter int
}

func (r *mysqlRows) Columns() []string {
	cs := make([]string, len(r.r.Fields))

	for i := range cs {
		cs[i] = string(r.r.Fields[i].Name)
	}

	return cs
}

func (r *mysqlRows) Close() error {
	r.iter = -1
	return nil
}

func (r *mysqlRows) Next(dest []driver.Value) error {
	if r.iter >= r.r.RowNumber() {
		return io.EOF
	}

	data := r.r.Data[r.iter]

	if len(dest) != len(data) {
		return fmt.Errorf("invalid dest number %d != %d", len(dest), len(data))
	}

	for i := range dest {
		switch v := data[i].(type) {
		case int8:
			dest[i] = int64(v)
		case int16:
			dest[i] = int64(v)
		case int32:
			dest[i] = int64(v)
		case int:
			dest[i] = int64(v)
		case int64:
			dest[i] = int64(v)
		case uint8:
			dest[i] = int64(v)
		case uint16:
			dest[i] = int64(v)
		case uint32:
			dest[i] = int64(v)
		case uint:
			dest[i] = int64(v)
		case uint64:
			dest[i] = int64(v)
		case bool:
			dest[i] = v
		case float32:
			dest[i] = float64(v)
		case float64:
			dest[i] = float64(v)
		case string:
			dest[i] = []byte(v)
		case []byte:
			dest[i] = v
		default:
			return fmt.Errorf("invalid data type %T", data[i])
		}
	}

	r.iter++
	return nil
}

func init() {
	sql.Register("mysql", mysqlDriver{})
}
