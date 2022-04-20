// This package implements database/sql/driver interface,
// so we can use go-mysql with database/sql
package driver

import (
	"crypto/tls"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
	"github.com/siddontang/go/hack"
)

// Map of dsn address (makes more sense than full dsn?) to tls Config
var customTLSConfigMap = make(map[string]*tls.Config)

type driver struct {
}

// Open: DSN user:password@addr[?db]
func (d driver) Open(dsn string) (sqldriver.Conn, error) {
	lastIndex := strings.LastIndex(dsn, "@")
	seps := []string{dsn[:lastIndex], dsn[lastIndex+1:]}
	if len(seps) != 2 {
		return nil, errors.Errorf("invalid dsn, must user:password@addr[[?db[&param=X]]")
	}

	var user string
	var password string
	var addr string
	var db string
	var err error
	var c *client.Conn

	if ss := strings.Split(seps[0], ":"); len(ss) == 2 {
		user, password = ss[0], ss[1]
	} else if len(ss) == 1 {
		user = ss[0]
	} else {
		return nil, errors.Errorf("invalid dsn, must user:password@addr[[?db[&param=X]]")
	}

	params := make(map[string]string)
	if ss := strings.Split(seps[1], "?"); len(ss) == 2 {
		// If the dsn used a `/` for the path separator this would be easier to parse
		// with `url.Parse` and we could use `.Path` to get the db and then use
		// `Query()` to get the parameters and values.
		// But for consistency with the current way of doing things...
		addr = ss[0]
		dbAndParams := ss[1]
		if ss := strings.Split(dbAndParams, "&"); len(ss) == 1 {
			db = ss[0]
		} else {
			// We have to assume the first is the db
			// Then need to handle possible multiple parameters / query strings
			for i, p := range ss {
				if i == 0 {
					db = p
				} else {
					// Build key value pairs
					kv := strings.Split(p, "=")
					params[kv[0]] = kv[1]
				}
			}
		}
	} else if len(ss) == 1 {
		addr = ss[0]
	} else {
		return nil, errors.Errorf("invalid dsn, must user:password@addr[[?db[&param=X]]")
	}

	tlsConfigName, tls := params["ssl"]
	if tls {
		switch tlsConfigName {
		case "true":
			// This actually does insecureSkipVerify
			// But not even sure if it makes sense to handle false? According to
			// client_test.go it doesn't - it'd result in an error
			c, err = client.Connect(addr, user, password, db, func(c *client.Conn) { c.UseSSL(true) })
		case "custom":
			// I was too concerned about mimicking what go-sql-driver/mysql does which will
			// allow any name for a custom tls profile and maps the query parameter value to
			// that TLSConfig variable... there is no need to be that clever.
			// Instead of doing that, let's store required custom TLSConfigs in a map that
			// uses the DSN address as the key
			c, err = client.Connect(addr, user, password, db, func(c *client.Conn) { c.SetTLSConfig(customTLSConfigMap[addr]) })
		default:
			return nil, errors.Errorf("Supported options are ssl=true or ssl=custom")
		}
	} else {
		c, err = client.Connect(addr, user, password, db)
	}
	if err != nil {
		return nil, err
	}

	return &conn{c}, nil
}

type conn struct {
	*client.Conn
}

func (c *conn) Prepare(query string) (sqldriver.Stmt, error) {
	st, err := c.Conn.Prepare(query)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &stmt{st}, nil
}

func (c *conn) Close() error {
	return c.Conn.Close()
}

func (c *conn) Begin() (sqldriver.Tx, error) {
	err := c.Conn.Begin()
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &tx{c.Conn}, nil
}

func buildArgs(args []sqldriver.Value) []interface{} {
	a := make([]interface{}, len(args))

	for i, arg := range args {
		a[i] = arg
	}

	return a
}

func replyError(err error) error {
	if mysql.ErrorEqual(err, mysql.ErrBadConn) {
		return sqldriver.ErrBadConn
	} else {
		return errors.Trace(err)
	}
}

func (c *conn) Exec(query string, args []sqldriver.Value) (sqldriver.Result, error) {
	a := buildArgs(args)
	r, err := c.Conn.Execute(query, a...)
	if err != nil {
		return nil, replyError(err)
	}
	return &result{r}, nil
}

func (c *conn) Query(query string, args []sqldriver.Value) (sqldriver.Rows, error) {
	a := buildArgs(args)
	r, err := c.Conn.Execute(query, a...)
	if err != nil {
		return nil, replyError(err)
	}
	return newRows(r.Resultset)
}

type stmt struct {
	*client.Stmt
}

func (s *stmt) Close() error {
	return s.Stmt.Close()
}

func (s *stmt) NumInput() int {
	return s.Stmt.ParamNum()
}

func (s *stmt) Exec(args []sqldriver.Value) (sqldriver.Result, error) {
	a := buildArgs(args)
	r, err := s.Stmt.Execute(a...)
	if err != nil {
		return nil, replyError(err)
	}
	return &result{r}, nil
}

func (s *stmt) Query(args []sqldriver.Value) (sqldriver.Rows, error) {
	a := buildArgs(args)
	r, err := s.Stmt.Execute(a...)
	if err != nil {
		return nil, replyError(err)
	}
	return newRows(r.Resultset)
}

type tx struct {
	*client.Conn
}

func (t *tx) Commit() error {
	return t.Conn.Commit()
}

func (t *tx) Rollback() error {
	return t.Conn.Rollback()
}

type result struct {
	*mysql.Result
}

func (r *result) LastInsertId() (int64, error) {
	return int64(r.Result.InsertId), nil
}

func (r *result) RowsAffected() (int64, error) {
	return int64(r.Result.AffectedRows), nil
}

type rows struct {
	*mysql.Resultset

	columns []string
	step    int
}

func newRows(r *mysql.Resultset) (*rows, error) {
	if r == nil {
		return nil, fmt.Errorf("invalid mysql query, no correct result")
	}

	rs := new(rows)
	rs.Resultset = r

	rs.columns = make([]string, len(r.Fields))

	for i, f := range r.Fields {
		rs.columns[i] = hack.String(f.Name)
	}
	rs.step = 0

	return rs, nil
}

func (r *rows) Columns() []string {
	return r.columns
}

func (r *rows) Close() error {
	r.step = -1
	return nil
}

func (r *rows) Next(dest []sqldriver.Value) error {
	if r.step >= r.Resultset.RowNumber() {
		return io.EOF
	} else if r.step == -1 {
		return io.ErrUnexpectedEOF
	}

	for i := 0; i < r.Resultset.ColumnNumber(); i++ {
		value, err := r.Resultset.GetValue(r.step, i)
		if err != nil {
			return err
		}

		dest[i] = sqldriver.Value(value)
	}

	r.step++

	return nil
}

func init() {
	sql.Register("mysql", driver{})
}

func SetCustomTLSConfig(dsn string, caPem []byte, certPem []byte, keyPem []byte, insecureSkipVerify bool, serverName string) {
	// Extract addr from dsn
	// We can hopefully extend the use of url.Parse if we switch the DSN style
	parsed, err := url.Parse(dsn)
	if err != nil {
		errors.Errorf("Unable to parse DSN. Need to extract address to use as key for storing custom TLS config")
	}
	addr := parsed.Host

	// I thought about using serverName instead of addr below, but decided against that as
	// having multiple CA certs for one hostname is likely when you have services running on
	// different ports.

	// Basic pass-through function so we can just import the driver
	customTLSConfigMap[addr] = client.NewClientTLSConfig(caPem, certPem, keyPem, insecureSkipVerify, serverName)
}
