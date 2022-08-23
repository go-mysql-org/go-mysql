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
	"regexp"
	"sync"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
	"github.com/siddontang/go/hack"
)

var customTLSMutex sync.Mutex

// Map of dsn address (makes more sense than full dsn?) to tls Config
var customTLSConfigMap = make(map[string]*tls.Config)

type driver struct {
}

type connInfo struct {
	standardDSN bool
	addr        string
	user        string
	password    string
	db          string
	params      url.Values
}

// ParseDSN takes a DSN string and splits it up into struct containing addr,
// user, password and db.
// It returns an error if unable to parse.
// The struct also contains a boolean indicating if the DSN is in legacy or
// standard form.
//
// Legacy form uses a `?` is used as the path separator: user:password@addr[?db]
// Standard form uses a `/`: user:password@addr/db?param=value
//
// Optional parameters are supported in the standard DSN form
func parseDSN(dsn string) (connInfo, error) {
	var matchErr error
	ci := connInfo{}

	// If a "/" occurs after "@" and then no more "@" or "/" occur after that
	ci.standardDSN, matchErr = regexp.MatchString("@[^@]+/[^@/]+", dsn)
	if matchErr != nil {
		return ci, errors.Errorf("invalid dsn, must be user:password@addr[/db[?param=X]]")
	}

	// Add a prefix so we can parse with url.Parse
	dsn = "mysql://" + dsn
	parsedDSN, parseErr := url.Parse(dsn)
	if parseErr != nil {
		return ci, errors.Errorf("invalid dsn, must be user:password@addr[/db[?param=X]]")
	}

	ci.addr = parsedDSN.Host
	ci.user = parsedDSN.User.Username()
	// We ignore the second argument as that is just a flag for existence of a password
	// If not set we get empty string anyway
	ci.password, _ = parsedDSN.User.Password()

	if ci.standardDSN {
		ci.db = parsedDSN.Path[1:]
		ci.params = parsedDSN.Query()
	} else {
		ci.db = parsedDSN.RawQuery
		// This is the equivalent to a "nil" list of parameters
		ci.params = url.Values{}
	}

	return ci, nil
}

// Open takes a supplied DSN string and opens a connection
// See ParseDSN for more information on the form of the DSN
func (d driver) Open(dsn string) (sqldriver.Conn, error) {
	var c *client.Conn

	ci, err := parseDSN(dsn)

	if err != nil {
		return nil, err
	}

	if ci.standardDSN {
		if ci.params["ssl"] != nil {
			tlsConfigName := ci.params.Get("ssl")
			switch tlsConfigName {
			case "true":
				// This actually does insecureSkipVerify
				// But not even sure if it makes sense to handle false? According to
				// client_test.go it doesn't - it'd result in an error
				c, err = client.Connect(ci.addr, ci.user, ci.password, ci.db, func(c *client.Conn) { c.UseSSL(true) })
			case "custom":
				// I was too concerned about mimicking what go-sql-driver/mysql does which will
				// allow any name for a custom tls profile and maps the query parameter value to
				// that TLSConfig variable... there is no need to be that clever.
				// Instead of doing that, let's store required custom TLSConfigs in a map that
				// uses the DSN address as the key
				c, err = client.Connect(ci.addr, ci.user, ci.password, ci.db, func(c *client.Conn) { c.SetTLSConfig(customTLSConfigMap[ci.addr]) })
			default:
				return nil, errors.Errorf("Supported options are ssl=true or ssl=custom")
			}
		} else {
			c, err = client.Connect(ci.addr, ci.user, ci.password, ci.db)
		}
	} else {
		// No more processing here. Let's only support url parameters with the newer style DSN
		c, err = client.Connect(ci.addr, ci.user, ci.password, ci.db)
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

// SetCustomTLSConfig sets a custom TLSConfig for the address (host:port) of the supplied DSN.
// It requires a full import of the driver (not by side-effects only).
// Example of supplying a custom CA, no client cert, no key, validating the
// certificate, and supplying a serverName for the validation:
//
// driver.SetCustomTLSConfig(CaPem, make([]byte, 0), make([]byte, 0), false, "my.domain.name")
func SetCustomTLSConfig(dsn string, caPem []byte, certPem []byte, keyPem []byte, insecureSkipVerify bool, serverName string) error {
	// Extract addr from dsn
	parsed, err := url.Parse(dsn)
	if err != nil {
		return errors.Errorf("Unable to parse DSN. Need to extract address to use as key for storing custom TLS config")
	}
	addr := parsed.Host

	// I thought about using serverName instead of addr below, but decided against that as
	// having multiple CA certs for one hostname is likely when you have services running on
	// different ports.

	customTLSMutex.Lock()
	// Basic pass-through function so we can just import the driver
	customTLSConfigMap[addr] = client.NewClientTLSConfig(caPem, certPem, keyPem, insecureSkipVerify, serverName)
	customTLSMutex.Unlock()

	return nil
}
