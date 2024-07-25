// This package implements database/sql/driver interface,
// so we can use go-mysql with database/sql
package driver

import (
	"crypto/tls"
	"database/sql"
	sqldriver "database/sql/driver"
	goErrors "errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
	"github.com/siddontang/go/hack"
)

var customTLSMutex sync.Mutex

// Map of dsn address (makes more sense than full dsn?) to tls Config
var (
	dsnRegex           = regexp.MustCompile("@[^@]+/[^@/]+")
	customTLSConfigMap = make(map[string]*tls.Config)
	options            = make(map[string]DriverOption)

	// can be provided by clients to allow more control in handling Go and database
	// types beyond the default Value types allowed
	namedValueCheckers []CheckNamedValueFunc
)

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
	ci := connInfo{}

	// If a "/" occurs after "@" and then no more "@" or "/" occur after that
	if strings.Contains(dsn, "@") {
		ci.standardDSN = dsnRegex.MatchString(dsn)
	} else {
		// when the `@` char is not present in the dsn, then look for `/` as the db separator
		// to indicate a standard DSN. The legacy form uses the `?` char as the db separator.
		// If neither `/` or `?` are in the dsn, simply treat the dsn as the legacy form.
		ci.standardDSN = strings.Contains(dsn, "/")
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
	var (
		c *client.Conn
		// by default database/sql driver retries will be enabled
		retries = true
	)

	ci, err := parseDSN(dsn)

	if err != nil {
		return nil, err
	}

	if ci.standardDSN {
		var timeout time.Duration
		configuredOptions := make([]client.Option, 0, len(ci.params))
		for key, value := range ci.params {
			if key == "ssl" && len(value) > 0 {
				tlsConfigName := value[0]
				switch tlsConfigName {
				case "true":
					// This actually does insecureSkipVerify
					// But not even sure if it makes sense to handle false? According to
					// client_test.go it doesn't - it'd result in an error
					configuredOptions = append(configuredOptions, UseSslOption)
				case "custom":
					// I was too concerned about mimicking what go-sql-driver/mysql does which will
					// allow any name for a custom tls profile and maps the query parameter value to
					// that TLSConfig variable... there is no need to be that clever.
					// Instead of doing that, let's store required custom TLSConfigs in a map that
					// uses the DSN address as the key
					configuredOptions = append(configuredOptions, func(c *client.Conn) error {
						c.SetTLSConfig(customTLSConfigMap[ci.addr])
						return nil
					})
				default:
					return nil, errors.Errorf("Supported options are ssl=true or ssl=custom")
				}
			} else if key == "timeout" && len(value) > 0 {
				if timeout, err = time.ParseDuration(value[0]); err != nil {
					return nil, errors.Wrap(err, "invalid duration value for timeout option")
				}
			} else if key == "retries" && len(value) > 0 {
				// by default keep the golang database/sql retry behavior enabled unless
				// the retries driver option is explicitly set to 'off'
				retries = !strings.EqualFold(value[0], "off")
			} else {
				if option, ok := options[key]; ok {
					opt := func(o DriverOption, v string) client.Option {
						return func(c *client.Conn) error {
							return o(c, v)
						}
					}(option, value[0])
					configuredOptions = append(configuredOptions, opt)
				} else {
					return nil, errors.Errorf("unsupported connection option: %s", key)
				}
			}
		}

		if timeout > 0 {
			c, err = client.ConnectWithTimeout(ci.addr, ci.user, ci.password, ci.db, timeout, configuredOptions...)
		} else {
			c, err = client.Connect(ci.addr, ci.user, ci.password, ci.db, configuredOptions...)
		}
	} else {
		// No more processing here. Let's only support url parameters with the newer style DSN
		c, err = client.Connect(ci.addr, ci.user, ci.password, ci.db)
	}
	if err != nil {
		return nil, err
	}

	// if retries are 'on' then return sqldriver.ErrBadConn which will trigger up to 3
	// retries by the database/sql package. If retries are 'off' then we'll return
	// the native go-mysql-org/go-mysql 'mysql.ErrBadConn' erorr which will prevent a retry.
	// In this case the sqldriver.Validator interface is implemented and will return
	// false for IsValid() signaling the connection is bad and should be discarded.
	return &conn{Conn: c, state: &state{valid: true, useStdLibErrors: retries}}, nil
}

type CheckNamedValueFunc func(*sqldriver.NamedValue) error

var _ sqldriver.NamedValueChecker = &conn{}
var _ sqldriver.Validator = &conn{}

type state struct {
	valid bool
	// when true, the driver connection will return ErrBadConn from the golang Standard Library
	useStdLibErrors bool
}

type conn struct {
	*client.Conn
	state *state
}

func (c *conn) CheckNamedValue(nv *sqldriver.NamedValue) error {
	for _, nvChecker := range namedValueCheckers {
		err := nvChecker(nv)
		if err == nil {
			// we've found a CheckNamedValueFunc that handled this named value
			// no need to keep looking
			return nil
		} else {
			// we've found an error, if the error is driver.ErrSkip then
			// keep looking otherwise return the unknown error
			if !goErrors.Is(sqldriver.ErrSkip, err) {
				return err
			}
		}
	}
	return sqldriver.ErrSkip
}

func (c *conn) IsValid() bool {
	return c.state.valid
}

func (c *conn) Prepare(query string) (sqldriver.Stmt, error) {
	st, err := c.Conn.Prepare(query)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &stmt{Stmt: st, connectionState: c.state}, nil
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

func (st *state) replyError(err error) error {
	isBadConnection := mysql.ErrorEqual(err, mysql.ErrBadConn)

	if st.useStdLibErrors && isBadConnection {
		return sqldriver.ErrBadConn
	} else {
		// if we have a bad connection, this mark the state of this connection as not valid
		// do the database/sql package can discard it instead of placing it back in the
		// sql.DB pool.
		st.valid = !isBadConnection
		return errors.Trace(err)
	}
}

func (c *conn) Exec(query string, args []sqldriver.Value) (sqldriver.Result, error) {
	a := buildArgs(args)
	r, err := c.Conn.Execute(query, a...)
	if err != nil {
		return nil, c.state.replyError(err)
	}
	return &result{r}, nil
}

func (c *conn) Query(query string, args []sqldriver.Value) (sqldriver.Rows, error) {
	a := buildArgs(args)
	r, err := c.Conn.Execute(query, a...)
	if err != nil {
		return nil, c.state.replyError(err)
	}
	return newRows(r.Resultset)
}

type stmt struct {
	*client.Stmt
	connectionState *state
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
		return nil, s.connectionState.replyError(err)
	}
	return &result{r}, nil
}

func (s *stmt) Query(args []sqldriver.Value) (sqldriver.Rows, error) {
	a := buildArgs(args)
	r, err := s.Stmt.Execute(a...)
	if err != nil {
		return nil, s.connectionState.replyError(err)
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
	options["compress"] = CompressOption
	options["collation"] = CollationOption
	options["readTimeout"] = ReadTimeoutOption
	options["writeTimeout"] = WriteTimeoutOption

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

// SetDSNOptions sets custom options to the driver that allows modifications to the connection.
// It requires a full import of the driver (not by side-effects only).
// Example of supplying a custom option:
//
//	driver.SetDSNOptions(map[string]DriverOption{
//			"my_option": func(c *client.Conn, value string) error {
//				c.SetCapability(mysql.CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS)
//				return nil
//			},
//		})
func SetDSNOptions(customOptions map[string]DriverOption) {
	for o, f := range customOptions {
		options[o] = f
	}
}

// AddNamedValueChecker sets a custom NamedValueChecker for the driver connection which
// allows for more control in handling Go and database types beyond the default Value types.
// See https://pkg.go.dev/database/sql/driver#NamedValueChecker
// Usage requires a full import of the driver (not by side-effects only). Also note that
// this function is not concurrent-safe, and should only be executed while setting up the driver
// before establishing any connections via `sql.Open()`.
func AddNamedValueChecker(nvCheckFunc ...CheckNamedValueFunc) {
	namedValueCheckers = append(namedValueCheckers, nvCheckFunc...)
}
