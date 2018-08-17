package server

import (
	"database/sql"
	"flag"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
	)

var serverConf *Server

var testAddr = flag.String("addr", "127.0.0.1:4000", "MySQL proxy server address")
var testUser = flag.String("user", "root", "MySQL user")
var testPassword = flag.String("pass", "1111", "MySQL password")
var testDB = flag.String("db", "test", "MySQL test database")

var publicKey = []byte(`-----BEGIN CERTIFICATE-----
MIIDBzCCAe+gAwIBAgIBAjANBgkqhkiG9w0BAQsFADA8MTowOAYDVQQDDDFNeVNR
TF9TZXJ2ZXJfOC4wLjEyX0F1dG9fR2VuZXJhdGVkX0NBX0NlcnRpZmljYXRlMB4X
DTE4MDgxNTAyMzg1NVoXDTI4MDgxMjAyMzg1NVowQDE+MDwGA1UEAww1TXlTUUxf
U2VydmVyXzguMC4xMl9BdXRvX0dlbmVyYXRlZF9TZXJ2ZXJfQ2VydGlmaWNhdGUw
ggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCuz6mKbSWxYJYqqPbjf4aK
0dL32FAyq0418Sw2BjWjGtwV8caWg08uevkLWHCL2AUUjsUH9AjfVLgEsgTyVvBe
ykj9VACAFOric/VsNEcrYjp7sGlFJqz0S+6RCjL3RzV3hGy1iHS6nGhmhjo0wNH1
aLRZC3zTbMCcQ8WPtsOMYa1M0+0FZ7nYI90087+ynMHJVbUKwQ8R/4Uvcjv42yA6
jZjBYa/EPcHMies2or/jfbjULi8WkN7BOTiFXosfGlKwWixZGwAM9VHrX1d7ojf4
vnnhv/Wgb/1UQuAVrBtvOVPpxdMsJ4vAyv+CnBq4HhBZq/MzSawpq0/Hl3n+B0qH
AgMBAAGjEDAOMAwGA1UdEwEB/wQCMAAwDQYJKoZIhvcNAQELBQADggEBAFrdJ2Ub
19eEmURSToH6P/AOwOPWpYrKBK7MNiMEDG3Ishc3ExhzP0BWdIZpGlhsNIxMUZcX
iCX1nDhyq2fSuzPXj81ZKmObJJyAYL5lKRvgiwDCBhrdW3axjHdJifBU6kRUTlJk
ItRwnUhRKiI/K4UGa09E0o9EPt9eajXil1Xj0TWfdDYwB1oosWADGAbUJEWlUwfz
gYjBl+6k8bjMtkfgo8IrBpcMOdXP8WP69lBZH9ElV8uvpnOmk1DIEbTziGc28xGN
x9+1K4fdHdVxRyct/jB1A2BVOqWWzfb+kKrZzYpDA6b+ciueySyN2GZbqPuVdhoC
SVqHiEL0C1sirgk=
-----END CERTIFICATE-----
 `)

var privateKey = []byte(`-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEArs+pim0lsWCWKqj243+GitHS99hQMqtONfEsNgY1oxrcFfHG
loNPLnr5C1hwi9gFFI7FB/QI31S4BLIE8lbwXspI/VQAgBTq4nP1bDRHK2I6e7Bp
RSas9EvukQoy90c1d4RstYh0upxoZoY6NMDR9Wi0WQt802zAnEPFj7bDjGGtTNPt
BWe52CPdNPO/spzByVW1CsEPEf+FL3I7+NsgOo2YwWGvxD3BzInrNqK/43241C4v
FpDewTk4hV6LHxpSsFosWRsADPVR619Xe6I3+L554b/1oG/9VELgFawbbzlT6cXT
LCeLwMr/gpwauB4QWavzM0msKatPx5d5/gdKhwIDAQABAoIBAQCf1Mzt/Qe2m1oR
nTVHImVQbbJX29bPzSMQXPcQWjMWc0uBYPMy0NJH7DXisrUMl7Flb3gk4nJuR9aM
rHurW+QgtxFvViuy7+0mqeFeUpozx3czekN173u5TefNgybAQsJXCaQfgqk/qhwm
iVmUvCv8FwS2WbOC1+9vsONkmDVJFs6FfPeB3sGDg9UjPoJJWjyyuU8jsyJmmD8B
hzdYDVPvEE93Bn5ko7l0SMlGpuDQYsguMlHGloiNQYEiXem5OHZyJOruK4YfNpta
tnc9fqLRYT/jmBc6u4G6NqECJT8M/45JUL+GtyKE9XrTeoevBIPWmjhrnR712uCx
ym2SpQchAoGBAN3jyLJ1BFyU6HOVq//+ebMa86rBJ50SB1SshZqeXYTIbKGHlMt5
ltlCnqzUCIg2RuISJaRUs65DZVl78e0vukv1J73L+o4UxnvyI6QUL6w2HcXVxeSV
zJnDbQlZXgTnjwRFyD8UuD/WR7PW8Kr+t2g/sILCgormQGrdBEEKzaC3AoGBAMmv
J4QASolQJU8p3uMvhdQIBRfA9AfA358KdSOcOKTY8vAlKT1l8njTqovFhtPtD6JJ
JqhTgmgqJnhV8wo5acKPWVJ7YLDPzWqm+JMiuVCrjYUCb9dykkfcdA7T08I2AImc
o0V+AWDF4vLnxVAg10QuPbt2cvr87OkXfTomejSxAoGAZ/jVUTHd0lgAODD5AKJI
enF1nhyWKFaLUtToYdQ1NAQKSwJR6apW0gUSfx93xAPS4Rnpw3/hFhYEhpK6gQ6t
N+xvK9NJ24vGmcuTgc3nrMVK3CnYacz7q01UaV4T3S536GOeeliGIXuR66Ya7bUf
Ud0OI6drLhTIOIrJ9IRQaC8CgYEArCZ50E5/UN28Ouo+eiUiUm3WCwSER+n19sby
XccHAo68LwdJQEM4yLHDrTKZd2OuKJPQD3XYphj0ZVrX7S48e/noAGOXkNSPPi3v
l4fYLmeAkLwXwwrkmbouxklBQ1Dc/JlbNAHuIzBGG5iB73dYx7XAs7bTRjGF9yA5
n90gCsECgYB6ciF2O7MUF47Ke99kkewDCuA1Vp0AF+xbA7WZfeRoBcfnkB8VDC2Z
yDbonYN5BVBknJ+Ck+RLI5FWZoYhyeqCn5pP1uQGk6gHmgQ7iIqlFq2jf/zCcakg
JaBn9QcZXHgpZLtcZOqncC7oUbzQv8UmeWfXJ9kK7NmMo27pKwDmiQ==
-----END RSA PRIVATE KEY-----
 `)

var ca = []byte(`-----BEGIN CERTIFICATE-----
MIIDBjCCAe6gAwIBAgIBATANBgkqhkiG9w0BAQsFADA8MTowOAYDVQQDDDFNeVNR
TF9TZXJ2ZXJfOC4wLjEyX0F1dG9fR2VuZXJhdGVkX0NBX0NlcnRpZmljYXRlMB4X
DTE4MDgxNTAyMzg1NVoXDTI4MDgxMjAyMzg1NVowPDE6MDgGA1UEAwwxTXlTUUxf
U2VydmVyXzguMC4xMl9BdXRvX0dlbmVyYXRlZF9DQV9DZXJ0aWZpY2F0ZTCCASIw
DQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAMNDdsNw4cFeexjtzMr96NUpAQkc
hgXJRHH9RKkF+XgrgFHpNyz/TUzVsQrpPeFxmv/XnWRSFJfPiF0BvUPt3V6qxi5X
I9VTLo5GiobG6BuMqXhVLn6ZWvVGA0vMg07USvB/TRsduac0ZiSozmVxXiuj78ES
UfmOhMbBnCDgwqhhGdFLGSnyMxmXypkOn4K90MFfGEe1exSrbkqK82tvopXFiQdG
g8UY42W/noPTNUp+b+O64dzkSQKbqMUVdFlM8Y5fmEbYTJm0WN8sh17c+9DrI1u/
0+0XojFDmMERn3BjysnJwhxGRolvu+FSQLeafkjXjrW5jJGjv3nmwVPgOQMCAwEA
AaMTMBEwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAmwI4Z083
Aau5mhoT34gjqCi3qzEia86x9wBTHRGSJV4VLH1kdVO4V5V6Z/M9Fi0to/FyfpSL
bJf/b49YbTzcL82MTF35wqmROr6b7gU6VEZXyMSbyYQNKjocdK9fj3HHCZDwbMd2
v7Sjca4YYB2NNzn6k7I4GfUc8h/qepq+wiyZZTkNk+aYhwoHvh40UVcNr+L1bONv
DYjJJrULCkjZkk1YKbWgqL8BIA3T94AfTSQ8/lFDaQjVLoCSQszqC7tXJqmvYnww
xcO5yRNsVffe8T3RvyEqcelRw3BK7u902J92Qam00Gb6ZtUxF2MOujXz1vOABwz+
TStwATB+cAlPMA==
-----END CERTIFICATE-----
 `)

func Test(t *testing.T) {
	//privKey, _ := pem.Decode(privateKey)
	//caCert, _ := pem.Decode(ca)
	//serverConf = NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, SHA256_PASSWORD, []string{SHA256_PASSWORD},
	//	publicKey, newTLSConfig(caCert.Bytes, privKey.Bytes))
	//serverConf = NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, SHA256_PASSWORD, []string{SHA256_PASSWORD}, defaultServer.pubKey, defaultServer.tlsConfig)
	//serverConf = NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, CACHING_SHA2_PASSWORD, []string{CACHING_SHA2_PASSWORD}, defaultServer.pubKey, defaultServer.tlsConfig)
	//serverConf = NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, MYSQL_NATIVE_PASSWORD, []string{MYSQL_NATIVE_PASSWORD}, defaultServer.pubKey, defaultServer.tlsConfig)
	serverConf = NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, SHA256_PASSWORD, []string{MYSQL_NATIVE_PASSWORD, SHA256_PASSWORD}, defaultServer.pubKey, defaultServer.tlsConfig)
	log.SetLevel(log.LevelDebug)
	TestingT(t)
}

type serverTestSuite struct {
	db *sql.DB

	l net.Listener
}

var _ = Suite(&serverTestSuite{})

type testHandler struct {
	s *serverTestSuite
}

func (h *testHandler) UseDB(dbName string) error {
	return nil
}

func (h *testHandler) handleQuery(query string, binary bool) (*mysql.Result, error) {
	ss := strings.Split(query, " ")
	switch strings.ToLower(ss[0]) {
	case "select":
		var r *mysql.Resultset
		var err error
		//for handle go mysql driver select @@max_allowed_packet
		if strings.Contains(strings.ToLower(query), "max_allowed_packet") {
			r, err = mysql.BuildSimpleResultset([]string{"@@max_allowed_packet"}, [][]interface{}{
				{mysql.MaxPayloadLen},
			}, binary)
		} else {
			r, err = mysql.BuildSimpleResultset([]string{"a", "b"}, [][]interface{}{
				{1, "hello world"},
			}, binary)
		}

		if err != nil {
			return nil, errors.Trace(err)
		} else {
			return &mysql.Result{0, 0, 0, r}, nil
		}
	case "insert":
		return &mysql.Result{0, 1, 0, nil}, nil
	case "delete":
		return &mysql.Result{0, 0, 1, nil}, nil
	case "update":
		return &mysql.Result{0, 0, 1, nil}, nil
	case "replace":
		return &mysql.Result{0, 0, 1, nil}, nil
	default:
		return nil, fmt.Errorf("invalid query %s", query)
	}

	return nil, nil
}

func (h *testHandler) HandleQuery(query string) (*mysql.Result, error) {
	return h.handleQuery(query, false)
}

func (h *testHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	return nil, nil
}
func (h *testHandler) HandleStmtPrepare(sql string) (params int, columns int, ctx interface{}, err error) {
	ss := strings.Split(sql, " ")
	switch strings.ToLower(ss[0]) {
	case "select":
		params = 1
		columns = 2
	case "insert":
		params = 2
		columns = 0
	case "replace":
		params = 2
		columns = 0
	case "update":
		params = 1
		columns = 0
	case "delete":
		params = 1
		columns = 0
	default:
		err = fmt.Errorf("invalid prepare %s", sql)
	}
	return params, columns, nil, err
}

func (h *testHandler) HandleStmtClose(context interface{}) error {
	return nil
}

func (h *testHandler) HandleStmtExecute(ctx interface{}, query string, args []interface{}) (*mysql.Result, error) {
	return h.handleQuery(query, true)
}

func (h *testHandler) HandleOtherCommand(cmd byte, data []byte) error {
	return mysql.NewError(mysql.ER_UNKNOWN_ERROR, fmt.Sprintf("command %d is not supported now", cmd))
}

func (s *serverTestSuite) SetUpSuite(c *C) {
	var err error

	s.l, err = net.Listen("tcp", *testAddr)
	c.Assert(err, IsNil)

	go s.onAccept(c)

	time.Sleep(500 * time.Millisecond)

	s.db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", *testUser, *testPassword, *testAddr, *testDB))
	c.Assert(err, IsNil)

	s.db.SetMaxIdleConns(4)
}

func (s *serverTestSuite) TearDownSuite(c *C) {
	if s.db != nil {
		s.db.Close()
	}

	if s.l != nil {
		s.l.Close()
	}
}

func (s *serverTestSuite) onAccept(c *C) {
	for {
		conn, err := s.l.Accept()
		if err != nil {
			return
		}

		go s.onConn(conn, c)
	}
}

func (s *serverTestSuite) onConn(conn net.Conn, c *C) {
	//co, err := NewConn(conn, *testUser, *testPassword, &testHandler{s})
	co, err := NewCustomizedConn(conn, serverConf, *testUser, *testPassword, &testHandler{s})
	c.Assert(err, IsNil)
	// set SSL if defined
	for {
		err = co.HandleCommand()
		if err != nil {
			return
		}
	}
}

func (s *serverTestSuite) TestSelect(c *C) {
	var a int64
	var b string

	err := s.db.QueryRow("SELECT a, b FROM tbl WHERE id=1").Scan(&a, &b)
	c.Assert(err, IsNil)
	c.Assert(a, Equals, int64(1))
	c.Assert(b, Equals, "hello world")
}

func (s *serverTestSuite) TestExec(c *C) {
	r, err := s.db.Exec("INSERT INTO tbl (a, b) values (1, \"hello world\")")
	c.Assert(err, IsNil)
	i, _ := r.LastInsertId()
	c.Assert(i, Equals, int64(1))

	r, err = s.db.Exec("REPLACE INTO tbl (a, b) values (1, \"hello world\")")
	c.Assert(err, IsNil)
	i, _ = r.RowsAffected()
	c.Assert(i, Equals, int64(1))

	r, err = s.db.Exec("UPDATE tbl SET b = \"abc\" where a = 1")
	c.Assert(err, IsNil)
	i, _ = r.RowsAffected()
	c.Assert(i, Equals, int64(1))

	r, err = s.db.Exec("DELETE FROM tbl where a = 1")
	c.Assert(err, IsNil)
	i, _ = r.RowsAffected()
	c.Assert(i, Equals, int64(1))
}

func (s *serverTestSuite) TestStmtSelect(c *C) {
	var a int64
	var b string

	err := s.db.QueryRow("SELECT a, b FROM tbl WHERE id=?", 1).Scan(&a, &b)
	c.Assert(err, IsNil)
	c.Assert(a, Equals, int64(1))
	c.Assert(b, Equals, "hello world")
}

func (s *serverTestSuite) TestStmtExec(c *C) {
	r, err := s.db.Exec("INSERT INTO tbl (a, b) values (?, ?)", 1, "hello world")
	c.Assert(err, IsNil)
	i, _ := r.LastInsertId()
	c.Assert(i, Equals, int64(1))

	r, err = s.db.Exec("REPLACE INTO tbl (a, b) values (?, ?)", 1, "hello world")
	c.Assert(err, IsNil)
	i, _ = r.RowsAffected()
	c.Assert(i, Equals, int64(1))

	r, err = s.db.Exec("UPDATE tbl SET b = \"abc\" where a = ?", 1)
	c.Assert(err, IsNil)
	i, _ = r.RowsAffected()
	c.Assert(i, Equals, int64(1))

	r, err = s.db.Exec("DELETE FROM tbl where a = ?", 1)
	c.Assert(err, IsNil)
	i, _ = r.RowsAffected()
	c.Assert(i, Equals, int64(1))
}
