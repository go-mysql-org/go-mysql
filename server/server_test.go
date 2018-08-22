package server

import (
	"crypto/tls"
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
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/mysql"
)

var testAddr = flag.String("addr", "127.0.0.1:4000", "MySQL proxy server address")
var testUser = flag.String("user", "root", "MySQL user")
var testPassword = flag.String("pass", "123456", "MySQL password")
var testDB = flag.String("db", "test", "MySQL test database")

var tlsConf = NewServerTLSConfig(caPem, certPem, keyPem, tls.VerifyClientCertIfGiven)

func prepareServerConf() []*Server {
	// add default server without TLS
	var servers = []*Server{
		// with default TLS
		NewDefaultServer(),
		// for key exchange, CLIENT_SSL must be enabled for the server and if the connection is not secured with TLS
		// server permits MYSQL_NATIVE_PASSWORD only
		NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, pubPem, tlsConf),
		NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, pubPem, tlsConf),
		// server permits SHA256_PASSWORD only
		NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_SHA256_PASSWORD, pubPem, tlsConf),
		// server permits CACHING_SHA2_PASSWORD only
		NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_CACHING_SHA2_PASSWORD, pubPem, tlsConf),

		// test auth switch: server permits SHA256_PASSWORD only but sent different method MYSQL_NATIVE_PASSWORD in handshake response
		NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, pubPem, tlsConf),
		// test auth switch: server permits CACHING_SHA2_PASSWORD only but sent different method MYSQL_NATIVE_PASSWORD in handshake response
		NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, pubPem, tlsConf),
		// test auth switch: server permits CACHING_SHA2_PASSWORD only but sent different method SHA256_PASSWORD in handshake response
		NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_SHA256_PASSWORD, pubPem, tlsConf),
		// test auth switch: server permits MYSQL_NATIVE_PASSWORD only but sent different method SHA256_PASSWORD in handshake response
		NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_SHA256_PASSWORD, pubPem, tlsConf),
		// test auth switch: server permits SHA256_PASSWORD only but sent different method CACHING_SHA2_PASSWORD in handshake response
		NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_CACHING_SHA2_PASSWORD, pubPem, tlsConf),
		// test auth switch: server permits MYSQL_NATIVE_PASSWORD only but sent different method CACHING_SHA2_PASSWORD in handshake response
		NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_CACHING_SHA2_PASSWORD, pubPem, tlsConf),
	}
	return servers
}

func Test(t *testing.T) {
	log.SetLevel(log.LevelDebug)

	// general tests
	inMemProvider := NewInMemoryProvider()
	inMemProvider.AddUser(*testUser, *testPassword)

	servers := prepareServerConf()
	//no TLS
	for _, svr := range servers {
		Suite(&serverTestSuite{
			server:       svr,
			credProvider: inMemProvider,
			tlsPara:      "false",
		})
	}

	// TLS if server supports
	for _, svr := range servers {
		if svr.tlsConfig != nil {
			Suite(&serverTestSuite{
				server:       svr,
				credProvider: inMemProvider,
				tlsPara:      "skip-verify",
			})
		}
	}

	TestingT(t)
}

type serverTestSuite struct {
	server       *Server
	credProvider CredentialProvider

	tlsPara string

	db *sql.DB

	l net.Listener
}

func (s *serverTestSuite) SetUpSuite(c *C) {
	var err error

	s.l, err = net.Listen("tcp", *testAddr)
	c.Assert(err, IsNil)

	go s.onAccept(c)

	time.Sleep(20 * time.Millisecond)

	s.db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s?tls=%s", *testUser, *testPassword, *testAddr, *testDB, s.tlsPara))
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
	co, err := NewCustomizedConn(conn, s.server, s.credProvider, &testHandler{s})
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

var pubPem = []byte(`-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAsraCori69OXEkA07Ykp2
Ju+aHz33PqgKj0qbSbPm6ePh2mer2GWOxC4q1wdRwzgddwTTqSdonhM4XuVyyNqq
gM7uv9JoWCONcKo28cPRK7gH7up7nYFllNFXUAA0/XQ+95tqtdITNplQLIceFIXz
5Bvi9fThcpf9M6qKdNUa2Wd24rM/n6qxoUG2ksDDVXQC30RAHkGCdNi10iya8Pj/
ZaEG86NXFpvvnLHRHiih/gXe7nby1sR6BxaEG2bLZd0cjdL5MuWOPeQ450H6mCtV
SX4poNq9YrdP4XW9M0N7nocRU0p5aUvLWxy6XrUTSP0iRkC7ppEPG0p2Xtsq7QGT
MwIDAQAB
-----END PUBLIC KEY-----`)

var certPem = []byte(`-----BEGIN CERTIFICATE-----
MIIDBjCCAe4CCQDg06wCf7hcuDANBgkqhkiG9w0BAQUFADBFMQswCQYDVQQGEwJB
VTETMBEGA1UECBMKU29tZS1TdGF0ZTEhMB8GA1UEChMYSW50ZXJuZXQgV2lkZ2l0
cyBQdHkgTHRkMB4XDTE4MDgxOTA4NDUyNVoXDTI4MDgxNjA4NDUyNVowRTELMAkG
A1UEBhMCQVUxEzARBgNVBAgTClNvbWUtU3RhdGUxITAfBgNVBAoTGEludGVybmV0
IFdpZGdpdHMgUHR5IEx0ZDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB
ALK2gqK4uvTlxJANO2JKdibvmh899z6oCo9Km0mz5unj4dpnq9hljsQuKtcHUcM4
HXcE06knaJ4TOF7lcsjaqoDO7r/SaFgjjXCqNvHD0Su4B+7qe52BZZTRV1AANP10
PvebarXSEzaZUCyHHhSF8+Qb4vX04XKX/TOqinTVGtlnduKzP5+qsaFBtpLAw1V0
At9EQB5BgnTYtdIsmvD4/2WhBvOjVxab75yx0R4oof4F3u528tbEegcWhBtmy2Xd
HI3S+TLljj3kOOdB+pgrVUl+KaDavWK3T+F1vTNDe56HEVNKeWlLy1scul61E0j9
IkZAu6aRDxtKdl7bKu0BkzMCAwEAATANBgkqhkiG9w0BAQUFAAOCAQEAma3yFqR7
xkeaZBg4/1I3jSlaNe5+2JB4iybAkMOu77fG5zytLomTbzdhewsuBwpTVMJdga8T
IdPeIFCin1U+5SkbjSMlpKf+krE+5CyrNJ5jAgO9ATIqx66oCTYXfGlNapGRLfSE
sa0iMqCe/dr4GPU+flW2DZFWiyJVDSF1JjReQnfrWY+SD2SpP/lmlgltnY8MJngd
xBLG5nsZCpUXGB713Q8ZyIm2ThVAMiskcxBleIZDDghLuhGvY/9eFJhZpvOkjWa6
XGEi4E1G/SA+zVKFl41nHKCdqXdmIOnpcLlFBUVloQok5a95Kqc1TYw3f+WbdFff
99dAgk3gWwWZQA==
-----END CERTIFICATE-----`)

var keyPem = []byte(`-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEAsraCori69OXEkA07Ykp2Ju+aHz33PqgKj0qbSbPm6ePh2mer
2GWOxC4q1wdRwzgddwTTqSdonhM4XuVyyNqqgM7uv9JoWCONcKo28cPRK7gH7up7
nYFllNFXUAA0/XQ+95tqtdITNplQLIceFIXz5Bvi9fThcpf9M6qKdNUa2Wd24rM/
n6qxoUG2ksDDVXQC30RAHkGCdNi10iya8Pj/ZaEG86NXFpvvnLHRHiih/gXe7nby
1sR6BxaEG2bLZd0cjdL5MuWOPeQ450H6mCtVSX4poNq9YrdP4XW9M0N7nocRU0p5
aUvLWxy6XrUTSP0iRkC7ppEPG0p2Xtsq7QGTMwIDAQABAoIBAGh1m8hHWCg7gXh9
838RbRx3IswuKS27hWiaQEiFWmzOIb7KqDy1qAxtu+ayRY1paHegH6QY/+Kd824s
ibpzbgQacJ04/HrAVTVMmQ8Z2VLHoAN7lcPL1bd14aZGaLLZVtDeTDJ413grhxxv
4ho27gcgcbo4Z+rWgk7H2WRPCAGYqWYAycm3yF5vy9QaO6edU+T588YsEQOos5iy
5pVFSGDGZkcUp1ukL3BJYR+jvygn6WPCobQ/LScUdi+ucitaI9i+UdlLokZARVRG
M/msqcTM73thR8yVRcexU6NUDxRBfZ/f7moSAEbBmGDXuxDcIyH9KGMQ2rMtN1X3
lK8UNwkCgYEA2STJq/IUQHjdqd3Dqh/Q7Zm8/pMWFqLJSkqpnFtFsXPyUOx9zDOy
KqkIfGeyKwvsj9X9BcZ0FUKj9zoct1/WpPY+h7i7+z0MIujBh4AMjAcDrt4o76yK
UHuVmG2xKTdJoAbqOdToQeX6E82Ioal5pbB2W7AbCQScNBPZ52jxgtcCgYEA0rE7
2dFiRm0YmuszFYxft2+GP6NgP3R2TQNEooi1uCXG2xgwObie1YCHzpZ5CfSqJIxP
XB7DXpIWi7PxJoeai2F83LnmdFz6F1BPRobwDoSFNdaSKLg4Yf856zpgYNKhL1fE
OoOXj4VBWBZh1XDfZV44fgwlMIf7edOF1XOagwUCgYAw953O+7FbdKYwF0V3iOM5
oZDAK/UwN5eC/GFRVDfcM5RycVJRCVtlSWcTfuLr2C2Jpiz/72fgH34QU3eEVsV1
v94MBznFB1hESw7ReqvZq/9FoO3EVrl+OtBaZmosLD6bKtQJJJ0Xtz/01UW5hxla
pveZ55XBK9v51nwuNjk4UwKBgHD8fJUllSchUCWb5cwzeAz98Kdl7LJ6uQo5q2/i
EllLYOWThiEeIYdrIuklholRPIDXAaPsF2c6vn5yo+q+o6EFSZlw0+YpCjDAb5Lp
wAh5BprFk6HkkM/0t9Guf4rMyYWC8odSlE9x7YXYkuSMYDCTI4Zs6vCoq7I8PbQn
B4AlAoGAZ6Ee5m/ph5UVp/3+cR6jCY7aHBUU/M3pbJSkVjBW+ymEBVJ6sUdz8k3P
x8BiPEQggNN7faWBqRWP7KXPnDYHh6shYUgPJwI5HX6NE/ZDnnXjeysHRyf0oCo5
S6tHXwHNKB5HS1c/KDyyNGjP2oi/MF4o/MGWNWEcK6TJA3RGOYM=
-----END RSA PRIVATE KEY-----`)

var caPem = []byte(`-----BEGIN CERTIFICATE-----
MIIDtTCCAp2gAwIBAgIJANeS1FOzWXlZMA0GCSqGSIb3DQEBBQUAMEUxCzAJBgNV
BAYTAkFVMRMwEQYDVQQIEwpTb21lLVN0YXRlMSEwHwYDVQQKExhJbnRlcm5ldCBX
aWRnaXRzIFB0eSBMdGQwHhcNMTgwODE2MTUxNDE5WhcNMjEwNjA1MTUxNDE5WjBF
MQswCQYDVQQGEwJBVTETMBEGA1UECBMKU29tZS1TdGF0ZTEhMB8GA1UEChMYSW50
ZXJuZXQgV2lkZ2l0cyBQdHkgTHRkMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIB
CgKCAQEAsV6xlhFxMn14Pn7XBRGLt8/HXmhVVu20IKFgIOyX7gAZr0QLsuT1fGf5
zH9HrlgOMkfdhV847U03KPfUnBsi9lS6/xOxnH/OzTYM0WW0eNMGF7eoxrS64GSb
PVX4pLi5+uwrrZT5HmDgZi49ANmuX6UYmH/eRRvSIoYUTV6t0aYsLyKvlpEAtRAe
4AlKB236j5ggmJ36QUhTFTbeNbeOOgloTEdPK8Y/kgpnhiqzMdPqqIc7IeXUc456
yX8MJUgniTM2qCNTFdEw+C2Ok0RbU6TI2SuEgVF4jtCcVEKxZ8kYbioONaePQKFR
/EhdXO+/ag1IEdXElH9knLOfB+zCgwIDAQABo4GnMIGkMB0GA1UdDgQWBBQgHiwD
00upIbCOunlK4HRw89DhjjB1BgNVHSMEbjBsgBQgHiwD00upIbCOunlK4HRw89Dh
jqFJpEcwRTELMAkGA1UEBhMCQVUxEzARBgNVBAgTClNvbWUtU3RhdGUxITAfBgNV
BAoTGEludGVybmV0IFdpZGdpdHMgUHR5IEx0ZIIJANeS1FOzWXlZMAwGA1UdEwQF
MAMBAf8wDQYJKoZIhvcNAQEFBQADggEBAFMZFQTFKU5tWIpWh8BbVZeVZcng0Kiq
qwbhVwaTkqtfmbqw8/w+faOWylmLncQEMmgvnUltGMQlQKBwQM2byzPkz9phal3g
uI0JWJYqtcMyIQUB9QbbhrDNC9kdt/ji/x6rrIqzaMRuiBXqH5LQ9h856yXzArqd
cAQGzzYpbUCIv7ciSB93cKkU73fQLZVy5ZBy1+oAa1V9U4cb4G/20/PDmT+G3Gxz
pEjeDKtz8XINoWgA2cSdfAhNZt5vqJaCIZ8qN0z6C7SUKwUBderERUMLUXdhUldC
KTVHyEPvd0aULd5S5vEpKCnHcQmFcLdoN8t9k9pR9ZgwqXbyJHlxWFo=
-----END CERTIFICATE-----`)
