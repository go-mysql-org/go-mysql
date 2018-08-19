package client

import (
	"flag"
	"fmt"
	"strings"
	"testing"

	"github.com/juju/errors"
	. "github.com/pingcap/check"

	"github.com/siddontang/go-mysql/mysql"
)

var testHost = flag.String("host", "127.0.0.1", "MySQL server host")
// We cover the whole range of MySQL server versions using docker-compose to bind them to different ports for testing.
// MySQL is constantly updating auth plugin to make it secure:
// starting from MySQL 8.0.4, a new auth plugin is introduced, causing plain password auth to fail with error:
// ERROR 1251 (08004): Client does not support authentication protocol requested by server; consider upgrading MySQL client
var testPort = flag.String("port", "3306", "MySQL server port") //5561,5641,3306,5722,8003,8012,8013
var testUser = flag.String("user", "root", "MySQL user")
var testPassword = flag.String("pass", "", "MySQL password")
var testDB = flag.String("db", "test", "MySQL test database")

func Test(t *testing.T) {
	segs := strings.Split(*testPort, ",")
	fmt.Println(segs)
	for _, seg := range segs {
		Suite(&clientTestSuite{port: seg})
	}
	TestingT(t)
}

type clientTestSuite struct {
	c    *Conn
	port string
}

func (s *clientTestSuite) SetUpSuite(c *C) {
	var err error
	addr := fmt.Sprintf("%s:%s", *testHost, s.port)
	s.c, err = Connect(addr, *testUser, *testPassword, "")
	if err != nil {
		c.Fatal(err)
	}

	_, err = s.c.Execute("CREATE DATABASE IF NOT EXISTS " + *testDB)
	c.Assert(err, IsNil)

	_, err = s.c.Execute("USE " + *testDB)
	c.Assert(err, IsNil)

	s.testConn_CreateTable(c)
	s.testStmt_CreateTable(c)
}

func (s *clientTestSuite) TearDownSuite(c *C) {
	if s.c == nil {
		return
	}

	s.testConn_DropTable(c)
	s.testStmt_DropTable(c)

	if s.c != nil {
		s.c.Close()
	}
}

func (s *clientTestSuite) testConn_DropTable(c *C) {
	_, err := s.c.Execute("drop table if exists mixer_test_conn")
	c.Assert(err, IsNil)
}

func (s *clientTestSuite) testConn_CreateTable(c *C) {
	str := `CREATE TABLE IF NOT EXISTS mixer_test_conn (
          id BIGINT(64) UNSIGNED  NOT NULL,
          str VARCHAR(256),
          f DOUBLE,
          e enum("test1", "test2"),
          u tinyint unsigned,
          i tinyint,
          PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8`

	_, err := s.c.Execute(str)
	c.Assert(err, IsNil)
}

func (s *clientTestSuite) TestConn_Ping(c *C) {
	err := s.c.Ping()
	c.Assert(err, IsNil)
}

// NOTE for MySQL 5.5 and 5.6, server side has to config SSL to pass the TLS test, otherwise, it will throw error that
//      MySQL server does not support TLS required by the client. However, for MySQL 5.7 and above, auto generated certificates
//      are used by default so that manual config is no longer necessary.
func (s *clientTestSuite) TestConn_TLS_Verify(c *C) {
	// Verify that the provided tls.Config is used when attempting to connect to mysql.
	// An empty tls.Config will result in a connection error.
	addr := fmt.Sprintf("%s:%s", *testHost, s.port)
	_, err := Connect(addr, *testUser, *testPassword, *testDB, func(c *Conn) {
		c.UseSSL(false)
	})
	if err == nil {
		c.Fatal("expected error")
	}

	expected := "either ServerName or InsecureSkipVerify must be specified in the tls.Config"
	if !strings.Contains(err.Error(), expected) {
		c.Fatal("expected '%s' to contain '%s'", err.Error(), expected)
	}
}

func (s *clientTestSuite) TestConn_TLS_Skip_Verify(c *C) {
	// An empty tls.Config will result in a connection error but we can configure to skip it.
	addr := fmt.Sprintf("%s:%s", *testHost, s.port)
	_, err := Connect(addr, *testUser, *testPassword, *testDB, func(c *Conn) {
		c.UseSSL(true)
	})
	c.Assert(err, Equals, nil)
}

func (s *clientTestSuite) TestConn_TLS_Certificate(c *C) {
	// This test uses the TLS suite in 'go-mysql/docker/resources'. The certificates are not valid for any names.
	// And if server uses auto-generated certificates, it will be an error like:
	// "x509: certificate is valid for MySQL_Server_8.0.12_Auto_Generated_Server_Certificate, not not-a-valid-name"
	tlsConfig := NewClientTLSConfig(caPem, certPem, keyPem, false, "not-a-valid-name")
	addr := fmt.Sprintf("%s:%s", *testHost, s.port)
	_, err := Connect(addr, *testUser, *testPassword, *testDB, func(c *Conn) {
		c.SetTLSConfig(tlsConfig)
	})
	if err == nil {
		c.Fatal("expected error")
	}
	if !strings.Contains(errors.Details(err), "certificate is not valid for any names") &&
		!strings.Contains(errors.Details(err), "certificate is valid for") {
			c.Fatalf("expected errors for server name verification, but got unknown error: %s", errors.Details(err))
	}
}

func (s *clientTestSuite) TestConn_Insert(c *C) {
	str := `insert into mixer_test_conn (id, str, f, e) values(1, "a", 3.14, "test1")`

	pkg, err := s.c.Execute(str)
	c.Assert(err, IsNil)
	c.Assert(pkg.AffectedRows, Equals, uint64(1))
}

func (s *clientTestSuite) TestConn_Select(c *C) {
	str := `select str, f, e from mixer_test_conn where id = 1`

	result, err := s.c.Execute(str)
	c.Assert(err, IsNil)
	c.Assert(result.Fields, HasLen, 3)
	c.Assert(result.Values, HasLen, 1)

	ss, _ := result.GetString(0, 0)
	c.Assert(ss, Equals, "a")

	f, _ := result.GetFloat(0, 1)
	c.Assert(f, Equals, float64(3.14))

	e, _ := result.GetString(0, 2)
	c.Assert(e, Equals, "test1")

	ss, _ = result.GetStringByName(0, "str")
	c.Assert(ss, Equals, "a")

	f, _ = result.GetFloatByName(0, "f")
	c.Assert(f, Equals, float64(3.14))

	e, _ = result.GetStringByName(0, "e")
	c.Assert(e, Equals, "test1")
}

func (s *clientTestSuite) TestConn_Escape(c *C) {
	e := `""''\abc`
	str := fmt.Sprintf(`insert into mixer_test_conn (id, str) values(5, "%s")`,
		mysql.Escape(e))

	_, err := s.c.Execute(str)
	c.Assert(err, IsNil)

	str = `select str from mixer_test_conn where id = ?`

	r, err := s.c.Execute(str, 5)
	c.Assert(err, IsNil)

	ss, _ := r.GetString(0, 0)
	c.Assert(ss, Equals, e)
}

func (s *clientTestSuite) TestConn_SetCharset(c *C) {
	err := s.c.SetCharset("gb2312")
	c.Assert(err, IsNil)

	err = s.c.SetCharset("utf8")
	c.Assert(err, IsNil)
}

func (s *clientTestSuite) testStmt_DropTable(c *C) {
	str := `drop table if exists mixer_test_stmt`

	stmt, err := s.c.Prepare(str)
	c.Assert(err, IsNil)

	defer stmt.Close()

	_, err = stmt.Execute()
	c.Assert(err, IsNil)
}

func (s *clientTestSuite) testStmt_CreateTable(c *C) {
	str := `CREATE TABLE IF NOT EXISTS mixer_test_stmt (
          id BIGINT(64) UNSIGNED  NOT NULL,
          str VARCHAR(256),
          f DOUBLE,
          e enum("test1", "test2"),
          u tinyint unsigned,
          i tinyint,
          PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8`

	stmt, err := s.c.Prepare(str)
	c.Assert(err, IsNil)

	defer stmt.Close()

	_, err = stmt.Execute()
	c.Assert(err, IsNil)
}

func (s *clientTestSuite) TestStmt_Delete(c *C) {
	str := `delete from mixer_test_stmt`

	stmt, err := s.c.Prepare(str)
	c.Assert(err, IsNil)

	defer stmt.Close()

	_, err = stmt.Execute()
	c.Assert(err, IsNil)
}

func (s *clientTestSuite) TestStmt_Insert(c *C) {
	str := `insert into mixer_test_stmt (id, str, f, e, u, i) values (?, ?, ?, ?, ?, ?)`

	stmt, err := s.c.Prepare(str)
	c.Assert(err, IsNil)

	defer stmt.Close()

	r, err := stmt.Execute(1, "a", 3.14, "test1", 255, -127)
	c.Assert(err, IsNil)

	c.Assert(r.AffectedRows, Equals, uint64(1))
}

func (s *clientTestSuite) TestStmt_Select(c *C) {
	str := `select str, f, e from mixer_test_stmt where id = ?`

	stmt, err := s.c.Prepare(str)
	c.Assert(err, IsNil)

	defer stmt.Close()

	result, err := stmt.Execute(1)
	c.Assert(err, IsNil)
	c.Assert(result.Values, HasLen, 1)
	c.Assert(result.Fields, HasLen, 3)

	ss, _ := result.GetString(0, 0)
	c.Assert(ss, Equals, "a")

	f, _ := result.GetFloat(0, 1)
	c.Assert(f, Equals, float64(3.14))

	e, _ := result.GetString(0, 2)
	c.Assert(e, Equals, "test1")

	ss, _ = result.GetStringByName(0, "str")
	c.Assert(ss, Equals, "a")

	f, _ = result.GetFloatByName(0, "f")
	c.Assert(f, Equals, float64(3.14))

	e, _ = result.GetStringByName(0, "e")
	c.Assert(e, Equals, "test1")

}

func (s *clientTestSuite) TestStmt_NULL(c *C) {
	str := `insert into mixer_test_stmt (id, str, f, e) values (?, ?, ?, ?)`

	stmt, err := s.c.Prepare(str)
	c.Assert(err, IsNil)

	defer stmt.Close()

	result, err := stmt.Execute(2, nil, 3.14, nil)
	c.Assert(err, IsNil)

	c.Assert(result.AffectedRows, Equals, uint64(1))

	stmt.Close()

	str = `select * from mixer_test_stmt where id = ?`
	stmt, err = s.c.Prepare(str)
	defer stmt.Close()

	c.Assert(err, IsNil)

	result, err = stmt.Execute(2)
	b, err := result.IsNullByName(0, "id")
	c.Assert(err, IsNil)
	c.Assert(b, Equals, false)

	b, err = result.IsNullByName(0, "str")
	c.Assert(err, IsNil)
	c.Assert(b, Equals, true)

	b, err = result.IsNullByName(0, "f")
	c.Assert(err, IsNil)
	c.Assert(b, Equals, false)

	b, err = result.IsNullByName(0, "e")
	c.Assert(err, IsNil)
	c.Assert(b, Equals, true)
}

func (s *clientTestSuite) TestStmt_Unsigned(c *C) {
	str := `insert into mixer_test_stmt (id, u) values (?, ?)`

	stmt, err := s.c.Prepare(str)
	c.Assert(err, IsNil)
	defer stmt.Close()

	result, err := stmt.Execute(3, uint8(255))
	c.Assert(err, IsNil)
	c.Assert(result.AffectedRows, Equals, uint64(1))

	str = `select u from mixer_test_stmt where id = ?`

	stmt, err = s.c.Prepare(str)
	c.Assert(err, IsNil)
	defer stmt.Close()

	result, err = stmt.Execute(3)
	c.Assert(err, IsNil)

	u, err := result.GetUint(0, 0)
	c.Assert(err, IsNil)
	c.Assert(u, Equals, uint64(255))
}

func (s *clientTestSuite) TestStmt_Signed(c *C) {
	str := `insert into mixer_test_stmt (id, i) values (?, ?)`

	stmt, err := s.c.Prepare(str)
	c.Assert(err, IsNil)
	defer stmt.Close()

	_, err = stmt.Execute(4, 127)
	c.Assert(err, IsNil)

	_, err = stmt.Execute(uint64(18446744073709551516), int8(-128))
	c.Assert(err, IsNil)
}

func (s *clientTestSuite) TestStmt_Trans(c *C) {
	_, err := s.c.Execute(`insert into mixer_test_stmt (id, str) values (1002, "abc")`)
	c.Assert(err, IsNil)

	err = s.c.Begin()
	c.Assert(err, IsNil)

	str := `select str from mixer_test_stmt where id = ?`

	stmt, err := s.c.Prepare(str)
	c.Assert(err, IsNil)

	defer stmt.Close()

	_, err = stmt.Execute(1002)
	c.Assert(err, IsNil)

	err = s.c.Commit()
	c.Assert(err, IsNil)

	r, err := stmt.Execute(1002)
	c.Assert(err, IsNil)

	str, _ = r.GetString(0, 0)
	c.Assert(str, Equals, `abc`)
}

// the certificates are in go-mysql/docker/resources

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

var certPem = []byte(`-----BEGIN CERTIFICATE-----
MIIDBjCCAe4CCQDg06wCf7hcuTANBgkqhkiG9w0BAQUFADBFMQswCQYDVQQGEwJB
VTETMBEGA1UECBMKU29tZS1TdGF0ZTEhMB8GA1UEChMYSW50ZXJuZXQgV2lkZ2l0
cyBQdHkgTHRkMB4XDTE4MDgxOTA4NDY0N1oXDTI4MDgxNjA4NDY0N1owRTELMAkG
A1UEBhMCQVUxEzARBgNVBAgTClNvbWUtU3RhdGUxITAfBgNVBAoTGEludGVybmV0
IFdpZGdpdHMgUHR5IEx0ZDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB
AMmivNyk3Rc1ZvLPhb3WPNkf9f2G4g9nMc0+eMrR1IKJ1U1A98ojeIBT+pfk1bSq
Ol0UDm66Vd3YQ+4HpyYHaYV6mwoTEulL9Quk8RLa7TRwQu3PLi3o567RhVIrx8Z3
umuWb9UUzJfSFH04Uy9+By4CJCqIQXU4BocLIKHhIkNjmAQ9fWO1hZ8zmPHSEfvu
Wqa/DYKGvF0MJr4Lnkm/sKUd+O94p9suvwM6OGIDibACiKRF2H+JbgQLbA58zkLv
DHtXOqsCL7HxiONX8VDrQjN/66Nh9omk/Bx2Ec8IqappHvWf768HSH79x/znaial
VEV+6K0gP+voJHfnA10laWMCAwEAATANBgkqhkiG9w0BAQUFAAOCAQEAPD+Fn1qj
HN62GD3eIgx6wJxYuemhdbgmEwrZZf4V70lS6e9Iloif0nBiISDxJUpXVWNRCN3Z
3QVC++F7deDmWL/3dSpXRvWsapzbCUhVQ2iBcnZ7QCOdvAqYR1ecZx70zvXCwBcd
6XKmRtdeNV6B211KRFmTYtVyPq4rcWrkTPGwPBncJI1eQQmyFv2T9SwVVp96Nbrq
sf7zrJGmuVCdXGPRi/ALVHtJCz6oPoft3I707eMe+ijnFqwGbmMD4fMD6Ync/hEz
PyR5FMZkXSXHS0gkA5pfwW7wJ2WSWDhI6JMS1gbatY7QzgHbKoQpxBPUXlnzzj2h
7O9cgFTh/XOZXQ==
-----END CERTIFICATE-----`)

var keyPem = []byte(`-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAyaK83KTdFzVm8s+FvdY82R/1/YbiD2cxzT54ytHUgonVTUD3
yiN4gFP6l+TVtKo6XRQObrpV3dhD7genJgdphXqbChMS6Uv1C6TxEtrtNHBC7c8u
LejnrtGFUivHxne6a5Zv1RTMl9IUfThTL34HLgIkKohBdTgGhwsgoeEiQ2OYBD19
Y7WFnzOY8dIR++5apr8Ngoa8XQwmvgueSb+wpR3473in2y6/Azo4YgOJsAKIpEXY
f4luBAtsDnzOQu8Me1c6qwIvsfGI41fxUOtCM3/ro2H2iaT8HHYRzwipqmke9Z/v
rwdIfv3H/OdqJqVURX7orSA/6+gkd+cDXSVpYwIDAQABAoIBAAGLY5L1GFRzLkSx
3j5kA7dODV5RyC2CBtmhnt8+2DffwmiDFOLRfrzM5+B9+j0WCLhpzOqANuQqIesS
1+7so5xIIiPjnYN393qNWuNgFe0O5xRXP+1OGWg3ZqQIfdFBXYYxcs3ZCPAoxctn
wQteFcP+dDR3MrkpIrOqHCfhR5foieOMP+9k5kCjk+aZqhEmFyko+X+xVO/32xs+
+3qXhUrHt3Op5on30QMOFguniQlYwLJkd9qVjGuGMIrVPxoUz0rya4SKrGKgkAr8
mvQe2+sZo7cc6zC2ceaGMJU7z1RalTrCObbg5mynlu+Vf0E/YiES0abkQhSbcSB9
mAkJC7ECgYEA/H1NDEiO164yYK9ji4HM/8CmHegWS4qsgrzAs8lU0yAcgdg9e19A
mNi8yssfIBCw62RRE4UGWS5F82myhmvq/mXbf8eCJ2CMgdCHQh1rT7WFD/Uc5Pe/
8Lv2jNMQ61POguPyq6D0qcf8iigKIMHa1MIgAOmrgWrxulfbSUhm370CgYEAzHBu
J9p4dAqW32+Hrtv2XE0KUjH72TXr13WErosgeGTfsIW2exXByvLasxOJSY4Wb8oS
OLZ7bgp/EBchAc7my+nF8n5uOJxipWQUB5BoeB9aUJZ9AnWF4RDl94Jlm5PYBG/J
lRXrMtSTTIgmSw3Ft2A1vRMOQaHX89lNwOZL758CgYAXOT84/gOFexRPKFKzpkDA
1WtyHMLQN/UeIVZoMwCGWtHEb6tYCa7bYDQdQwmd3Wsoe5WpgfbPhR4SAYrWKl72
/09tNWCXVp4V4qRORH52Wm/ew+Dgfpk8/0zyLwfDXXYFPAo6Fxfp9ecYng4wbSQ/
pYtkChooUTniteoJl4s+0QKBgHbFEpoAqF3yEPi52L/TdmrlLwvVkhT86IkB8xVc
Kn8HS5VH+V3EpBN9x2SmAupCq/JCGRftnAOwAWWdqkVcqGTq6V8Z6HrnD8A6RhCm
6qpuvI94/iNBl4fLw25pyRH7cFITh68fTsb3DKQ3rNeJpsYEFPRFb9Ddb5JxOmTI
5nDNAoGBAM+SyOhUGU+0Uw2WJaGWzmEutjeMRr5Z+cZ8keC/ZJNdji/faaQoeOQR
OXI8O6RBTBwVNQMyDyttT8J8BkISwfAhSdPkjgPw9GZ1pGREl53uCFDIlX2nvtQM
ioNzG5WHB7Gd7eUUTA91kRF9MZJTHPqNiNGR0Udj/trGyGqJebni
-----END RSA PRIVATE KEY-----`)
