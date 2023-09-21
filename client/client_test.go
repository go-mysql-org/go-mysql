package client

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/test_util"
	"github.com/go-mysql-org/go-mysql/test_util/test_keys"
)

type clientTestSuite struct {
	suite.Suite
	c    *Conn
	port string
}

func TestClientSuite(t *testing.T) {
	segs := strings.Split(*test_util.MysqlPort, ",")
	for _, seg := range segs {
		suite.Run(t, &clientTestSuite{port: seg})
	}
}

func (s *clientTestSuite) SetupSuite() {
	var err error
	addr := fmt.Sprintf("%s:%s", *test_util.MysqlHost, s.port)
	s.c, err = Connect(addr, *testUser, *testPassword, "")
	require.NoError(s.T(), err)

	var result *mysql.Result
	result, err = s.c.Execute("CREATE DATABASE IF NOT EXISTS " + *testDB)
	require.NoError(s.T(), err)
	require.GreaterOrEqual(s.T(), result.RowNumber(), 0)

	_, err = s.c.Execute("USE " + *testDB)
	require.NoError(s.T(), err)

	s.testConn_CreateTable()
	s.testStmt_CreateTable()
}

func (s *clientTestSuite) TearDownSuite() {
	if s.c == nil {
		return
	}

	s.testConn_DropTable()
	s.testStmt_DropTable()

	if s.c != nil {
		s.c.Close()
	}
}

func (s *clientTestSuite) testConn_DropTable() {
	_, err := s.c.Execute("drop table if exists mixer_test_conn")
	require.NoError(s.T(), err)
}

func (s *clientTestSuite) testConn_CreateTable() {
	str := `CREATE TABLE IF NOT EXISTS mixer_test_conn (
          id BIGINT(64) UNSIGNED  NOT NULL,
          str VARCHAR(256),
          f DOUBLE,
          e enum("test1", "test2"),
          u tinyint unsigned,
          i tinyint,
          j json,
          PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8`

	_, err := s.c.Execute(str)
	require.NoError(s.T(), err)
}

func (s *clientTestSuite) TestConn_Ping() {
	err := s.c.Ping()
	require.NoError(s.T(), err)
}

func (s *clientTestSuite) TestConn_SetCapability() {
	caps := []uint32{
		mysql.CLIENT_LONG_PASSWORD,
		mysql.CLIENT_FOUND_ROWS,
		mysql.CLIENT_LONG_FLAG,
		mysql.CLIENT_CONNECT_WITH_DB,
		mysql.CLIENT_NO_SCHEMA,
		mysql.CLIENT_COMPRESS,
		mysql.CLIENT_ODBC,
		mysql.CLIENT_LOCAL_FILES,
		mysql.CLIENT_IGNORE_SPACE,
		mysql.CLIENT_PROTOCOL_41,
		mysql.CLIENT_INTERACTIVE,
		mysql.CLIENT_SSL,
		mysql.CLIENT_IGNORE_SIGPIPE,
		mysql.CLIENT_TRANSACTIONS,
		mysql.CLIENT_RESERVED,
		mysql.CLIENT_SECURE_CONNECTION,
		mysql.CLIENT_MULTI_STATEMENTS,
		mysql.CLIENT_MULTI_RESULTS,
		mysql.CLIENT_PS_MULTI_RESULTS,
		mysql.CLIENT_PLUGIN_AUTH,
		mysql.CLIENT_CONNECT_ATTRS,
		mysql.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA,
	}

	for _, capI := range caps {
		require.False(s.T(), s.c.ccaps&capI > 0)
		s.c.SetCapability(capI)
		require.True(s.T(), s.c.ccaps&capI > 0)
		s.c.UnsetCapability(capI)
		require.False(s.T(), s.c.ccaps&capI > 0)
	}
}

// NOTE for MySQL 5.5 and 5.6, server side has to config SSL to pass the TLS test, otherwise, it will throw error that
// MySQL server does not support TLS required by the client. However, for MySQL 5.7 and above, auto generated certificates
// are used by default so that manual config is no longer necessary.
func (s *clientTestSuite) TestConn_TLS_Verify() {
	// Verify that the provided tls.Config is used when attempting to connect to mysql.
	// An empty tls.Config will result in a connection error.
	addr := fmt.Sprintf("%s:%s", *test_util.MysqlHost, s.port)
	_, err := Connect(addr, *testUser, *testPassword, *testDB, func(c *Conn) {
		c.UseSSL(false)
	})
	expected := "either ServerName or InsecureSkipVerify must be specified in the tls.Config"

	require.ErrorContains(s.T(), err, expected)
}

func (s *clientTestSuite) TestConn_TLS_Skip_Verify() {
	// An empty tls.Config will result in a connection error but we can configure to skip it.
	addr := fmt.Sprintf("%s:%s", *test_util.MysqlHost, s.port)
	_, err := Connect(addr, *testUser, *testPassword, *testDB, func(c *Conn) {
		c.UseSSL(true)
	})
	require.NoError(s.T(), err)
}

func (s *clientTestSuite) TestConn_TLS_Certificate() {
	// This test uses the TLS suite in 'go-mysql/docker/resources'. The certificates are not valid for any names.
	// And if server uses auto-generated certificates, it will be an error like:
	// "x509: certificate is valid for MySQL_Server_8.0.12_Auto_Generated_Server_Certificate, not not-a-valid-name"
	tlsConfig := NewClientTLSConfig(test_keys.CaPem, test_keys.CertPem, test_keys.KeyPem, false, "not-a-valid-name")
	addr := fmt.Sprintf("%s:%s", *test_util.MysqlHost, s.port)
	_, err := Connect(addr, *testUser, *testPassword, *testDB, func(c *Conn) {
		c.SetTLSConfig(tlsConfig)
	})
	require.Error(s.T(), err)
	if !strings.Contains(errors.ErrorStack(err), "certificate is not valid for any names") &&
		!strings.Contains(errors.ErrorStack(err), "certificate is valid for") {
		s.T().Fatalf("expected errors for server name verification, but got unknown error: %s", errors.ErrorStack(err))
	}
}

func (s *clientTestSuite) TestConn_Insert() {
	str := `insert into mixer_test_conn (id, str, f, e) values(1, "a", 3.14, "test1")`

	pkg, err := s.c.Execute(str)
	require.NoError(s.T(), err)
	require.Equal(s.T(), uint64(1), pkg.AffectedRows)
}

func (s *clientTestSuite) TestConn_Insert2() {
	str := `insert into mixer_test_conn (id, j) values(?, ?)`
	j := json.RawMessage(`[]`)
	pkg, err := s.c.Execute(str, []interface{}{2, j}...)
	require.NoError(s.T(), err)
	require.Equal(s.T(), uint64(1), pkg.AffectedRows)
}

func (s *clientTestSuite) TestConn_Select() {
	str := `select str, f, e from mixer_test_conn where id = 1`

	result, err := s.c.Execute(str)
	require.NoError(s.T(), err)
	require.Len(s.T(), result.Fields, 3)
	require.Len(s.T(), result.Values, 1)

	ss, _ := result.GetString(0, 0)
	require.Equal(s.T(), "a", ss)

	f, _ := result.GetFloat(0, 1)
	require.Equal(s.T(), 3.14, f)

	e, _ := result.GetString(0, 2)
	require.Equal(s.T(), "test1", e)

	ss, _ = result.GetStringByName(0, "str")
	require.Equal(s.T(), "a", ss)

	f, _ = result.GetFloatByName(0, "f")
	require.Equal(s.T(), 3.14, f)

	e, _ = result.GetStringByName(0, "e")
	require.Equal(s.T(), "test1", e)
}

func (s *clientTestSuite) TestConn_Select2() {
	str := `insert into test.mixer_test_conn (id, str, f, e, u, i,j) values (6, "a", 3.14, "test1", 255, -127,'{"key1": 1, "key2": null}'),(7, NULL,NULL, NULL, NULL, NULL,NULL)`
	result, err := s.c.Execute(str)
	require.NoError(s.T(), err)
	require.Equal(s.T(), uint64(2), result.AffectedRows)

	str = `select * from mixer_test_conn where id in (6,7)`
	ss := [][]string{{"6", "a", "3.14", "test1", "255", "-127", "{\"key1\": 1, \"key2\": null}"}, {"7", "", "", "", "", "", ""}}
	result, err = s.c.Execute(str)
	require.NoError(s.T(), err)

	require.Len(s.T(), result.Values, 2)

	for i, row := range result.Values {
		for idx, v := range row {
			require.Equal(s.T(), v.String(), ss[i][idx])
		}
	}
}

func (s *clientTestSuite) TestConn_Escape() {
	e := `""''\abc`
	str := fmt.Sprintf(`insert into mixer_test_conn (id, str) values(5, "%s")`,
		mysql.Escape(e))

	_, err := s.c.Execute(str)
	require.NoError(s.T(), err)

	str = `select str from mixer_test_conn where id = ?`

	r, err := s.c.Execute(str, 5)
	require.NoError(s.T(), err)

	ss, _ := r.GetString(0, 0)
	require.Equal(s.T(), e, ss)
}

func (s *clientTestSuite) TestConn_SetCharset() {
	err := s.c.SetCharset("gb2312")
	require.NoError(s.T(), err)

	err = s.c.SetCharset("utf8")
	require.NoError(s.T(), err)
}

func (s *clientTestSuite) testStmt_DropTable() {
	str := `drop table if exists mixer_test_stmt`

	stmt, err := s.c.Prepare(str)
	require.NoError(s.T(), err)

	defer stmt.Close()

	_, err = stmt.Execute()
	require.NoError(s.T(), err)
}

func (s *clientTestSuite) testStmt_CreateTable() {
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
	require.NoError(s.T(), err)

	defer stmt.Close()

	_, err = stmt.Execute()
	require.NoError(s.T(), err)
}

func (s *clientTestSuite) TestStmt_Delete() {
	str := `delete from mixer_test_stmt`

	stmt, err := s.c.Prepare(str)
	require.NoError(s.T(), err)

	defer stmt.Close()

	_, err = stmt.Execute()
	require.NoError(s.T(), err)
}

func (s *clientTestSuite) TestStmt_Insert() {
	str := `insert into mixer_test_stmt (id, str, f, e, u, i) values (?, ?, ?, ?, ?, ?)`

	stmt, err := s.c.Prepare(str)
	require.NoError(s.T(), err)

	defer stmt.Close()

	r, err := stmt.Execute(1, "a", 3.14, "test1", 255, -127)
	require.NoError(s.T(), err)

	require.Equal(s.T(), uint64(1), r.AffectedRows)
}

func (s *clientTestSuite) TestStmt_Select() {
	str := `select str, f, e from mixer_test_stmt where id = ?`

	stmt, err := s.c.Prepare(str)
	require.NoError(s.T(), err)

	defer stmt.Close()

	result, err := stmt.Execute(1)
	require.NoError(s.T(), err)
	require.Len(s.T(), result.Fields, 3)
	require.Len(s.T(), result.Values, 1)

	ss, _ := result.GetString(0, 0)
	require.Equal(s.T(), "a", ss)

	f, _ := result.GetFloat(0, 1)
	require.Equal(s.T(), 3.14, f)

	e, _ := result.GetString(0, 2)
	require.Equal(s.T(), "test1", e)

	ss, _ = result.GetStringByName(0, "str")
	require.Equal(s.T(), "a", ss)

	f, _ = result.GetFloatByName(0, "f")
	require.Equal(s.T(), 3.14, f)

	e, _ = result.GetStringByName(0, "e")
	require.Equal(s.T(), "test1", e)
}

func (s *clientTestSuite) TestStmt_NULL() {
	str := `insert into mixer_test_stmt (id, str, f, e) values (?, ?, ?, ?)`

	stmt, err := s.c.Prepare(str)
	require.NoError(s.T(), err)

	defer stmt.Close()

	result, err := stmt.Execute(2, nil, 3.14, nil)
	require.NoError(s.T(), err)
	require.Equal(s.T(), uint64(1), result.AffectedRows)

	stmt.Close()

	str = `select * from mixer_test_stmt where id = ?`
	stmt, err = s.c.Prepare(str)
	require.NoError(s.T(), err)

	defer stmt.Close()

	result, err = stmt.Execute(2)
	require.NoError(s.T(), err)

	b, err := result.IsNullByName(0, "id")
	require.NoError(s.T(), err)
	require.False(s.T(), b)

	b, err = result.IsNullByName(0, "str")
	require.NoError(s.T(), err)
	require.True(s.T(), b)

	b, err = result.IsNullByName(0, "f")
	require.NoError(s.T(), err)
	require.False(s.T(), b)

	b, err = result.IsNullByName(0, "e")
	require.NoError(s.T(), err)
	require.True(s.T(), b)
}

func (s *clientTestSuite) TestStmt_Unsigned() {
	str := `insert into mixer_test_stmt (id, u) values (?, ?)`

	stmt, err := s.c.Prepare(str)
	require.NoError(s.T(), err)
	defer stmt.Close()

	result, err := stmt.Execute(3, uint8(255))
	require.NoError(s.T(), err)
	require.Equal(s.T(), uint64(1), result.AffectedRows)

	str = `select u from mixer_test_stmt where id = ?`

	stmt, err = s.c.Prepare(str)
	require.NoError(s.T(), err)
	defer stmt.Close()

	result, err = stmt.Execute(3)
	require.NoError(s.T(), err)

	u, err := result.GetUint(0, 0)
	require.NoError(s.T(), err)
	require.Equal(s.T(), uint64(255), u)
}

func (s *clientTestSuite) TestStmt_Signed() {
	str := `insert into mixer_test_stmt (id, i) values (?, ?)`

	stmt, err := s.c.Prepare(str)
	require.NoError(s.T(), err)
	defer stmt.Close()

	_, err = stmt.Execute(4, 127)
	require.NoError(s.T(), err)

	_, err = stmt.Execute(uint64(18446744073709551516), int8(-128))
	require.NoError(s.T(), err)
}

func (s *clientTestSuite) TestStmt_Trans() {
	_, err := s.c.Execute(`insert into mixer_test_stmt (id, str) values (1002, "abc")`)
	require.NoError(s.T(), err)

	err = s.c.Begin()
	require.NoError(s.T(), err)

	str := `select str from mixer_test_stmt where id = ?`

	stmt, err := s.c.Prepare(str)
	require.NoError(s.T(), err)

	defer stmt.Close()

	_, err = stmt.Execute(1002)
	require.NoError(s.T(), err)

	err = s.c.Commit()
	require.NoError(s.T(), err)

	r, err := stmt.Execute(1002)
	require.NoError(s.T(), err)

	str, _ = r.GetString(0, 0)
	require.Equal(s.T(), `abc`, str)
}
