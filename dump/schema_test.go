package dump

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/test_util"
)

type schemaTestSuite struct {
	suite.Suite
	conn *client.Conn
	d    *Dumper
}

func TestSchemaSuite(t *testing.T) {
	suite.Run(t, new(schemaTestSuite))
}

func (s *schemaTestSuite) SetupSuite() {
	addr := fmt.Sprintf("%s:%s", *test_util.MysqlHost, *test_util.MysqlPort)

	var err error
	s.conn, err = client.Connect(addr, "root", "", "")
	require.NoError(s.T(), err)

	s.d, err = NewDumper(*execution, addr, "root", "")
	require.NoError(s.T(), err)
	require.NotNil(s.T(), s.d)

	s.d.SetCharset("utf8")
	s.d.SetErrOut(os.Stderr)

	_, err = s.conn.Execute("CREATE DATABASE IF NOT EXISTS test1")
	require.NoError(s.T(), err)

	_, err = s.conn.Execute("CREATE DATABASE IF NOT EXISTS test2")
	require.NoError(s.T(), err)

	str := `CREATE TABLE IF NOT EXISTS test%d.t%d (
			id int AUTO_INCREMENT,
			name varchar(256),
			PRIMARY KEY(id)
			) ENGINE=INNODB`
	_, err = s.conn.Execute(fmt.Sprintf(str, 1, 1))
	require.NoError(s.T(), err)

	_, err = s.conn.Execute(fmt.Sprintf(str, 2, 1))
	require.NoError(s.T(), err)

	_, err = s.conn.Execute(fmt.Sprintf(str, 1, 2))
	require.NoError(s.T(), err)

	_, err = s.conn.Execute(fmt.Sprintf(str, 2, 2))
	require.NoError(s.T(), err)

	str = `INSERT INTO test%d.t%d (name) VALUES ("a"), ("b"), ("\\"), ("''")`

	_, err = s.conn.Execute(fmt.Sprintf(str, 1, 1))
	require.NoError(s.T(), err)

	_, err = s.conn.Execute(fmt.Sprintf(str, 2, 1))
	require.NoError(s.T(), err)

	_, err = s.conn.Execute(fmt.Sprintf(str, 1, 2))
	require.NoError(s.T(), err)

	_, err = s.conn.Execute(fmt.Sprintf(str, 2, 2))
	require.NoError(s.T(), err)
}

func (s *schemaTestSuite) TearDownSuite() {
	if s.conn != nil {
		_, err := s.conn.Execute("DROP DATABASE IF EXISTS test1")
		require.NoError(s.T(), err)

		_, err = s.conn.Execute("DROP DATABASE IF EXISTS test2")
		require.NoError(s.T(), err)

		s.conn.Close()
	}
}

func (s *schemaTestSuite) TestDump() {
	// Using mysql 5.7 can't work, error:
	// 	mysqldump: Error 1412: Table definition has changed,
	// 	please retry transaction when dumping table `test_replication` at row: 0
	// err := s.d.Dump(io.Discard)
	// c.Assert(err, IsNil)

	s.d.AddDatabases("test1", "test2")

	s.d.AddIgnoreTables("test1", "t2")

	err := s.d.Dump(io.Discard)
	require.NoError(s.T(), err)

	s.d.AddTables("test1", "t1")

	err = s.d.Dump(io.Discard)
	require.NoError(s.T(), err)
}

func (s *schemaTestSuite) TestParse() {
	var buf bytes.Buffer

	s.d.Reset()

	s.d.AddDatabases("test1", "test2")

	err := s.d.Dump(&buf)
	require.NoError(s.T(), err)

	err = Parse(&buf, new(testParseHandler), true)
	require.NoError(s.T(), err)
}
