package schema

import (
	"database/sql"
	"flag"
	"fmt"
	"testing"

	"github.com/go-mysql-org/go-mysql/client"
	_ "github.com/go-mysql-org/go-mysql/driver"
	"github.com/go-mysql-org/go-mysql/test_util"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var schema = flag.String("schema", "test", "MySQL Database")
var pwd = flag.String("pwd", "", "MySQL password")

type schemaTestSuite struct {
	suite.Suite
	conn  *client.Conn
	sqlDB *sql.DB
}

func TestSchemaSuite(t *testing.T) {
	suite.Run(t, new(schemaTestSuite))
}

func (s *schemaTestSuite) SetupSuite() {
	addr := fmt.Sprintf("%s:%s", *test_util.MysqlHost, *test_util.MysqlPort)

	var err error
	s.conn, err = client.Connect(addr, "root", *pwd, *schema)
	require.NoError(s.T(), err)

	s.sqlDB, err = sql.Open("mysql", fmt.Sprintf("root:%s@%s", *pwd, addr))
	require.NoError(s.T(), err)
}

func (s *schemaTestSuite) TearDownSuite() {
	if s.conn != nil {
		s.conn.Close()
	}

	if s.sqlDB != nil {
		s.sqlDB.Close()
	}
}

func (s *schemaTestSuite) TestSchema() {
	_, err := s.conn.Execute(`DROP TABLE IF EXISTS schema_test`)
	require.NoError(s.T(), err)

	str := `
        CREATE TABLE IF NOT EXISTS schema_test (
            id INT,
            id1 INT,
            id2 INT,
            name VARCHAR(256),
            status ENUM('appointing','serving','abnormal','stop','noaftermarket','finish','financial_audit'),
            se SET('a', 'b', 'c'),
            f FLOAT,
            d DECIMAL(2, 1),
            uint INT UNSIGNED,
            zfint INT ZEROFILL,
            name_ucs VARCHAR(256) CHARACTER SET ucs2,
            name_utf8 VARCHAR(256) CHARACTER SET utf8,
            name_char CHAR(10),
            name_binary BINARY(11),
            name_varbinary VARBINARY(12),
            PRIMARY KEY(id2, id),
            UNIQUE (id1),
            INDEX name_idx (name)
        ) ENGINE = INNODB;
    `

	_, err = s.conn.Execute(str)
	require.NoError(s.T(), err)

	ta, err := NewTable(s.conn, *schema, "schema_test")
	require.NoError(s.T(), err)

	require.Len(s.T(), ta.Columns, 15)
	require.Len(s.T(), ta.Indexes, 3)
	require.Equal(s.T(), []int{2, 0}, ta.PKColumns)
	require.True(s.T(), ta.IsPrimaryKey(0))
	require.False(s.T(), ta.IsPrimaryKey(1))
	require.True(s.T(), ta.IsPrimaryKey(2))
	require.False(s.T(), ta.IsPrimaryKey(3))
	require.Equal(s.T(), &ta.Columns[2], ta.GetPKColumn(0))
	require.Equal(s.T(), &ta.Columns[0], ta.GetPKColumn(1))
	require.Nil(s.T(), ta.GetPKColumn(2))
	require.Nil(s.T(), ta.GetPKColumn(3))
	require.Len(s.T(), ta.Indexes[0].Columns, 2)
	require.Equal(s.T(), "PRIMARY", ta.Indexes[0].Name)
	require.Equal(s.T(), "name_idx", ta.Indexes[2].Name)
	require.Equal(s.T(), TYPE_STRING, ta.Columns[3].Type)
	require.Equal(s.T(), uint(256), ta.Columns[3].MaxSize)
	require.Equal(s.T(), uint(0), ta.Columns[3].FixedSize)
	require.Equal(s.T(), []string{"appointing", "serving", "abnormal", "stop", "noaftermarket", "finish", "financial_audit"}, ta.Columns[4].EnumValues)
	require.Equal(s.T(), []string{"a", "b", "c"}, ta.Columns[5].SetValues)
	require.Equal(s.T(), TYPE_DECIMAL, ta.Columns[7].Type)
	require.False(s.T(), ta.Columns[0].IsUnsigned)
	require.True(s.T(), ta.Columns[8].IsUnsigned)
	require.True(s.T(), ta.Columns[9].IsUnsigned)
	require.Contains(s.T(), ta.Columns[10].Collation, "ucs2")
	require.Equal(s.T(), uint(256), ta.Columns[10].MaxSize)
	require.Equal(s.T(), uint(0), ta.Columns[10].FixedSize)
	require.Contains(s.T(), ta.Columns[11].Collation, "utf8")
	require.Equal(s.T(), TYPE_STRING, ta.Columns[12].Type)
	require.Equal(s.T(), uint(10), ta.Columns[12].MaxSize)
	require.Equal(s.T(), uint(10), ta.Columns[12].FixedSize)
	require.Equal(s.T(), TYPE_BINARY, ta.Columns[13].Type)
	require.Equal(s.T(), uint(11), ta.Columns[13].MaxSize)
	require.Equal(s.T(), uint(11), ta.Columns[13].FixedSize)
	require.Equal(s.T(), TYPE_BINARY, ta.Columns[14].Type)
	require.Equal(s.T(), uint(12), ta.Columns[14].MaxSize)
	require.Equal(s.T(), uint(0), ta.Columns[14].FixedSize)

	taSqlDb, err := NewTableFromSqlDB(s.sqlDB, *schema, "schema_test")
	require.NoError(s.T(), err)

	require.Equal(s.T(), ta, taSqlDb)
}

func (s *schemaTestSuite) TestQuoteSchema() {
	str := "CREATE TABLE IF NOT EXISTS `a-b_test` (`a.b` INT) ENGINE = INNODB"

	_, err := s.conn.Execute(str)
	require.NoError(s.T(), err)

	ta, err := NewTable(s.conn, *schema, "a-b_test")
	require.NoError(s.T(), err)

	require.Equal(s.T(), "a.b", ta.Columns[0].Name)
}
