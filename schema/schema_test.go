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

var (
	schema = flag.String("schema", "test", "MySQL Database")
	pwd    = flag.String("pwd", "", "MySQL password")
)

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

func (s *schemaTestSuite) TestSchemaWithMultiValueIndex() {
	_, err := s.conn.Execute(`DROP TABLE IF EXISTS multi_value_idx_test`)
	require.NoError(s.T(), err)

	str := `
        CREATE TABLE IF NOT EXISTS multi_value_idx_test (
            id INT,
            entries json,
            PRIMARY KEY(id)
        ) ENGINE = INNODB;
    `

	_, err = s.conn.Execute(str)
	require.NoError(s.T(), err)

	str = `CREATE INDEX idx_entries ON multi_value_idx_test((CAST((entries->'$') AS CHAR(64))));`
	_, err = s.conn.Execute(str)
	require.NoError(s.T(), err)

	ta, err := NewTable(s.conn, *schema, "multi_value_idx_test")
	require.NoError(s.T(), err)

	require.Len(s.T(), ta.Indexes, 2)

	require.Equal(s.T(), "PRIMARY", ta.Indexes[0].Name)
	require.Len(s.T(), ta.Indexes[0].Columns, 1)
	require.Equal(s.T(), "id", ta.Indexes[0].Columns[0])

	require.Equal(s.T(), "idx_entries", ta.Indexes[1].Name)
	require.Len(s.T(), ta.Indexes[1].Columns, 1)
	require.Equal(s.T(), "", ta.Indexes[1].Columns[0])

	taSqlDb, err := NewTableFromSqlDB(s.sqlDB, *schema, "multi_value_idx_test")
	require.NoError(s.T(), err)

	require.Equal(s.T(), ta, taSqlDb)
}

func (s *schemaTestSuite) TestSchemaWithInvisibleIndex() {
	_, err := s.conn.Execute(`DROP TABLE IF EXISTS invisible_idx_test`)
	require.NoError(s.T(), err)

	// Create table first to check invisible index support via column presence
	str := `
        CREATE TABLE IF NOT EXISTS invisible_idx_test (
            id INT,
            name VARCHAR(256),
            email VARCHAR(256),
            PRIMARY KEY(id),
            INDEX name_idx (name),
            INDEX email_idx (email)
        ) ENGINE = INNODB;
    `

	_, err = s.conn.Execute(str)
	require.NoError(s.T(), err)

	// Check if invisible index support exists by checking SHOW INDEX columns
	r, err := s.conn.Execute(fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", *schema, "invisible_idx_test"))
	require.NoError(s.T(), err)
	hasInvisibleIndex := hasInvisibleIndexSupportFromResult(r)

	// Add INVISIBLE keyword only if database supports it
	if hasInvisibleIndex {
		_, err = s.conn.Execute(`ALTER TABLE invisible_idx_test ALTER INDEX name_idx INVISIBLE`)
		require.NoError(s.T(), err)
	}

	ta, err := NewTable(s.conn, *schema, "invisible_idx_test")
	require.NoError(s.T(), err)

	require.Len(s.T(), ta.Indexes, 3)

	// PRIMARY key should always be visible
	require.Equal(s.T(), "PRIMARY", ta.Indexes[0].Name)
	require.True(s.T(), ta.Indexes[0].Visible)

	// Find name_idx and email_idx (order may vary)
	var nameIdx, emailIdx *Index
	for _, idx := range ta.Indexes {
		switch idx.Name {
		case "name_idx":
			nameIdx = idx
		case "email_idx":
			emailIdx = idx
		}
	}

	require.NotNil(s.T(), nameIdx)
	require.NotNil(s.T(), emailIdx)

	// email_idx should always be visible (default)
	require.True(s.T(), emailIdx.Visible)

	// name_idx visibility depends on database support for invisible indexes
	if hasInvisibleIndex {
		require.False(s.T(), nameIdx.Visible, "name_idx should be invisible when database supports invisible indexes")
	} else {
		require.True(s.T(), nameIdx.Visible, "name_idx should be visible when database doesn't support invisible indexes")
	}

	taSqlDb, err := NewTableFromSqlDB(s.sqlDB, *schema, "invisible_idx_test")
	require.NoError(s.T(), err)

	require.Equal(s.T(), ta, taSqlDb)
}

func (s *schemaTestSuite) TestInvisibleIndexColumnDetection() {
	_, err := s.conn.Execute(`DROP TABLE IF EXISTS column_detection_test`)
	require.NoError(s.T(), err)

	str := `
        CREATE TABLE IF NOT EXISTS column_detection_test (
            id INT PRIMARY KEY,
            name VARCHAR(256),
            INDEX name_idx (name)
        ) ENGINE = INNODB;
    `

	_, err = s.conn.Execute(str)
	require.NoError(s.T(), err)

	// Test both detection functions work consistently
	r, err := s.conn.Execute(fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", *schema, "column_detection_test"))
	require.NoError(s.T(), err)

	hasInvisibleFromResult := hasInvisibleIndexSupportFromResult(r)

	// Get columns and test the other detection function
	cols, err := s.sqlDB.Query(fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", *schema, "column_detection_test"))
	require.NoError(s.T(), err)
	defer cols.Close()

	columnNames, err := cols.Columns()
	require.NoError(s.T(), err)
	hasInvisibleFromColumns := hasInvisibleIndexSupportFromColumns(columnNames)

	// Both detection methods should agree
	require.Equal(s.T(), hasInvisibleFromResult, hasInvisibleFromColumns, "Detection methods should be consistent")

	// Test that both connection types work identically
	ta1, err := NewTable(s.conn, *schema, "column_detection_test")
	require.NoError(s.T(), err)

	ta2, err := NewTableFromSqlDB(s.sqlDB, *schema, "column_detection_test")
	require.NoError(s.T(), err)

	require.Equal(s.T(), ta1, ta2, "Both connection types should produce identical results")
}

func TestInvisibleIndexLogic(t *testing.T) {
	// Test MySQL-style visibility logic
	require.True(t, isIndexInvisible("NO"), "Visible=NO should be invisible")
	require.False(t, isIndexInvisible("YES"), "Visible=YES should be visible")

	// Test case insensitivity
	require.True(t, isIndexInvisible("no"), "Should be case insensitive")
	require.True(t, isIndexInvisible("No"), "Should be case insensitive")
	require.False(t, isIndexInvisible("yes"), "Should be case insensitive")
	require.False(t, isIndexInvisible("YES"), "Should be case insensitive")

	// Test other values default to visible
	require.False(t, isIndexInvisible(""), "Empty string should default to visible")
	require.False(t, isIndexInvisible("UNKNOWN"), "Unknown value should default to visible")
}

func TestIndexVisibilityDefault(t *testing.T) {
	// Test that NewIndex creates visible indexes by default
	idx := NewIndex("test_index")
	require.True(t, idx.Visible)

	// Test AddIndex creates visible indexes by default
	ta := &Table{Schema: "test", Name: "test_table"}
	addedIdx := ta.AddIndex("added_index")
	require.True(t, addedIdx.Visible)
}

func (s *schemaTestSuite) TestVisibleFieldInSchema() {
	_, err := s.conn.Execute(`DROP TABLE IF EXISTS visible_field_test`)
	require.NoError(s.T(), err)

	str := `
        CREATE TABLE IF NOT EXISTS visible_field_test (
            id INT,
            name VARCHAR(256),
            PRIMARY KEY(id),
            INDEX name_idx (name)
        ) ENGINE = INNODB;
    `

	_, err = s.conn.Execute(str)
	require.NoError(s.T(), err)

	ta, err := NewTable(s.conn, *schema, "visible_field_test")
	require.NoError(s.T(), err)

	// All indexes should be visible by default
	for _, idx := range ta.Indexes {
		require.True(s.T(), idx.Visible, "Index %s should be visible by default", idx.Name)
	}

	// Test with SQL DB connection as well
	taSqlDb, err := NewTableFromSqlDB(s.sqlDB, *schema, "visible_field_test")
	require.NoError(s.T(), err)

	for _, idx := range taSqlDb.Indexes {
		require.True(s.T(), idx.Visible, "Index %s should be visible by default (SQL DB)", idx.Name)
	}
}
