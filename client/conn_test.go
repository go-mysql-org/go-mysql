package client

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/test_util"
)

type connTestSuite struct {
	suite.Suite
	c    *Conn
	port string
}

func TestConnSuite(t *testing.T) {
	segs := strings.Split(*test_util.MysqlPort, ",")
	for _, seg := range segs {
		suite.Run(t, &connTestSuite{port: seg})
	}
}

func (s *connTestSuite) SetupSuite() {
	var err error
	addr := fmt.Sprintf("%s:%s", *test_util.MysqlHost, s.port)
	s.c, err = Connect(addr, *testUser, *testPassword, "", func(c *Conn) {
		// required for the ExecuteMultiple test
		c.SetCapability(mysql.CLIENT_MULTI_STATEMENTS)
		c.SetAttributes(map[string]string{"attrtest": "attrvalue"})
	})
	require.NoError(s.T(), err)

	_, err = s.c.Execute("CREATE DATABASE IF NOT EXISTS " + *testDB)
	require.NoError(s.T(), err)

	_, err = s.c.Execute("USE " + *testDB)
	require.NoError(s.T(), err)

	s.testExecute_CreateTable()
}

func (s *connTestSuite) TearDownSuite() {
	if s.c == nil {
		return
	}

	s.testExecute_DropTable()

	if s.c != nil {
		s.c.Close()
	}
}

var (
	testExecuteSelectStreamingRows      = [...]string{"foo", "helloworld", "bar", "", "spam"}
	testExecuteSelectStreamingTablename = "execute_plain_table"
)

func (s *connTestSuite) testExecute_CreateTable() {
	str := `CREATE TABLE IF NOT EXISTS ` + testExecuteSelectStreamingTablename + ` (
          id INT UNSIGNED NOT NULL,
          str VARCHAR(256),
          PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8`

	result, err := s.c.Execute(str)
	require.NoError(s.T(), err)
	result.Close()

	result, err = s.c.Execute(`TRUNCATE TABLE ` + testExecuteSelectStreamingTablename)
	require.NoError(s.T(), err)
	result.Close()

	stmt, err := s.c.Prepare(`INSERT INTO ` + testExecuteSelectStreamingTablename + ` (id, str) VALUES (?, ?)`)
	require.NoError(s.T(), err)
	defer stmt.Close()

	for id, str := range testExecuteSelectStreamingRows {
		result, err := stmt.Execute(id, str)
		require.NoError(s.T(), err)
		result.Close()
	}
}

func (s *connTestSuite) testExecute_DropTable() {
	_, err := s.c.Execute(`drop table if exists ` + testExecuteSelectStreamingTablename)
	require.NoError(s.T(), err)
}

func (s *connTestSuite) TestFieldList() {
	fields, err := s.c.FieldList(testExecuteSelectStreamingTablename, "")
	require.NoError(s.T(), err)
	require.Len(s.T(), fields, 2)
}

func (s *connTestSuite) TestExecuteMultiple() {
	queries := []string{
		`INSERT INTO ` + testExecuteSelectStreamingTablename + ` (id, str) VALUES (999, "executemultiple")`,
		`SELECT id FROM ` + testExecuteSelectStreamingTablename + ` LIMIT 2`,
		`DELETE FROM ` + testExecuteSelectStreamingTablename + ` WHERE id=999`,
		`THIS IS BOGUS()`,
	}

	_, err := s.c.Execute("USE " + *testDB)
	require.Nil(s.T(), err)

	count := 0
	result, err := s.c.ExecuteMultiple(strings.Join(queries, "; "), func(result *mysql.Result, err error) {
		switch count {
		// the INSERT/DELETE query have no resultset, but should have set affectedrows
		// the err should be nil
		// also, since this is not the last query, the SERVER_MORE_RESULTS_EXISTS
		// flag should be set
		case 0, 2:
			require.True(s.T(), result.Status&mysql.SERVER_MORE_RESULTS_EXISTS > 0)
			require.Nil(s.T(), result.Resultset)
			require.Equal(s.T(), uint64(1), result.AffectedRows)
			require.NoError(s.T(), err)
		case 1:
			// the SELECT query should have an resultset
			// still not the last query, flag should be set
			require.True(s.T(), result.Status&mysql.SERVER_MORE_RESULTS_EXISTS > 0)
			require.NotNil(s.T(), result.Resultset)
			require.NoError(s.T(), err)
		case 3:
			// this query is obviously bogus so the error should be non-nil
			require.Nil(s.T(), result)
			require.Error(s.T(), err)
		}
		count++
	})

	require.Equal(s.T(), 4, count)
	require.NoError(s.T(), err)
	require.True(s.T(), result.StreamingDone)
	require.Equal(s.T(), mysql.StreamingMultiple, result.Streaming)
}

func (s *connTestSuite) TestExecuteSelectStreaming() {
	var (
		expectedRowId                int64
		perResultCallbackCalledTimes int
		result                       mysql.Result
	)

	const colsInResult = 2 // id, str

	err := s.c.ExecuteSelectStreaming(`SELECT id, str FROM `+testExecuteSelectStreamingTablename+` ORDER BY id`,
		&result,
		func(row []mysql.FieldValue) error {
			// Check number of columns
			require.Len(s.T(), row, colsInResult)
			// Check type of columns
			require.Equal(s.T(), mysql.FieldValueType(mysql.FieldValueTypeUnsigned), row[0].Type)
			require.Equal(s.T(), mysql.FieldValueType(mysql.FieldValueTypeString), row[1].Type)

			id := row[0].AsInt64()
			str := row[1].AsString()

			// Check order of rows
			require.Equal(s.T(), expectedRowId, id)
			// Check string values (protection from incorrect reuse of memory)
			require.Equal(s.T(), testExecuteSelectStreamingRows[id], string(str))

			expectedRowId++

			return nil
		}, func(result *mysql.Result) error {
			// result.Resultset must be defined at this point
			require.NotNil(s.T(), result.Resultset)
			// Check number of columns
			require.Len(s.T(), result.Resultset.Fields, colsInResult)

			perResultCallbackCalledTimes++
			return nil
		})
	require.NoError(s.T(), err)

	// Check total rows count
	require.Equal(s.T(), int64(len(testExecuteSelectStreamingRows)), expectedRowId)

	// Check perResultCallback call count
	require.Equal(s.T(), 1, perResultCallbackCalledTimes)
}

func (s *connTestSuite) TestAttributes() {
	// Test that both custom attributes and library set attributes are visible
	require.Equal(s.T(), "go-mysql", s.c.attributes["_client_name"])
	require.Equal(s.T(), "attrvalue", s.c.attributes["attrtest"])
}
