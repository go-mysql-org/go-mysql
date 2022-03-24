package client

import (
	"fmt"
	"strings"

	. "github.com/pingcap/check"

	"github.com/go-mysql-org/go-mysql/mysql"
)

type connTestSuite struct {
	c    *Conn
	port string
}

func (s *connTestSuite) SetUpSuite(c *C) {
	var err error
	addr := fmt.Sprintf("%s:%s", *testHost, s.port)
	s.c, err = Connect(addr, *testUser, *testPassword, "", func(c *Conn) {
		// required for the ExecuteMultiple test
		c.SetCapability(mysql.CLIENT_MULTI_STATEMENTS)
	})
	if err != nil {
		c.Fatal(err)
	}

	_, err = s.c.Execute("CREATE DATABASE IF NOT EXISTS " + *testDB)
	c.Assert(err, IsNil)

	_, err = s.c.Execute("USE " + *testDB)
	c.Assert(err, IsNil)

	s.testExecute_CreateTable(c)
}

func (s *connTestSuite) TearDownSuite(c *C) {
	if s.c == nil {
		return
	}

	s.testExecute_DropTable(c)

	if s.c != nil {
		s.c.Close()
	}
}

var (
	testExecuteSelectStreamingRows      = [...]string{"foo", "helloworld", "bar", "", "spam"}
	testExecuteSelectStreamingTablename = "execute_plain_table"
)

func (s *connTestSuite) testExecute_CreateTable(c *C) {
	str := `CREATE TABLE IF NOT EXISTS ` + testExecuteSelectStreamingTablename + ` (
          id INT UNSIGNED NOT NULL,
          str VARCHAR(256),
          PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8`

	result, err := s.c.Execute(str)
	c.Assert(err, IsNil)
	result.Close()

	result, err = s.c.Execute(`TRUNCATE TABLE ` + testExecuteSelectStreamingTablename)
	c.Assert(err, IsNil)
	result.Close()

	stmt, err := s.c.Prepare(`INSERT INTO ` + testExecuteSelectStreamingTablename + ` (id, str) VALUES (?, ?)`)
	c.Assert(err, IsNil)
	defer stmt.Close()

	for id, str := range testExecuteSelectStreamingRows {
		result, err := stmt.Execute(id, str)
		c.Assert(err, IsNil)
		result.Close()
	}
}

func (s *connTestSuite) testExecute_DropTable(c *C) {
	_, err := s.c.Execute(`drop table if exists ` + testExecuteSelectStreamingTablename)
	c.Assert(err, IsNil)
}

func (s *connTestSuite) TestExecuteMultiple(c *C) {
	queries := []string{
		`INSERT INTO ` + testExecuteSelectStreamingTablename + ` (id, str) VALUES (999, "executemultiple")`,
		`SELECT id FROM ` + testExecuteSelectStreamingTablename + ` LIMIT 2`,
		`DELETE FROM ` + testExecuteSelectStreamingTablename + ` WHERE id=999`,
		`THIS IS BOGUS()`,
	}

	count := 0
	result, err := s.c.ExecuteMultiple(strings.Join(queries, "; "), func(result *mysql.Result, err error) {
		switch count {
		// the INSERT/DELETE query have no resultset, but should have set affectedrows
		// the err should be nil
		// also, since this is not the last query, the SERVER_MORE_RESULTS_EXISTS
		// flag should be set
		case 0, 2:
			c.Assert(result.Status&mysql.SERVER_MORE_RESULTS_EXISTS, Not(Equals), 0)
			c.Assert(result.Resultset, IsNil)
			c.Assert(result.AffectedRows, Equals, uint64(1))
			c.Assert(err, IsNil)
		case 1:
			// the SELECT query should have an resultset
			// still not the last query, flag should be set
			c.Assert(result.Status&mysql.SERVER_MORE_RESULTS_EXISTS, Not(Equals), 0)
			c.Assert(result.Resultset, NotNil)
			c.Assert(err, IsNil)
		case 3:
			// this query is obviously bogus so the error should be non-nil
			c.Assert(result, IsNil)
			c.Assert(err, NotNil)
		}
		count++
	})

	c.Assert(count, Equals, 4)
	c.Assert(err, IsNil)
	c.Assert(result.StreamingDone, Equals, true)
	c.Assert(result.Streaming, Equals, mysql.StreamingMultiple)
}

func (s *connTestSuite) TestExecuteSelectStreaming(c *C) {
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
			c.Assert(row, HasLen, colsInResult)
			// Check type of columns
			c.Assert(row[0].Type, Equals, mysql.FieldValueType(mysql.FieldValueTypeUnsigned))
			c.Assert(row[1].Type, Equals, mysql.FieldValueType(mysql.FieldValueTypeString))

			id := row[0].AsInt64()
			str := row[1].AsString()

			// Check order of rows
			c.Assert(id, Equals, expectedRowId)
			// Check string values (protection from incorrect reuse of memory)
			c.Assert(string(str), Equals, testExecuteSelectStreamingRows[id])

			expectedRowId++

			return nil
		}, func(result *mysql.Result) error {
			// result.Resultset must be defined at this point
			c.Assert(result.Resultset, NotNil)
			// Check number of columns
			c.Assert(result.Resultset.Fields, HasLen, colsInResult)

			perResultCallbackCalledTimes++
			return nil
		})
	c.Assert(err, IsNil)

	// Check total rows count
	c.Assert(expectedRowId, Equals, int64(len(testExecuteSelectStreamingRows)))

	// Check perResultCallback call count
	c.Assert(perResultCallbackCalledTimes, Equals, 1)
}
