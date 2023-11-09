package driver

import (
	"flag"
	"fmt"
	"net/url"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/go-mysql-org/go-mysql/test_util"
)

var testUser = flag.String("user", "root", "MySQL user")
var testPassword = flag.String("pass", "", "MySQL password")
var testDB = flag.String("db", "test", "MySQL test database")

func TestDriver(t *testing.T) {
	suite.Run(t, new(testDriverSuite))
}

type testDriverSuite struct {
	suite.Suite
	db *sqlx.DB
}

func (s *testDriverSuite) SetupSuite() {
	addr := fmt.Sprintf("%s:%s", *test_util.MysqlHost, *test_util.MysqlPort)
	dsn := fmt.Sprintf("%s:%s@%s/%s", *testUser, *testPassword, addr, *testDB)

	var err error
	s.db, err = sqlx.Open("mysql", dsn)
	require.NoError(s.T(), err)
}

func (s *testDriverSuite) TearDownSuite() {
	if s.db != nil {
		s.db.Close()
	}
}

func (s *testDriverSuite) TestConn() {
	var n int
	err := s.db.Get(&n, "SELECT 1")
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, n)

	_, err = s.db.Exec("USE test")
	require.NoError(s.T(), err)
}

func (s *testDriverSuite) TestStmt() {
	stmt, err := s.db.Preparex("SELECT ? + ?")
	require.NoError(s.T(), err)

	var n int
	err = stmt.Get(&n, 1, 1)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, n)

	err = stmt.Close()
	require.NoError(s.T(), err)
}

func (s *testDriverSuite) TestTransaction() {
	tx, err := s.db.Beginx()
	require.NoError(s.T(), err)

	var n int
	err = tx.Get(&n, "SELECT 1")
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, n)

	err = tx.Commit()
	require.NoError(s.T(), err)
}

func TestParseDSN(t *testing.T) {
	// List of DSNs to test and expected results
	// Use different numbered domains to more readily see what has failed - since we
	// test in a loop we get the same line number on error
	testDSNs := map[string]connInfo{
		"user:password@localhost?db":                 {standardDSN: false, addr: "localhost", user: "user", password: "password", db: "db", params: url.Values{}},
		"user@1.domain.com?db":                       {standardDSN: false, addr: "1.domain.com", user: "user", password: "", db: "db", params: url.Values{}},
		"user:password@2.domain.com/db":              {standardDSN: true, addr: "2.domain.com", user: "user", password: "password", db: "db", params: url.Values{}},
		"user:password@3.domain.com/db?ssl=true":     {standardDSN: true, addr: "3.domain.com", user: "user", password: "password", db: "db", params: url.Values{"ssl": []string{"true"}}},
		"user:password@4.domain.com/db?ssl=custom":   {standardDSN: true, addr: "4.domain.com", user: "user", password: "password", db: "db", params: url.Values{"ssl": []string{"custom"}}},
		"user:password@5.domain.com/db?unused=param": {standardDSN: true, addr: "5.domain.com", user: "user", password: "password", db: "db", params: url.Values{"unused": []string{"param"}}},
	}

	for supplied, expected := range testDSNs {
		actual, err := parseDSN(supplied)
		require.NoError(t, err)
		// Compare that with expected
		require.Equal(t, expected, actual)
	}
}
