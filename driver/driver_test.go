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

var (
	testUser     = flag.String("user", "root", "MySQL user")
	testPassword = flag.String("pass", "", "MySQL password")
	testDB       = flag.String("db", "test", "MySQL test database")
)

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
	testDSNs := map[string]Connector{
		"user:password@localhost?db":                    {Addr: "localhost", User: "user", Password: "password", DB: "db", Params: url.Values{}},
		"user@1.domain.com?db":                          {Addr: "1.domain.com", User: "user", Password: "", DB: "db", Params: url.Values{}},
		"user:password@2.domain.com/db":                 {Addr: "2.domain.com", User: "user", Password: "password", DB: "db", Params: url.Values{}},
		"user:password@3.domain.com/db?ssl=true":        {Addr: "3.domain.com", User: "user", Password: "password", DB: "db", Params: url.Values{"ssl": []string{"true"}}},
		"user:password@3.domain.com/db?ssl=false":       {Addr: "3.domain.com", User: "user", Password: "password", DB: "db", Params: url.Values{"ssl": []string{"false"}}},
		"user:password@3.domain.com/db?ssl=skip-verify": {Addr: "3.domain.com", User: "user", Password: "password", DB: "db", Params: url.Values{"ssl": []string{"skip-verify"}}},
		"user:password@4.domain.com/db?ssl=custom":      {Addr: "4.domain.com", User: "user", Password: "password", DB: "db", Params: url.Values{"ssl": []string{"custom"}}},
		"user:password@4.domain.com/db?tls=custom":      {Addr: "4.domain.com", User: "user", Password: "password", DB: "db", Params: url.Values{"tls": []string{"custom"}}},
		"user:password@5.domain.com/db?unused=param":    {Addr: "5.domain.com", User: "user", Password: "password", DB: "db", Params: url.Values{"unused": []string{"param"}}},
		"user:password@5.domain.com/db?timeout=1s":      {Addr: "5.domain.com", User: "user", Password: "password", DB: "db", Params: url.Values{"timeout": []string{"1s"}}},
		"user:password@5.domain.com/db?readTimeout=1m":  {Addr: "5.domain.com", User: "user", Password: "password", DB: "db", Params: url.Values{"readTimeout": []string{"1m"}}},
		"user:password@5.domain.com/db?writeTimeout=1m": {Addr: "5.domain.com", User: "user", Password: "password", DB: "db", Params: url.Values{"writeTimeout": []string{"1m"}}},
		"user:password@5.domain.com/db?compress=zlib":   {Addr: "5.domain.com", User: "user", Password: "password", DB: "db", Params: url.Values{"compress": []string{"zlib"}}},
		"mysql://user:password@5.domain.com/db":         {Addr: "5.domain.com", User: "user", Password: "password", DB: "db", Params: url.Values{}},
		"user:password@5.domain.com:3307/db":            {Addr: "5.domain.com:3307", User: "user", Password: "password", DB: "db", Params: url.Values{}},
		"user:password@127.0.0.1/db":                    {Addr: "127.0.0.1", User: "user", Password: "password", DB: "db", Params: url.Values{}},
		"user:password@127.0.0.1:3308/db":               {Addr: "127.0.0.1:3308", User: "user", Password: "password", DB: "db", Params: url.Values{}},
		"user:password@tcp(127.0.0.1:3309)/db":          {Addr: "127.0.0.1:3309", User: "user", Password: "password", DB: "db", Params: url.Values{}},
		"mysql://127.0.0.1:3306":                        {Addr: "127.0.0.1:3306", User: "", Password: "", DB: "", Params: url.Values{}},

		// per the documentation in the README, the 'user:password@' is optional as are the '/db?param=X' portions of the DSN
		"6.domain.com":                  {Addr: "6.domain.com", User: "", Password: "", DB: "", Params: url.Values{}},
		"7.domain.com?db":               {Addr: "7.domain.com", User: "", Password: "", DB: "db", Params: url.Values{}},
		"8.domain.com/db":               {Addr: "8.domain.com", User: "", Password: "", DB: "db", Params: url.Values{}},
		"9.domain.com/db?compress=zlib": {Addr: "9.domain.com", User: "", Password: "", DB: "db", Params: url.Values{"compress": []string{"zlib"}}},
	}

	for supplied, expected := range testDSNs {
		actual, err := ParseDSN(supplied)
		require.NoError(t, err)
		// Compare that with expected
		require.Equal(t, expected, actual)
	}
}
