package driver

import (
	"flag"
	"fmt"
	"net/url"
	"reflect"
	"testing"

	"github.com/jmoiron/sqlx"
	. "github.com/pingcap/check"

	"github.com/dumbmachine/go-mysql/test_util"
)

var testUser = flag.String("user", "root", "MySQL user")
var testPassword = flag.String("pass", "", "MySQL password")
var testDB = flag.String("db", "test", "MySQL test database")

func TestDriver(t *testing.T) {
	TestingT(t)
}

type testDriverSuite struct {
	db *sqlx.DB
}

var _ = Suite(&testDriverSuite{})

func (s *testDriverSuite) SetUpSuite(c *C) {
	addr := fmt.Sprintf("%s:%s", *test_util.MysqlHost, *test_util.MysqlPort)
	dsn := fmt.Sprintf("%s:%s@%s/%s", *testUser, *testPassword, addr, *testDB)

	var err error
	s.db, err = sqlx.Open("mysql", dsn)
	c.Assert(err, IsNil)
}

func (s *testDriverSuite) TearDownSuite(c *C) {
	if s.db != nil {
		s.db.Close()
	}
}

func (s *testDriverSuite) TestConn(c *C) {
	var n int
	err := s.db.Get(&n, "SELECT 1")
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 1)

	_, err = s.db.Exec("USE test")
	c.Assert(err, IsNil)
}

func (s *testDriverSuite) TestStmt(c *C) {
	stmt, err := s.db.Preparex("SELECT ? + ?")
	c.Assert(err, IsNil)

	var n int
	err = stmt.Get(&n, 1, 1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 2)

	err = stmt.Close()
	c.Assert(err, IsNil)
}

func (s *testDriverSuite) TestTransaction(c *C) {
	tx, err := s.db.Beginx()
	c.Assert(err, IsNil)

	var n int
	err = tx.Get(&n, "SELECT 1")
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 1)

	err = tx.Commit()
	c.Assert(err, IsNil)
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
		if err != nil {
			t.Errorf("TestParseDSN failed. Got error: %s", err)
		}
		// Compare that with expected
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("TestParseDSN failed.\nExpected:\n%#v\nGot:\n%#v", expected, actual)
		}
	}
}
