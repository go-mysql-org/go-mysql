package driver

import (
	"flag"
	"fmt"
	"net/url"
	"reflect"
	"testing"

	"github.com/jmoiron/sqlx"
	. "github.com/pingcap/check"
)

// Use docker mysql to test, mysql is 3306
var testHost = flag.String("host", "127.0.0.1", "MySQL master host")

// possible choices for different MySQL versions are: 5561,5641,3306,5722,8003,8012
var testPort = flag.Int("port", 3306, "MySQL server port")
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
	addr := fmt.Sprintf("%s:%d", *testHost, *testPort)
	dsn := fmt.Sprintf("%s:%s@%s?%s", *testUser, *testPassword, addr, *testDB)

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
		"user:password@localhost?db":                 connInfo{standardDSN: false, addr: "localhost", user: "user", password: "password", db: "db", params: url.Values{}},
		"user@1.domain.com?db":                       connInfo{standardDSN: false, addr: "1.domain.com", user: "user", password: "", db: "db", params: url.Values{}},
		"user:password@2.domain.com/db":              connInfo{standardDSN: true, addr: "2.domain.com", user: "user", password: "password", db: "db", params: url.Values{}},
		"user:password@3.domain.com/db?ssl=true":     connInfo{standardDSN: true, addr: "3.domain.com", user: "user", password: "password", db: "db", params: url.Values{"ssl": []string{"true"}}},
		"user:password@4.domain.com/db?ssl=custom":   connInfo{standardDSN: true, addr: "4.domain.com", user: "user", password: "password", db: "db", params: url.Values{"ssl": []string{"custom"}}},
		"user:password@5.domain.com/db?unused=param": connInfo{standardDSN: true, addr: "5.domain.com", user: "user", password: "password", db: "db", params: url.Values{"unused": []string{"param"}}},
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
