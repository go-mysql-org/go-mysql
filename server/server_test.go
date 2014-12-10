package server

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	mysql "github.com/siddontang/go-mysql/mysql"
	. "gopkg.in/check.v1"
	"net"
	"strings"
	"testing"
	"time"
)

var testAddr = flag.String("addr", "127.0.0.1:4000", "MySQL proxy server address")
var testUser = flag.String("user", "root", "MySQL user")
var testPassword = flag.String("pass", "", "MySQL password")
var testDB = flag.String("db", "test", "MySQL test database")

func Test(t *testing.T) {
	TestingT(t)
}

type serverTestSuite struct {
	db *sql.DB

	l net.Listener
}

var _ = Suite(&serverTestSuite{})

type testHandler struct {
	s *serverTestSuite
}

func (h *testHandler) UseDB(dbName string) error {
	return nil
}

func (h *testHandler) handleQuery(query string, binary bool) (*mysql.Result, error) {
	ss := strings.Split(query, " ")
	switch strings.ToLower(ss[0]) {
	case "select":
		var r *mysql.Resultset
		var err error
		//for handle go mysql driver select @@max_allowed_packet
		if strings.Contains(strings.ToLower(query), "max_allowed_packet") {
			r, err = mysql.BuildSimpleResultset([]string{"@@max_allowed_packet"}, [][]interface{}{
				[]interface{}{mysql.MaxPayloadLen},
			}, binary)
		} else {
			r, err = mysql.BuildSimpleResultset([]string{"a", "b"}, [][]interface{}{
				[]interface{}{1, "hello world"},
			}, binary)
		}

		if err != nil {
			return nil, err
		} else {
			return &mysql.Result{0, 0, 0, r}, nil
		}
	case "insert":
		return &mysql.Result{0, 1, 0, nil}, nil
	case "delete":
		return &mysql.Result{0, 0, 1, nil}, nil
	case "update":
		return &mysql.Result{0, 0, 1, nil}, nil
	case "replace":
		return &mysql.Result{0, 0, 1, nil}, nil
	default:
		return nil, fmt.Errorf("invalid query %s", query)
	}

	return nil, nil
}

func (h *testHandler) HandleQuery(query string) (*mysql.Result, error) {
	return h.handleQuery(query, false)
}

func (h *testHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	return nil, nil
}
func (h *testHandler) HandleStmtPreprare(id uint32, sql string) (*Stmt, error) {
	stmt := &Stmt{}
	stmt.ID = id
	stmt.Query = sql
	ss := strings.Split(sql, " ")
	switch strings.ToLower(ss[0]) {
	case "select":
		stmt.Columns = 2
		stmt.Params = 1
	case "insert":
		stmt.Columns = 0
		stmt.Params = 2
	case "replace":
		stmt.Columns = 0
		stmt.Params = 2
	case "update":
		stmt.Columns = 0
		stmt.Params = 1
	case "delete":
		stmt.Columns = 0
		stmt.Params = 1
	default:
		return nil, fmt.Errorf("invalid prepare %s", sql)
	}

	stmt.ResetParams()

	return stmt, nil
}

func (h *testHandler) HandleStmtExecute(s *Stmt) (*mysql.Result, error) {
	return h.handleQuery(s.Query, true)
}

func (s *serverTestSuite) SetUpSuite(c *C) {
	var err error

	s.l, err = net.Listen("tcp", *testAddr)
	c.Assert(err, IsNil)

	go s.onAccept(c)

	time.Sleep(500 * time.Millisecond)

	s.db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", *testUser, *testPassword, *testAddr, *testDB))
	c.Assert(err, IsNil)

	s.db.SetMaxIdleConns(4)
}

func (s *serverTestSuite) TearDownSuite(c *C) {
	if s.db != nil {
		s.db.Close()
	}

	if s.l != nil {
		s.l.Close()
	}
}

func (s *serverTestSuite) onAccept(c *C) {
	for {
		conn, err := s.l.Accept()
		if err != nil {
			return
		}

		go s.onConn(conn, c)
	}
}

func (s *serverTestSuite) onConn(conn net.Conn, c *C) {
	co, err := NewConn(conn, *testPassword, &testHandler{s})
	c.Assert(err, IsNil)

	for {
		err = co.HandleCommand()
		if err != nil {
			return
		}
	}
}

func (s *serverTestSuite) TestSelect(c *C) {
	var a int64
	var b string

	err := s.db.QueryRow("SELECT a, b FROM tbl WHERE id=1").Scan(&a, &b)
	c.Assert(err, IsNil)
	c.Assert(a, Equals, int64(1))
	c.Assert(b, Equals, "hello world")
}

func (s *serverTestSuite) TestExec(c *C) {
	r, err := s.db.Exec("INSERT INTO tbl (a, b) values (1, \"hello world\")")
	c.Assert(err, IsNil)
	i, _ := r.LastInsertId()
	c.Assert(i, Equals, int64(1))

	r, err = s.db.Exec("REPLACE INTO tbl (a, b) values (1, \"hello world\")")
	c.Assert(err, IsNil)
	i, _ = r.RowsAffected()
	c.Assert(i, Equals, int64(1))

	r, err = s.db.Exec("UPDATE tbl SET b = \"abc\" where a = 1")
	c.Assert(err, IsNil)
	i, _ = r.RowsAffected()
	c.Assert(i, Equals, int64(1))

	r, err = s.db.Exec("DELETE FROM tbl where a = 1")
	c.Assert(err, IsNil)
	i, _ = r.RowsAffected()
	c.Assert(i, Equals, int64(1))
}

func (s *serverTestSuite) TestStmtSelect(c *C) {
	var a int64
	var b string

	err := s.db.QueryRow("SELECT a, b FROM tbl WHERE id=?", 1).Scan(&a, &b)
	c.Assert(err, IsNil)
	c.Assert(a, Equals, int64(1))
	c.Assert(b, Equals, "hello world")
}

func (s *serverTestSuite) TestStmtExec(c *C) {
	r, err := s.db.Exec("INSERT INTO tbl (a, b) values (?, ?)", 1, "hello world")
	c.Assert(err, IsNil)
	i, _ := r.LastInsertId()
	c.Assert(i, Equals, int64(1))

	r, err = s.db.Exec("REPLACE INTO tbl (a, b) values (?, ?)", 1, "hello world")
	c.Assert(err, IsNil)
	i, _ = r.RowsAffected()
	c.Assert(i, Equals, int64(1))

	r, err = s.db.Exec("UPDATE tbl SET b = \"abc\" where a = ?", 1)
	c.Assert(err, IsNil)
	i, _ = r.RowsAffected()
	c.Assert(i, Equals, int64(1))

	r, err = s.db.Exec("DELETE FROM tbl where a = ?", 1)
	c.Assert(err, IsNil)
	i, _ = r.RowsAffected()
	c.Assert(i, Equals, int64(1))
}
