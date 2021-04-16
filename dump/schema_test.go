package dump

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/go-mysql-org/go-mysql/client"
	. "github.com/pingcap/check"
)

type schemaTestSuite struct {
	conn *client.Conn
	d    *Dumper
}

var _ = Suite(&schemaTestSuite{})

func (s *schemaTestSuite) SetUpSuite(c *C) {
	var err error
	s.conn, err = client.Connect(fmt.Sprintf("%s:%d", *host, *port), "root", "", "")
	c.Assert(err, IsNil)

	s.d, err = NewDumper(*execution, fmt.Sprintf("%s:%d", *host, *port), "root", "")
	c.Assert(err, IsNil)
	c.Assert(s.d, NotNil)

	s.d.SetCharset("utf8")
	s.d.SetErrOut(os.Stderr)

	_, err = s.conn.Execute("CREATE DATABASE IF NOT EXISTS test1")
	c.Assert(err, IsNil)

	_, err = s.conn.Execute("CREATE DATABASE IF NOT EXISTS test2")
	c.Assert(err, IsNil)

	str := `CREATE TABLE IF NOT EXISTS test%d.t%d (
			id int AUTO_INCREMENT,
			name varchar(256),
			PRIMARY KEY(id)
			) ENGINE=INNODB`
	_, err = s.conn.Execute(fmt.Sprintf(str, 1, 1))
	c.Assert(err, IsNil)

	_, err = s.conn.Execute(fmt.Sprintf(str, 2, 1))
	c.Assert(err, IsNil)

	_, err = s.conn.Execute(fmt.Sprintf(str, 1, 2))
	c.Assert(err, IsNil)

	_, err = s.conn.Execute(fmt.Sprintf(str, 2, 2))
	c.Assert(err, IsNil)

	str = `INSERT INTO test%d.t%d (name) VALUES ("a"), ("b"), ("\\"), ("''")`

	_, err = s.conn.Execute(fmt.Sprintf(str, 1, 1))
	c.Assert(err, IsNil)

	_, err = s.conn.Execute(fmt.Sprintf(str, 2, 1))
	c.Assert(err, IsNil)

	_, err = s.conn.Execute(fmt.Sprintf(str, 1, 2))
	c.Assert(err, IsNil)

	_, err = s.conn.Execute(fmt.Sprintf(str, 2, 2))
	c.Assert(err, IsNil)
}

func (s *schemaTestSuite) TearDownSuite(c *C) {
	if s.conn != nil {
		_, err := s.conn.Execute("DROP DATABASE IF EXISTS test1")
		c.Assert(err, IsNil)

		_, err = s.conn.Execute("DROP DATABASE IF EXISTS test2")
		c.Assert(err, IsNil)

		s.conn.Close()
	}
}

func (s *schemaTestSuite) TestDump(c *C) {
	// Using mysql 5.7 can't work, error:
	// 	mysqldump: Error 1412: Table definition has changed,
	// 	please retry transaction when dumping table `test_replication` at row: 0
	// err := s.d.Dump(ioutil.Discard)
	// c.Assert(err, IsNil)

	s.d.AddDatabases("test1", "test2")

	s.d.AddIgnoreTables("test1", "t2")

	err := s.d.Dump(ioutil.Discard)
	c.Assert(err, IsNil)

	s.d.AddTables("test1", "t1")

	err = s.d.Dump(ioutil.Discard)
	c.Assert(err, IsNil)
}

func (s *schemaTestSuite) TestParse(c *C) {
	var buf bytes.Buffer

	s.d.Reset()

	s.d.AddDatabases("test1", "test2")

	err := s.d.Dump(&buf)
	c.Assert(err, IsNil)

	err = Parse(&buf, new(testParseHandler), true)
	c.Assert(err, IsNil)
}
