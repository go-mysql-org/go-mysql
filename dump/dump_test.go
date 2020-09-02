package dump

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
)

// use docker mysql for test
var host = flag.String("host", "127.0.0.1", "MySQL host")
var port = flag.Int("port", 3306, "MySQL host")

var execution = flag.String("exec", "mysqldump", "mysqldump execution path")

func Test(t *testing.T) {
	TestingT(t)
}

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

type testParseHandler struct {
	gset mysql.GTIDSet
}

func (h *testParseHandler) BinLog(name string, pos uint64) error {
	return nil
}

func (h *testParseHandler) GtidSet(gtidsets string) (err error) {
	if h.gset != nil {
		err = h.gset.Update(gtidsets)
	} else {
		h.gset, err = mysql.ParseGTIDSet("mysql", gtidsets)
	}
	return err
}

func (h *testParseHandler) Data(schema string, table string, values []string) error {
	return nil
}

type GtidParseTest struct {
	gset mysql.GTIDSet
}

func (h *GtidParseTest) UpdateGtidSet(gtidStr string) (err error) {
	if h.gset != nil {
		err = h.gset.Update(gtidStr)
	} else {
		h.gset, err = mysql.ParseGTIDSet("mysql", gtidStr)
	}
	return err
}

func (s *parserTestSuite) TestParseGtidExp(c *C) {
	//	binlogExp := regexp.MustCompile("^CHANGE MASTER TO MASTER_LOG_FILE='(.+)', MASTER_LOG_POS=(\\d+);")
	//	gtidExp := regexp.MustCompile("(\\w{8}(-\\w{4}){3}-\\w{12}:\\d+-\\d+)")
	tbls := []struct {
		input    string
		expected string
	}{
		{`SET @@GLOBAL.GTID_PURGED='071a84e8-b253-11e8-8472-005056a27e86:1-76,
2337be48-0456-11e9-bd1c-00505690543b:1-7,
41d816cd-0455-11e9-be42-005056901a22:1-2,
5f1eea9e-b1e5-11e8-bc77-005056a221ed:1-144609156,
75848cdb-8131-11e7-b6fc-1c1b0de85e7b:1-151378598,
780ad602-0456-11e9-8bcd-005056901a22:1-516653148,
92809ddd-1e3c-11e9-9d04-00505690f6ab:1-11858565,
c59598c7-0467-11e9-bbbe-005056901a22:1-226464969,
cbd7809d-0433-11e9-b1cf-00505690543b:1-18233950,
cca778e9-8cdf-11e8-94d0-005056a247b1:1-303899574,
cf80679b-7695-11e8-8873-1c1b0d9a4ab9:1-12836047,
d0951f24-1e21-11e9-bb2e-00505690b730:1-4758092,
e7574090-b123-11e8-8bb4-005056a29643:1-12'
`, "071a84e8-b253-11e8-8472-005056a27e86:1-76,2337be48-0456-11e9-bd1c-00505690543b:1-7,41d816cd-0455-11e9-be42-005056901a22:1-2,5f1eea9e-b1e5-11e8-bc77-005056a221ed:1-144609156,75848cdb-8131-11e7-b6fc-1c1b0de85e7b:1-151378598,780ad602-0456-11e9-8bcd-005056901a22:1-516653148,92809ddd-1e3c-11e9-9d04-00505690f6ab:1-11858565,c59598c7-0467-11e9-bbbe-005056901a22:1-226464969,cbd7809d-0433-11e9-b1cf-00505690543b:1-18233950,cca778e9-8cdf-11e8-94d0-005056a247b1:1-303899574,cf80679b-7695-11e8-8873-1c1b0d9a4ab9:1-12836047,d0951f24-1e21-11e9-bb2e-00505690b730:1-4758092,e7574090-b123-11e8-8bb4-005056a29643:1-12"},
		{`SET @@GLOBAL.GTID_PURGED='071a84e8-b253-11e8-8472-005056a27e86:1-76,
2337be48-0456-11e9-bd1c-00505690543b:1-7';
`, "071a84e8-b253-11e8-8472-005056a27e86:1-76,2337be48-0456-11e9-bd1c-00505690543b:1-7"},
		{`SET @@GLOBAL.GTID_PURGED='c0977f88-3104-11e9-81e1-00505690245b:1-274559';
`, "c0977f88-3104-11e9-81e1-00505690245b:1-274559"},
		{`CHANGE MASTER TO MASTER_LOG_FILE='mysql-bin.008995', MASTER_LOG_POS=102052485;`, ""},
	}

	for _, tt := range tbls {
		reader := strings.NewReader(tt.input)
		var handler = new(testParseHandler)

		Parse(reader, handler, true)

		if tt.expected == "" {
			if handler.gset != nil {
				c.Assert(handler.gset, IsNil)
			} else {
				continue
			}
		}
		expectedGtidset, err := mysql.ParseGTIDSet("mysql", tt.expected)
		c.Assert(err, IsNil)
		c.Assert(expectedGtidset.Equal(handler.gset), IsTrue)
	}

}

func (s *parserTestSuite) TestParseFindTable(c *C) {
	tbl := []struct {
		sql   string
		table string
	}{
		{"INSERT INTO `note` VALUES ('title', 'here is sql: INSERT INTO `table` VALUES (\\'some value\\')');", "note"},
		{"INSERT INTO `note` VALUES ('1', '2', '3');", "note"},
		{"INSERT INTO `a.b` VALUES ('1');", "a.b"},
	}

	for _, t := range tbl {
		res := valuesExp.FindAllStringSubmatch(t.sql, -1)[0][1]
		c.Assert(res, Equals, t.table)
	}
}

type parserTestSuite struct {
}

var _ = Suite(&parserTestSuite{})

func (s *parserTestSuite) TestUnescape(c *C) {
	tbl := []struct {
		escaped  string
		expected string
	}{
		{`\\n`, `\n`},
		{`\\t`, `\t`},
		{`\\"`, `\"`},
		{`\\'`, `\'`},
		{`\\0`, `\0`},
		{`\\b`, `\b`},
		{`\\Z`, `\Z`},
		{`\\r`, `\r`},
		{`abc`, `abc`},
		{`abc\`, `abc`},
		{`ab\c`, `abc`},
		{`\abc`, `abc`},
	}

	for _, t := range tbl {
		unesacped := unescapeString(t.escaped)
		c.Assert(unesacped, Equals, t.expected)
	}
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

func (s *parserTestSuite) TestParseValue(c *C) {
	str := `'abc\\',''`
	values, err := parseValues(str)
	c.Assert(err, IsNil)
	c.Assert(values, DeepEquals, []string{`'abc\'`, `''`})

	str = `123,'\Z#÷QÎx£. Æ‘ÇoPâÅ_\r—\\','','qn'`
	values, err = parseValues(str)
	c.Assert(err, IsNil)
	c.Assert(values, HasLen, 4)

	str = `123,'\Z#÷QÎx£. Æ‘ÇoPâÅ_\r—\\','','qn\'`
	values, err = parseValues(str)
	c.Assert(err, NotNil)
}

func (s *parserTestSuite) TestParseLine(c *C) {
	lines := []struct {
		line     string
		expected string
	}{
		{line: "INSERT INTO `test` VALUES (1, 'first', 'hello mysql; 2', 'e1', 'a,b');",
			expected: "1, 'first', 'hello mysql; 2', 'e1', 'a,b'"},
		{line: "INSERT INTO `test` VALUES (0x22270073646661736661736466, 'first', 'hello mysql; 2', 'e1', 'a,b');",
			expected: "0x22270073646661736661736466, 'first', 'hello mysql; 2', 'e1', 'a,b'"},
	}

	f := func(c rune) bool {
		return c == '\r' || c == '\n'
	}

	for _, t := range lines {
		l := strings.TrimRightFunc(t.line, f)

		m := valuesExp.FindAllStringSubmatch(l, -1)

		c.Assert(m, HasLen, 1)
		c.Assert(m[0][1], Matches, "test")
		c.Assert(m[0][2], Matches, t.expected)
	}
}
