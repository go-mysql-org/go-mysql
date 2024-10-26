package dump

import (
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

// This tests the binlogExp regexp that matches the line that mysqldump adds when called with --master-data or --source-data
func TestBinlogExp(t *testing.T) {
	stmts := []struct {
		input string
		file  string
		pos   string
	}{
		{
			// MySQL 9.1.0
			`CHANGE REPLICATION SOURCE TO SOURCE_LOG_FILE='binlog.000002', SOURCE_LOG_POS=170923;`,
			`binlog.000002`,
			`170923`,
		},
		{
			`CHANGE MASTER TO MASTER_LOG_FILE='mysql-bin.008995', MASTER_LOG_POS=102052485;`,
			`mysql-bin.008995`,
			`102052485`,
		},
	}

	for _, stmt := range stmts {
		m := binlogExp.FindAllStringSubmatch(stmt.input, -1)
		require.NotNil(t, m)
		require.Equal(t, stmt.file, m[0][3])
		require.Equal(t, stmt.pos, m[0][5])
	}
}

func TestParseGtidExp(t *testing.T) {
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
		{
			`SET @@GLOBAL.GTID_PURGED='e50bd2d3-6ad7-11e9-890c-42010af0017c:1-5291126581:5291126583-5323107666';
`,
			"e50bd2d3-6ad7-11e9-890c-42010af0017c:1-5291126581:5291126583-5323107666",
		},
		{`SET @@GLOBAL.GTID_PURGED='071a84e8-b253-11e8-8472-005056a27e86:1,
2337be48-0456-11e9-bd1c-00505690543b:1-7,
5f1eea9e-b1e5-11e8-bc77-005056a221ed:1-144609156,
e7574090-b123-11e8-8bb4-005056a29643:1'
`, "071a84e8-b253-11e8-8472-005056a27e86:1,2337be48-0456-11e9-bd1c-00505690543b:1-7,5f1eea9e-b1e5-11e8-bc77-005056a221ed:1-144609156,e7574090-b123-11e8-8bb4-005056a29643:1"},
	}

	for _, tt := range tbls {
		reader := strings.NewReader(tt.input)
		var handler = new(testParseHandler)

		err := Parse(reader, handler, true)
		require.NoError(t, err)

		if tt.expected == "" {
			if handler.gset != nil {
				require.Nil(t, handler.gset)
			} else {
				continue
			}
		}
		expectedGtidset, err := mysql.ParseGTIDSet("mysql", tt.expected)
		require.NoError(t, err)
		require.True(t, expectedGtidset.Equal(handler.gset))
	}
}

func TestParseFindTable(t *testing.T) {
	tbl := []struct {
		sql   string
		table string
	}{
		{"INSERT INTO `note` VALUES ('title', 'here is sql: INSERT INTO `table` VALUES (\\'some value\\')');", "note"},
		{"INSERT INTO `note` VALUES ('1', '2', '3');", "note"},
		{"INSERT INTO `a.b` VALUES ('1');", "a.b"},
	}

	for _, te := range tbl {
		res := valuesExp.FindAllStringSubmatch(te.sql, -1)[0][1]
		require.Equal(t, te.table, res)
	}
}

func TestUnescape(t *testing.T) {
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

	for _, te := range tbl {
		unesacped := unescapeString(te.escaped)
		require.Equal(t, te.expected, unesacped)
	}
}

func TestParseValue(t *testing.T) {
	str := `'abc\\',''`
	values, err := parseValues(str)
	require.NoError(t, err)
	require.Equal(t, []string{`'abc\'`, `''`}, values)

	str = `123,'\Z#÷QÎx£. Æ‘ÇoPâÅ_\r—\\','','qn'`
	values, err = parseValues(str)
	require.NoError(t, err)
	require.Len(t, values, 4)

	str = `123,'\Z#÷QÎx£. Æ‘ÇoPâÅ_\r—\\','','qn\'`
	_, err = parseValues(str)
	require.Error(t, err)
}

func TestParseLine(t *testing.T) {
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

	for _, te := range lines {
		l := strings.TrimRightFunc(te.line, f)

		m := valuesExp.FindAllStringSubmatch(l, -1)

		require.Len(t, m, 1)
		require.Equal(t, "test", m[0][1])
		require.Equal(t, te.expected, m[0][2])
	}
}
