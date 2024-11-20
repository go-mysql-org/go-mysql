package canal

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/siddontang/go-log/log"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/test_util"
)

type canalTestSuite struct {
	addr string
	suite.Suite
	c *Canal
}

type canalTestSuiteOption func(c *canalTestSuite)

func withAddr(addr string) canalTestSuiteOption {
	return func(c *canalTestSuite) {
		c.addr = addr
	}
}

func newCanalTestSuite(opts ...canalTestSuiteOption) *canalTestSuite {
	c := new(canalTestSuite)
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func TestCanalSuite(t *testing.T) {
	suite.Run(t, newCanalTestSuite())
	suite.Run(t, newCanalTestSuite(withAddr(mysql.DEFAULT_IPV6_ADDR)))
}

const (
	miA = 0
	miB = -1
	miC = 1

	umiA = 0
	umiB = 1
	umiC = 16777215
)

func (s *canalTestSuite) SetupSuite() {
	cfg := NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%s", *test_util.MysqlHost, *test_util.MysqlPort)
	if s.addr != "" {
		cfg.Addr = s.addr
	}
	cfg.User = "root"
	cfg.HeartbeatPeriod = 200 * time.Millisecond
	cfg.ReadTimeout = 300 * time.Millisecond
	cfg.Dump.ExecutionPath = "mysqldump"
	cfg.Dump.TableDB = "test"
	cfg.Dump.Tables = []string{"canal_test"}
	cfg.Dump.Where = "id>0"

	// include & exclude config
	cfg.IncludeTableRegex = make([]string, 1)
	cfg.IncludeTableRegex[0] = ".*\\.canal_test"
	cfg.ExcludeTableRegex = make([]string, 2)
	cfg.ExcludeTableRegex[0] = "mysql\\..*"
	cfg.ExcludeTableRegex[1] = ".*\\..*_inner"

	var err error
	s.c, err = NewCanal(cfg)
	require.NoError(s.T(), err)
	s.execute("DROP TABLE IF EXISTS test.canal_test")
	sql := `
        CREATE TABLE IF NOT EXISTS test.canal_test (
			id int AUTO_INCREMENT,
			content blob DEFAULT NULL,
            name varchar(100),
			mi mediumint(8) NOT NULL DEFAULT 0,
			umi mediumint(8) unsigned NOT NULL DEFAULT 0,
            PRIMARY KEY(id)
            )ENGINE=innodb;
    `

	s.execute(sql)

	s.execute("DELETE FROM test.canal_test")
	s.execute("INSERT INTO test.canal_test (content, name, mi, umi) VALUES (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?)",
		"1", "a", miA, umiA,
		`\0\ndsfasdf`, "b", miC, umiC,
		"", "c", miB, umiB,
	)

	s.execute("SET GLOBAL binlog_format = 'ROW'")

	s.c.SetEventHandler(&testEventHandler{})
	go func() {
		set, _ := mysql.ParseGTIDSet("mysql", "")
		err = s.c.StartFromGTID(set)
		require.NoError(s.T(), err)
	}()
}

func (s *canalTestSuite) TearDownSuite() {
	// To test the heartbeat and read timeout,so need to sleep 1 seconds without data transmission
	s.T().Logf("Start testing the heartbeat and read timeout")
	time.Sleep(time.Second)

	if s.c != nil {
		s.c.Close()
		s.c = nil
	}
}

func (s *canalTestSuite) execute(query string, args ...interface{}) *mysql.Result {
	r, err := s.c.Execute(query, args...)
	require.NoError(s.T(), err)
	return r
}

type testEventHandler struct {
	DummyEventHandler
}

func (h *testEventHandler) OnRow(e *RowsEvent) error {
	log.Infof("OnRow %s %v\n", e.Action, e.Rows)
	umi, ok := e.Rows[0][4].(uint32) // 4th col is umi. mysqldump gives uint64 instead of uint32
	if ok && (umi != umiA && umi != umiB && umi != umiC) {
		return fmt.Errorf("invalid unsigned medium int %d", umi)
	}
	return nil
}

func (h *testEventHandler) String() string {
	return "testEventHandler"
}

func (s *canalTestSuite) TestCanal() {
	<-s.c.WaitDumpDone()

	for i := 1; i < 10; i++ {
		s.execute("INSERT INTO test.canal_test (name) VALUES (?)", fmt.Sprintf("%d", i))
	}
	s.execute("INSERT INTO test.canal_test (mi,umi) VALUES (?,?), (?,?), (?,?)",
		miA, umiA,
		miC, umiC,
		miB, umiB,
	)
	s.execute("ALTER TABLE test.canal_test ADD `age` INT(5) NOT NULL AFTER `name`")
	s.execute("INSERT INTO test.canal_test (name,age) VALUES (?,?)", "d", "18")

	err := s.c.CatchMasterPos(10 * time.Second)
	require.NoError(s.T(), err)
}

func (s *canalTestSuite) TestAnalyzeAdvancesSyncedPos() {
	<-s.c.WaitDumpDone()

	// We should not need to use FLUSH BINARY LOGS
	// An ANALYZE TABLE statement should advance the saved position.
	// There are still cases that don't advance, such as
	// statements that won't parse like [CREATE|DROP] TRIGGER.
	s.c.cfg.DisableFlushBinlogWhileWaiting = true
	defer func() {
		s.c.cfg.DisableFlushBinlogWhileWaiting = false
	}()

	startingPos, err := s.c.GetMasterPos()
	require.NoError(s.T(), err)

	s.execute("ANALYZE TABLE test.canal_test")
	err = s.c.CatchMasterPos(10 * time.Second)
	require.NoError(s.T(), err)

	// Ensure the ending pos is greater than the starting pos
	// but the filename is the same. This ensures that
	// FLUSH BINARY LOGS was not used.
	endingPos, err := s.c.GetMasterPos()
	require.NoError(s.T(), err)
	require.Equal(s.T(), startingPos.Name, endingPos.Name)
	require.Greater(s.T(), endingPos.Pos, startingPos.Pos)
}

func (s *canalTestSuite) TestCanalFilter() {
	// included
	sch, err := s.c.GetTable("test", "canal_test")
	require.NoError(s.T(), err)
	require.NotNil(s.T(), sch)
	_, err = s.c.GetTable("not_exist_db", "canal_test")
	require.NotErrorIs(s.T(), err, ErrExcludedTable)
	// excluded
	sch, err = s.c.GetTable("test", "canal_test_inner")
	require.ErrorIs(s.T(), err, ErrExcludedTable)
	require.Nil(s.T(), sch)
	sch, err = s.c.GetTable("mysql", "canal_test")
	require.ErrorIs(s.T(), err, ErrExcludedTable)
	require.Nil(s.T(), sch)
	sch, err = s.c.GetTable("not_exist_db", "not_canal_test")
	require.ErrorIs(s.T(), err, ErrExcludedTable)
	require.Nil(s.T(), sch)
}

func TestCreateTableExp(t *testing.T) {
	cases := []string{
		"CREATE TABLE /*generated by server */ mydb.mytable (`id` int(10)) ENGINE=InnoDB",
		"CREATE TABLE `mydb`.`mytable` (`id` int(10)) ENGINE=InnoDB",
		"CREATE TABLE IF NOT EXISTS mydb.`mytable` (`id` int(10)) ENGINE=InnoDB",
		"CREATE TABLE IF NOT EXISTS `mydb`.mytable (`id` int(10)) ENGINE=InnoDB",
	}
	expected := &node{
		db:    "mydb",
		table: "mytable",
	}
	pr := parser.New()
	for _, s := range cases {
		stmts, _, err := pr.Parse(s, "", "")
		require.NoError(t, err)
		for _, st := range stmts {
			nodes := parseStmt(st)
			if len(nodes) == 0 {
				continue
			}
			require.Equal(t, expected, nodes[0])
		}
	}
}
func TestAlterTableExp(t *testing.T) {
	cases := []string{
		"ALTER TABLE /*generated by server*/ `mydb`.`mytable` ADD `field2` DATE  NULL  AFTER `field1`;",
		"ALTER TABLE `mytable` ADD `field2` DATE  NULL  AFTER `field1`;",
		"ALTER TABLE mydb.mytable ADD `field2` DATE  NULL  AFTER `field1`;",
		"ALTER TABLE mytable ADD `field2` DATE  NULL  AFTER `field1`;",
		"ALTER TABLE mydb.mytable ADD field2 DATE  NULL  AFTER `field1`;",
	}

	table := "mytable"
	db := "mydb"
	pr := parser.New()
	for _, s := range cases {
		stmts, _, err := pr.Parse(s, "", "")
		require.NoError(t, err)
		for _, st := range stmts {
			nodes := parseStmt(st)
			if len(nodes) == 0 {
				continue
			}
			rdb := nodes[0].db
			rtable := nodes[0].table
			if (len(rdb) > 0 && rdb != db) || rtable != table {
				t.Fatalf("TestAlterTableExp:case %s failed db %s,table %s\n", s, rdb, rtable)
			}
		}
	}
}

func TestRenameTableExp(t *testing.T) {
	cases := []string{
		"rename /* generate by server */table `mydb`.`mytable0` to `mydb`.`mytable0tmp`",
		"rename table `mytable0` to `mytable0tmp`",
		"rename table mydb.mytable0 to mydb.mytable0tmp",
		"rename table mytable0 to mytable0tmp",

		"rename table `mydb`.`mytable0` to `mydb`.`mytable0tmp`, `mydb`.`mytable1` to `mydb`.`mytable1tmp`",
		"rename table `mytable0` to `mytable0tmp`, `mytable1` to `mytable1tmp`",
		"rename table mydb.mytable0 to mydb.mytable0tmp, mydb.mytable1 to mydb.mytable1tmp",
		"rename table mytable0 to mytable0tmp, mytable1 to mytabletmp",
	}
	baseTable := "mytable"
	db := "mydb"
	pr := parser.New()
	for _, s := range cases {
		stmts, _, err := pr.Parse(s, "", "")
		require.NoError(t, err)
		for _, st := range stmts {
			nodes := parseStmt(st)
			if len(nodes) == 0 {
				continue
			}
			for i, node := range nodes {
				rdb := node.db
				rtable := node.table
				table := fmt.Sprintf("%s%d", baseTable, i)
				if (len(rdb) > 0 && rdb != db) || rtable != table {
					t.Fatalf("TestRenameTableExp:case %s failed db %s,table %s\n", s, rdb, rtable)
				}
			}
		}
	}
}

func TestDropTableExp(t *testing.T) {
	cases := []string{
		"drop table test0",
		"DROP TABLE test0",
		"DROP TABLE test0",
		"DROP table IF EXISTS test.test0",
		"drop table `test0`",
		"DROP TABLE `test0`",
		"DROP table IF EXISTS `test`.`test0`",
		"DROP TABLE `test0` /* generated by server */",
		"DROP /*generated by server */ table if exists test0",
		"DROP table if exists `test0`",
		"DROP table if exists test.test0",
		"DROP table if exists `test`.test0",
		"DROP table if exists `test`.`test0`",
		"DROP table if exists test.`test0`",
		"DROP table if exists test.`test0`",
	}

	baseTable := "test"
	db := "test"
	pr := parser.New()
	for _, s := range cases {
		stmts, _, err := pr.Parse(s, "", "")
		require.NoError(t, err)
		for _, st := range stmts {
			nodes := parseStmt(st)
			if len(nodes) == 0 {
				continue
			}
			for i, node := range nodes {
				rdb := node.db
				rtable := node.table
				table := fmt.Sprintf("%s%d", baseTable, i)
				if (len(rdb) > 0 && rdb != db) || rtable != table {
					t.Fatalf("TestDropTableExp:case %s failed db %s,table %s\n", s, rdb, rtable)
				}
			}
		}
	}
}

func TestWithoutSchemeExp(t *testing.T) {
	cases := []replication.QueryEvent{
		{
			Schema: []byte("test"),
			Query:  []byte("drop table test0"),
		},
		{
			Schema: []byte("test"),
			Query:  []byte("rename table `test0` to `testtmp`"),
		},
		{
			Schema: []byte("test"),
			Query:  []byte("ALTER TABLE `test0` ADD `field2` DATE  NULL  AFTER `field1`;"),
		},
		{
			Schema: []byte("test"),
			Query:  []byte("CREATE TABLE IF NOT EXISTS test0 (`id` int(10)) ENGINE=InnoDB"),
		},
	}
	table := "test0"
	db := "test"
	pr := parser.New()
	for _, s := range cases {
		stmts, _, err := pr.Parse(string(s.Query), "", "")
		require.NoError(t, err)
		for _, st := range stmts {
			nodes := parseStmt(st)
			if len(nodes) == 0 {
				continue
			}
			if nodes[0].db != "" || nodes[0].table != table || string(s.Schema) != db {
				t.Fatalf("TestCreateTableExp:case %s failed\n", s.Query)
			}
		}
	}
}

func TestCreateIndexExp(t *testing.T) {
	cases := []string{
		"create index test0 on test.test (id)",
		"create index test0 ON test.test (id)",
		"CREATE INDEX test0 on `test`.test (id)",
		"CREATE INDEX test0 ON test.test (id)",
		"CREATE index test0 on `test`.test (id)",
		"CREATE index test0 ON test.test (id)",
		"create INDEX test0 on `test`.test (id)",
		"create INDEX test0 ON test.test (id)",
		"CREATE INDEX `test0` ON `test`.`test` (`id`) /* generated by server */",
		"CREATE /*generated by server */ INDEX `test0` ON `test`.`test` (`id`)",
		"CREATE INDEX `test0` ON `test`.test (id)",
		"CREATE INDEX `test0` ON test.`test` (id)",
		"CREATE INDEX `test0` ON test.test (`id`)",
		"CREATE INDEX test0 ON `test`.`test` (`id`)",
		"CREATE INDEX test0 ON `test`.`test` (id)",
		"CREATE INDEX test0 ON test.test (`id`)",
	}

	baseTable := "test"
	db := "test"
	pr := parser.New()
	for _, s := range cases {
		stmts, _, err := pr.Parse(s, "", "")
		require.NoError(t, err)
		for _, st := range stmts {
			nodes := parseStmt(st)
			require.NotZero(t, nodes)
			for _, node := range nodes {
				rdb := node.db
				rtable := node.table
				require.Equal(t, db, rdb)
				require.Equal(t, baseTable, rtable)
			}
		}
	}
}

func TestDropIndexExp(t *testing.T) {
	cases := []string{
		"drop index test0 on test.test",
		"DROP INDEX test0 ON test.test",
		"drop INDEX test0 on test.test",
		"DROP index test0 ON test.test",
		"drop INDEX `test0` on `test`.`test`",
		"drop INDEX test0 ON `test`.`test`",
		"drop INDEX test0 on `test`.test",
		"drop INDEX test0 on test.`test`",
		"DROP index `test0` on `test`.`test`",
		"DROP index test0 ON `test`.`test`",
		"DROP index test0 on `test`.test",
		"DROP index test0 on test.`test`",
		"DROP INDEX `test0` ON `test`.`test` /* generated by server */",
		"DROP /*generated by server */ INDEX `test0` ON `test`.`test`",
		"DROP INDEX `test0` ON `test`.test",
		"DROP INDEX `test0` ON test.`test`",
		"DROP INDEX `test0` ON test.test",
		"DROP INDEX test0 ON `test`.`test`",
		"DROP INDEX test0 ON `test`.`test`",
		"DROP INDEX test0 ON test.test",
	}

	baseTable := "test"
	db := "test"
	pr := parser.New()
	for _, s := range cases {
		stmts, _, err := pr.Parse(s, "", "")
		require.NoError(t, err)
		for _, st := range stmts {
			nodes := parseStmt(st)
			require.NotZero(t, nodes)
			for _, node := range nodes {
				rdb := node.db
				rtable := node.table
				require.Equal(t, db, rdb)
				require.Equal(t, baseTable, rtable)
			}
		}
	}
}

func TestIncludeExcludeTableRegex(t *testing.T) {
	cfg := NewDefaultConfig()

	// include & exclude config
	cfg.IncludeTableRegex = make([]string, 1)
	cfg.IncludeTableRegex[0] = ".*\\.canal_test"
	cfg.ExcludeTableRegex = make([]string, 2)
	cfg.ExcludeTableRegex[0] = "mysql\\..*"
	cfg.ExcludeTableRegex[1] = ".*\\..*_inner"

	c := new(Canal)
	c.cfg = cfg
	require.Nil(t, c.initTableFilter())
	require.True(t, c.checkTableMatch("test.canal_test"))
	require.False(t, c.checkTableMatch("test.canal_test_inner"))
	require.False(t, c.checkTableMatch("mysql.canal_test_inner"))

	cfg.IncludeTableRegex = nil
	c = new(Canal)
	c.cfg = cfg
	require.Nil(t, c.initTableFilter())
	require.True(t, c.checkTableMatch("test.canal_test"))
	require.False(t, c.checkTableMatch("test.canal_test_inner"))
	require.False(t, c.checkTableMatch("mysql.canal_test_inner"))
}
