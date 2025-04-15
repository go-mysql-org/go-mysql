package replication

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/test_util"
	"github.com/go-mysql-org/go-mysql/utils"
)

var testOutputLogs = flag.Bool("out", false, "output binlog event")

type testSyncerSuite struct {
	suite.Suite
	b *BinlogSyncer
	c *client.Conn

	wg sync.WaitGroup

	flavor string
}

func TestSyncerSuite(t *testing.T) {
	suite.Run(t, new(testSyncerSuite))
}

func (t *testSyncerSuite) TearDownTest() {
	if t.b != nil {
		t.b.Close()
		t.b = nil
	}

	if t.c != nil {
		t.c.Close()
		t.c = nil
	}
}

func (t *testSyncerSuite) testExecute(query string) {
	_, err := t.c.Execute(query)
	require.NoError(t.T(), err)
}

func (t *testSyncerSuite) testSync(s *BinlogStreamer) {
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()

		if s == nil {
			return
		}

		for {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			e, err := s.GetEvent(ctx)
			cancel()

			if err == context.DeadlineExceeded {
				return
			}

			require.NoError(t.T(), err)

			if *testOutputLogs {
				e.Dump(os.Stdout)
				os.Stdout.Sync()
			}
		}
	}()

	// use mixed format
	t.testExecute("SET SESSION binlog_format = 'MIXED'")

	str := `DROP TABLE IF EXISTS test_replication`
	t.testExecute(str)

	str = `CREATE TABLE test_replication (
			id BIGINT(64) UNSIGNED  NOT NULL AUTO_INCREMENT,
			str VARCHAR(256),
			f FLOAT,
			d DOUBLE,
			de DECIMAL(10,2),
			i INT,
			bi BIGINT,
			e enum ("e1", "e2"),
			b BIT(8),
			y YEAR,
			da DATE,
			ts TIMESTAMP,
			dt DATETIME,
			tm TIME,
			t TEXT,
			bb BLOB,
			se SET('a', 'b', 'c'),
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8`

	t.testExecute(str)

	// use row format
	t.testExecute("SET SESSION binlog_format = 'ROW'")

	t.testExecute(`INSERT INTO test_replication (str, f, i, e, b, y, da, ts, dt, tm, de, t, bb, se)
		VALUES ("3", -3.14, 10, "e1", 0b0011, 1985,
		"2012-05-07", "2012-05-07 14:01:01", "2012-05-07 14:01:01",
		"14:01:01", -45363.64, "abc", "12345", "a,b")`)

	if t.flavor == mysql.MySQLFlavor {
		t.testExecute("SET SESSION binlog_row_image = 'MINIMAL'")

		if eq, err := t.c.CompareServerVersion("8.0.0"); (err == nil) && (eq >= 0) {
			t.testExecute("SET SESSION binlog_row_value_options = 'PARTIAL_JSON'")
		}

		const id = 100
		t.testExecute(fmt.Sprintf(`INSERT INTO test_replication (id, str, f, i, bb, de) VALUES (%d, "4", -3.14, 100, "abc", -45635.64)`, id))
		t.testExecute(fmt.Sprintf(`UPDATE test_replication SET f = -12.14, de = 555.34 WHERE id = %d`, id))
		t.testExecute(fmt.Sprintf(`DELETE FROM test_replication WHERE id = %d`, id))
	}

	// check whether we can create the table including the json field
	str = `DROP TABLE IF EXISTS test_json`
	t.testExecute(str)

	str = `CREATE TABLE test_json (
			id BIGINT(64) UNSIGNED  NOT NULL AUTO_INCREMENT,
			c1 JSON,
			c2 DECIMAL(10, 0),
			PRIMARY KEY (id)
			) ENGINE=InnoDB`

	if _, err := t.c.Execute(str); err == nil {
		t.testExecute(`INSERT INTO test_json (c2) VALUES (1)`)
		t.testExecute(`INSERT INTO test_json (c1, c2) VALUES ('{"key1": "value1", "key2": "value2"}', 1)`)
	}

	t.testExecute("DROP TABLE IF EXISTS test_json_v2")

	str = `CREATE TABLE test_json_v2 (
			id INT, 
			c JSON,
			PRIMARY KEY (id)
			) ENGINE=InnoDB`

	if _, err := t.c.Execute(str); err == nil {
		tbls := []string{
			// Refer: https://github.com/shyiko/mysql-binlog-connector-java/blob/c8e81c879710dc19941d952f9031b0a98f8b7c02/src/test/java/com/github/shyiko/mysql/binlog/event/deserialization/json/JsonBinaryValueIntegrationTest.java#L84
			// License: https://github.com/shyiko/mysql-binlog-connector-java#license
			`INSERT INTO test_json_v2 VALUES (0, NULL)`,
			`INSERT INTO test_json_v2 VALUES (1, '{\"a\": 2}')`,
			`INSERT INTO test_json_v2 VALUES (2, '[1,2]')`,
			`INSERT INTO test_json_v2 VALUES (3, '{\"a\":\"b\", \"c\":\"d\",\"ab\":\"abc\", \"bc\": [\"x\", \"y\"]}')`,
			`INSERT INTO test_json_v2 VALUES (4, '[\"here\", [\"I\", \"am\"], \"!!!\"]')`,
			`INSERT INTO test_json_v2 VALUES (5, '\"scalar string\"')`,
			`INSERT INTO test_json_v2 VALUES (6, 'true')`,
			`INSERT INTO test_json_v2 VALUES (7, 'false')`,
			`INSERT INTO test_json_v2 VALUES (8, 'null')`,
			`INSERT INTO test_json_v2 VALUES (9, '-1')`,
			`INSERT INTO test_json_v2 VALUES (10, CAST(CAST(1 AS UNSIGNED) AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (11, '32767')`,
			`INSERT INTO test_json_v2 VALUES (12, '32768')`,
			`INSERT INTO test_json_v2 VALUES (13, '-32768')`,
			`INSERT INTO test_json_v2 VALUES (14, '-32769')`,
			`INSERT INTO test_json_v2 VALUES (15, '2147483647')`,
			`INSERT INTO test_json_v2 VALUES (16, '2147483648')`,
			`INSERT INTO test_json_v2 VALUES (17, '-2147483648')`,
			`INSERT INTO test_json_v2 VALUES (18, '-2147483649')`,
			`INSERT INTO test_json_v2 VALUES (19, '18446744073709551615')`,
			`INSERT INTO test_json_v2 VALUES (20, '18446744073709551616')`,
			`INSERT INTO test_json_v2 VALUES (21, '3.14')`,
			`INSERT INTO test_json_v2 VALUES (22, '{}')`,
			`INSERT INTO test_json_v2 VALUES (23, '[]')`,
			`INSERT INTO test_json_v2 VALUES (24, CAST(CAST('2015-01-15 23:24:25' AS DATETIME) AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (25, CAST(CAST('23:24:25' AS TIME) AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (125, CAST(CAST('23:24:25.12' AS TIME(3)) AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (225, CAST(CAST('23:24:25.0237' AS TIME(3)) AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (26, CAST(CAST('2015-01-15' AS DATE) AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (27, CAST(TIMESTAMP'2015-01-15 23:24:25' AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (127, CAST(TIMESTAMP'2015-01-15 23:24:25.12' AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (227, CAST(TIMESTAMP'2015-01-15 23:24:25.0237' AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (327, CAST(UNIX_TIMESTAMP('2015-01-15 23:24:25') AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (28, CAST(ST_GeomFromText('POINT(1 1)') AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (29, CAST('[]' AS CHAR CHARACTER SET 'ascii'))`,
			// TODO: 30 and 31 are BIT type from JSON_TYPE, may support later.
			`INSERT INTO test_json_v2 VALUES (30, CAST(x'cafe' AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (31, CAST(x'cafebabe' AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (100, CONCAT('{\"', REPEAT('a', 64 * 1024 - 1), '\":123}'))`,
		}

		for _, query := range tbls {
			t.testExecute(query)
		}

		// "Partial Updates of JSON Values" from https://dev.mysql.com/doc/refman/8.0/en/json.html
		jsonOrig := `'{"a":"aaaaaaaaaaaaa", "c":"ccccccccccccccc", "ab":["abababababababa", "babababababab"]}'`
		tbls = []string{
			`ALTER TABLE test_json_v2 ADD COLUMN d JSON DEFAULT NULL, ADD COLUMN e JSON DEFAULT NULL`,
			`INSERT INTO test_json_v2 VALUES (101, ` + jsonOrig + `, ` + jsonOrig + `, ` + jsonOrig + `)`,
			`UPDATE test_json_v2 SET c = JSON_SET(c, '$.ab', '["ab_updatedccc"]') WHERE id = 101`,
			`UPDATE test_json_v2 SET d = JSON_SET(d, '$.ab', '["ab_updatedddd"]') WHERE id = 101`,
			`UPDATE test_json_v2 SET e = JSON_SET(e, '$.ab', '["ab_updatedeee"]') WHERE id = 101`,
			`UPDATE test_json_v2 SET d = JSON_SET(d, '$.ab', '["ab_ddd"]'), e = json_set(e, '$.ab', '["ab_eee"]') WHERE id = 101`,
			// ToDo(atercattus): add more tests with JSON_REPLACE() and JSON_REMOVE()
		}
		for _, query := range tbls {
			t.testExecute(query)
		}

		// If MySQL supports JSON, it must supports GEOMETRY.
		t.testExecute("DROP TABLE IF EXISTS test_geo")

		str = `CREATE TABLE test_geo (g GEOMETRY)`
		_, err = t.c.Execute(str)
		require.NoError(t.T(), err)

		tbls = []string{
			`INSERT INTO test_geo VALUES (POINT(1, 1))`,
			`INSERT INTO test_geo VALUES (LINESTRING(POINT(0,0), POINT(1,1), POINT(2,2)))`,
			// TODO: add more geometry tests
		}

		for _, query := range tbls {
			t.testExecute(query)
		}
	}

	str = `DROP TABLE IF EXISTS test_parse_time`
	t.testExecute(str)

	// Must allow zero time.
	t.testExecute(`SET sql_mode=''`)
	str = `CREATE TABLE test_parse_time (
			a1 DATETIME, 
			a2 DATETIME(3), 
			a3 DATETIME(6), 
			b1 TIMESTAMP, 
			b2 TIMESTAMP(3) , 
			b3 TIMESTAMP(6))`
	t.testExecute(str)

	t.testExecute(`INSERT INTO test_parse_time VALUES
		("2014-09-08 17:51:04.123456", "2014-09-08 17:51:04.123456", "2014-09-08 17:51:04.123456", 
		"2014-09-08 17:51:04.123456","2014-09-08 17:51:04.123456","2014-09-08 17:51:04.123456"),
		("0000-00-00 00:00:00.000000", "0000-00-00 00:00:00.000000", "0000-00-00 00:00:00.000000",
		"0000-00-00 00:00:00.000000", "0000-00-00 00:00:00.000000", "0000-00-00 00:00:00.000000"),
		("2014-09-08 17:51:04.000456", "2014-09-08 17:51:04.000456", "2014-09-08 17:51:04.000456", 
		"2014-09-08 17:51:04.000456","2014-09-08 17:51:04.000456","2014-09-08 17:51:04.000456")`)

	t.wg.Wait()
}

func (t *testSyncerSuite) setupTest(flavor string) {
	var port uint16 = 3306
	switch flavor {
	case mysql.MariaDBFlavor:
		port = 3316
	}

	t.flavor = flavor

	var err error
	if t.c != nil {
		t.c.Close()
	}

	t.c, err = client.Connect(fmt.Sprintf("%s:%d", *test_util.MysqlHost, port), "root", "", "")
	if err != nil {
		t.T().Skip(err.Error())
	}

	_, err = t.c.Execute("CREATE DATABASE IF NOT EXISTS test")
	require.NoError(t.T(), err)

	_, err = t.c.Execute("USE test")
	require.NoError(t.T(), err)

	if t.b != nil {
		t.b.Close()
	}

	cfg := BinlogSyncerConfig{
		ServerID:   100,
		Flavor:     flavor,
		Host:       *test_util.MysqlHost,
		Port:       port,
		User:       "root",
		Password:   "",
		UseDecimal: true,
	}

	t.b = NewBinlogSyncer(cfg)
}

func (t *testSyncerSuite) testPositionSync() {
	// get current master binlog file and position
	showBinlogStatus := "SHOW BINARY LOG STATUS"
	showReplicas := "SHOW REPLICAS"
	if eq, err := t.c.CompareServerVersion("8.4.0"); (err == nil) && (eq < 0) {
		showBinlogStatus = "SHOW MASTER STATUS"
		showReplicas = "SHOW SLAVE HOSTS"
	}
	r, err := t.c.Execute(showBinlogStatus)
	require.NoError(t.T(), err)
	binFile, _ := r.GetString(0, 0)
	binPos, _ := r.GetInt(0, 1)

	s, err := t.b.StartSync(mysql.Position{Name: binFile, Pos: uint32(binPos)})
	require.NoError(t.T(), err)

	r, err = t.c.Execute(showReplicas)
	require.NoError(t.T(), err)

	// List of replicas must not be empty
	require.Greater(t.T(), len(r.Values), 0)

	// Slave_UUID is empty for mysql 8.0.28+ (8.0.32 still broken)
	if eq, err := t.c.CompareServerVersion("8.0.28"); (err == nil) && (eq < 0) {
		// check we have set Slave_UUID
		slaveUUID, _ := r.GetString(0, 4)
		require.Len(t.T(), slaveUUID, 36)
	} else if err != nil {
		require.NoError(t.T(), err)
	}

	// Test re-sync.
	time.Sleep(100 * time.Millisecond)
	_ = t.b.c.SetReadDeadline(utils.Now().Add(time.Millisecond))
	time.Sleep(100 * time.Millisecond)

	t.testSync(s)
}

func (t *testSyncerSuite) TestMysqlPositionSync() {
	t.setupTest(mysql.MySQLFlavor)
	t.testPositionSync()
}

func (t *testSyncerSuite) TestMysqlGTIDSync() {
	t.setupTest(mysql.MySQLFlavor)

	r, err := t.c.Execute("SELECT @@gtid_mode")
	require.NoError(t.T(), err)
	modeOn, _ := r.GetString(0, 0)
	if modeOn != "ON" {
		t.T().Skip("GTID mode is not ON")
	}

	r, err = t.c.Execute("SHOW GLOBAL VARIABLES LIKE 'SERVER_UUID'")
	require.NoError(t.T(), err)

	var masterUuid uuid.UUID
	if s, _ := r.GetString(0, 1); len(s) > 0 && s != "NONE" {
		masterUuid, err = uuid.Parse(s)
		require.NoError(t.T(), err)
	}

	set, _ := mysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:%d-%d", masterUuid.String(), 1, 2))

	s, err := t.b.StartSyncGTID(set)
	require.NoError(t.T(), err)

	t.testSync(s)
}

func (t *testSyncerSuite) TestMariadbPositionSync() {
	t.setupTest(mysql.MariaDBFlavor)

	t.testPositionSync()
}

func (t *testSyncerSuite) TestMariadbGTIDSync() {
	t.setupTest(mysql.MariaDBFlavor)

	// get current master gtid binlog pos
	r, err := t.c.Execute("SELECT @@gtid_binlog_pos")
	require.NoError(t.T(), err)

	str, _ := r.GetString(0, 0)
	set, _ := mysql.ParseMariadbGTIDSet(str)

	s, err := t.b.StartSyncGTID(set)
	require.NoError(t.T(), err)

	t.testSync(s)
}

func (t *testSyncerSuite) TestMariadbAnnotateRows() {
	t.setupTest(mysql.MariaDBFlavor)
	t.b.cfg.DumpCommandFlag = BINLOG_SEND_ANNOTATE_ROWS_EVENT
	t.testPositionSync()
}

func (t *testSyncerSuite) TestMysqlSemiPositionSync() {
	t.setupTest(mysql.MySQLFlavor)

	t.b.cfg.SemiSyncEnabled = true

	t.testPositionSync()
}

func (t *testSyncerSuite) TestMysqlBinlogCodec() {
	t.setupTest(mysql.MySQLFlavor)

	resetBinaryLogs := "RESET BINARY LOGS AND GTIDS"
	if eq, err := t.c.CompareServerVersion("8.4.0"); (err == nil) && (eq < 0) {
		resetBinaryLogs = "RESET MASTER"
	}

	t.testExecute(resetBinaryLogs)

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	go func() {
		defer wg.Done()

		t.testSync(nil)

		t.testExecute("FLUSH LOGS")

		t.testSync(nil)
	}()

	binlogDir := "./var"

	os.RemoveAll(binlogDir)

	err := t.b.StartBackup(binlogDir, mysql.Position{Name: "", Pos: uint32(0)}, 2*time.Second)
	require.NoError(t.T(), err)

	p := NewBinlogParser()
	p.SetVerifyChecksum(true)

	f := func(e *BinlogEvent) error {
		if *testOutputLogs {
			e.Dump(os.Stdout)
			os.Stdout.Sync()
		}
		return nil
	}

	dir, err := os.Open(binlogDir)
	require.NoError(t.T(), err)
	defer dir.Close()

	files, err := dir.Readdirnames(-1)
	require.NoError(t.T(), err)

	for _, file := range files {
		err = p.ParseFile(path.Join(binlogDir, file), 0, f)
		require.NoError(t.T(), err)
	}
}
