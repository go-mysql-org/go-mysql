package replication

import (
	"flag"
	"fmt"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	. "gopkg.in/check.v1"
	"os"
	"sync"
	"testing"
	"time"
)

// Use docker mysql to test, mysql is 3306, mariadb is 3316
var testHost = flag.String("host", "127.0.0.1", "MySQL master host")

var testOutputLogs = flag.Bool("out", true, "output binlog event")

func TestBinLogSyncer(t *testing.T) {
	TestingT(t)
}

type testSyncerSuite struct {
	b *BinlogSyncer
	c *client.Conn

	wg sync.WaitGroup

	flavor string
}

var _ = Suite(&testSyncerSuite{})

func (t *testSyncerSuite) SetUpSuite(c *C) {

}

func (t *testSyncerSuite) TearDownSuite(c *C) {
}

func (t *testSyncerSuite) SetUpTest(c *C) {
}

func (t *testSyncerSuite) TearDownTest(c *C) {
	if t.b != nil {
		t.b.Close()
	}

	if t.c != nil {
		t.c.Close()
	}
}

func (t *testSyncerSuite) testExecute(c *C, query string) {
	_, err := t.c.Execute(query)
	c.Assert(err, IsNil)
}

func (t *testSyncerSuite) testSync(c *C, s *BinlogStreamer) {
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()

		for {
			e, err := s.GetEventTimeout(1 * time.Second)
			if err != nil {
				if err != ErrGetEventTimeout {
					c.Fatal(err)
				}
				return
			}

			if *testOutputLogs {
				e.Dump(os.Stdout)
				os.Stdout.Sync()
			}
		}
	}()

	//use mixed format
	t.testExecute(c, "SET SESSION binlog_format = 'MIXED'")

	str := `DROP TABLE IF EXISTS test_replication`
	t.testExecute(c, str)

	str = `CREATE TABLE IF NOT EXISTS test_replication (
	         id BIGINT(64) UNSIGNED  NOT NULL AUTO_INCREMENT,
	         str VARCHAR(256),
	         f FLOAT,
	         d DOUBLE,
	         de DECIMAL(5,2),
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
	      PRIMARY KEY (id)
	       ) ENGINE=InnoDB DEFAULT CHARSET=utf8`

	t.testExecute(c, str)

	//use row format
	t.testExecute(c, `INSERT INTO test_replication (str, f, i) VALUES ("3", 3.14, 10)`)
	t.testExecute(c, `INSERT INTO test_replication (e) VALUES ("e1")`)
	t.testExecute(c, `INSERT INTO test_replication (b) VALUES (0b0011)`)
	t.testExecute(c, `INSERT INTO test_replication (y) VALUES (1985)`)
	t.testExecute(c, `INSERT INTO test_replication (da) VALUES ("2012-05-07")`)
	t.testExecute(c, `INSERT INTO test_replication (ts) VALUES ("2012-05-07 14:01:01")`)
	t.testExecute(c, `INSERT INTO test_replication (dt) VALUES ("2012-05-07 14:01:01")`)
	t.testExecute(c, `INSERT INTO test_replication (tm) VALUES ("14:01:01")`)
	t.testExecute(c, `INSERT INTO test_replication (de) VALUES (122.24)`)
	t.testExecute(c, `INSERT INTO test_replication (t) VALUES ("abc")`)
	t.testExecute(c, `INSERT INTO test_replication (bb) VALUES ("12345")`)

	t.testExecute(c, "SET SESSION binlog_format = 'ROW'")

	id := 100

	if t.flavor == mysql.MySQLFlavor {
		for _, image := range []string{BINLOG_ROW_IMAGE_FULL, BINLOG_ROW_IAMGE_MINIMAL, BINLOG_ROW_IMAGE_NOBLOB} {
			t.testExecute(c, fmt.Sprintf("SET SESSION binlog_row_image = '%s'", image))

			t.testExecute(c, fmt.Sprintf(`INSERT INTO test_replication (id, str, f, i, bb) VALUES (%d, "4", 3.14, 100, "abc")`, id))
			t.testExecute(c, fmt.Sprintf(`UPDATE test_replication SET f = 2.14 WHERE id = %d`, id))
			t.testExecute(c, fmt.Sprintf(`DELETE FROM test_replication WHERE id = %d`, id))
			id++
		}
	} else {
		t.testExecute(c, fmt.Sprintf(`INSERT INTO test_replication (id, str, f, i, bb) VALUES (%d, "4", 3.14, 100, "abc")`, id))
		t.testExecute(c, fmt.Sprintf(`UPDATE test_replication SET f = 2.14 WHERE id = %d`, id))
		t.testExecute(c, fmt.Sprintf(`DELETE FROM test_replication WHERE id = %d`, id))
	}

	t.wg.Wait()
}

func (t *testSyncerSuite) setupTest(c *C, flavor string) {
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

	t.c, err = client.Connect(fmt.Sprintf("%s:%d", *testHost, port), "root", "", "")
	c.Assert(err, IsNil)

	_, err = t.c.Execute("CREATE DATABASE IF NOT EXISTS test")
	c.Assert(err, IsNil)

	_, err = t.c.Execute("USE test")
	c.Assert(err, IsNil)

	t.b = NewBinlogSyncer(100, flavor)

	err = t.b.RegisterSlave(*testHost, port, "root", "")
	c.Assert(err, IsNil)
}

func (t *testSyncerSuite) testPostionSync(c *C, flavor string) {
	t.setupTest(c, flavor)

	//get current master binlog file and position
	r, err := t.c.Execute("SHOW MASTER STATUS")
	c.Assert(err, IsNil)
	binFile, _ := r.GetString(0, 0)

	s, err := t.b.StartSync(mysql.Position{binFile, uint32(4)})
	c.Assert(err, IsNil)

	t.testSync(c, s)
}

func (t *testSyncerSuite) TestMysqlPostionSync(c *C) {
	t.testPostionSync(c, mysql.MySQLFlavor)
}

func (t *testSyncerSuite) TestMysqlGTIDSync(c *C) {
	t.setupTest(c, mysql.MySQLFlavor)

	r, err := t.c.Execute("SELECT @@gtid_mode")
	c.Assert(err, IsNil)
	modeOn, _ := r.GetString(0, 0)
	if modeOn != "ON" {
		c.Skip("GTID mode is not ON")
	}

	masterUuid, err := t.b.GetMasterUUID()
	c.Assert(err, IsNil)

	set, _ := mysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:%d-%d", masterUuid.String(), 1, 2))

	s, err := t.b.StartSyncGTID(set)
	c.Assert(err, IsNil)

	t.testSync(c, s)
}

func (t *testSyncerSuite) TestMariadbPositionSync(c *C) {
	t.testPostionSync(c, mysql.MariaDBFlavor)
}

func (t *testSyncerSuite) TestMariadbGTIDSync(c *C) {

}
