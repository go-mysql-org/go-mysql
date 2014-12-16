package replication

import (
	"flag"
	"fmt"
	"github.com/siddontang/go-mysql/client"
	. "gopkg.in/check.v1"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var testHost = flag.String("host", "127.0.0.1", "MySQL master host")
var testPort = flag.Int("port", 3306, "MySQL master port")
var testUser = flag.String("user", "root", "MySQL master user")
var testPassword = flag.String("pass", "", "MySQL master password")

var testGTIDHost = flag.String("gtid_host", "127.0.0.1", "MySQL master (uses GTID) host")
var testGTIDPort = flag.Int("gtid_port", 3307, "MySQL master (uses GTID) port")
var testGTIDUser = flag.String("gtid_user", "root", "MySQL master (uses GTID) user")
var testGITDPassword = flag.String("gtid_pass", "", "MySQL master (uses GTID) password")

func TestBinLogSyncer(t *testing.T) {
	TestingT(t)
}

type testSyncerSuite struct {
	b *BinlogSyncer
	c *client.Conn

	wg sync.WaitGroup
}

var _ = Suite(&testSyncerSuite{})

func (t *testSyncerSuite) SetUpSuite(c *C) {
	var err error

	t.c, err = client.Connect(fmt.Sprintf("%s:%d", *testHost, *testPort), *testUser, *testPassword, "test")
	c.Assert(err, IsNil)
}

func (t *testSyncerSuite) TearDownSuite(c *C) {
	if t.c != nil {
		t.c.Close()
	}
}

func (t *testSyncerSuite) SetUpTest(c *C) {
	t.b = NewBinlogSyncer(100)
}

func (t *testSyncerSuite) TearDownTest(c *C) {
	t.b.Close()
}

func (t *testSyncerSuite) testExecute(c *C, query string) {
	_, err := t.c.Execute(query)
	c.Assert(err, IsNil)
}

func (t *testSyncerSuite) TestSync(c *C) {

	err := t.b.RegisterSlave(*testHost, uint16(*testPort), *testUser, *testPassword)
	c.Assert(err, IsNil)

	//get current master binlog file and position
	r, err := t.c.Execute("SHOW MASTER STATUS")
	c.Assert(err, IsNil)
	binFile, _ := r.GetString(0, 0)
	binPos, _ := r.GetInt(0, 1)

	if len(binFile) > 0 {
		seps := strings.Split(binFile, ".")
		n, err := strconv.Atoi(seps[1])
		c.Assert(err, IsNil)
		binFile = fmt.Sprintf("%s.%06d", seps[0], n+1)
		binPos = 4
	}

	//	t.testExecute(c, "FLUSH LOGS")

	s, err := t.b.StartSync(binFile, uint32(binPos))
	c.Assert(err, IsNil)

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

			e.Dump(os.Stderr)
			os.Stderr.Sync()
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
	      PRIMARY KEY (id)
	       ) ENGINE=InnoDB DEFAULT CHARSET=utf8`

	t.testExecute(c, str)

	//use row format
	t.testExecute(c, "SET SESSION binlog_format = 'ROW'")
	t.testExecute(c, `INSERT INTO test_replication (str, f, i) VALUES ("3", 3.14, 10)`)
	t.testExecute(c, `INSERT INTO test_replication (str, d, i) VALUES ("4", 3.14, 100)`)
	t.testExecute(c, `UPDATE test_replication SET f = 2.0 WHERE id = 1`)
	t.testExecute(c, `DELETE FROM test_replication WHERE id = 2`)
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

	t.wg.Wait()
}
