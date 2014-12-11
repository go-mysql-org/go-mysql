package replication

import (
	"flag"
	. "gopkg.in/check.v1"
	"os"
	"testing"
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
}

var _ = Suite(&testSyncerSuite{})

func (t *testSyncerSuite) SetUpTest(c *C) {
	t.b = NewBinlogSyncer(100)
}

func (t *testSyncerSuite) TearDownTest(c *C) {
	t.b.Close()
}

func (t *testSyncerSuite) TestSync(c *C) {
	err := t.b.RegisterSlave(*testHost, uint16(*testPort), *testUser, *testPassword)
	c.Assert(err, IsNil)

	s, err := t.b.StartSync("", 4)
	c.Assert(err, IsNil)

	for {
		e, err := s.GetEvent()
		if err != nil {
			c.Fatal(err)
			break
		}

		e.Dump(os.Stderr)
		os.Stderr.Sync()
	}
}
