package replication

import (
	"context"
	"os"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"

	"github.com/instructure/mc-go-mysql/mysql"
)

func (t *testSyncerSuite) TestStartBackupEndInGivenTime(c *C) {
	t.setupTest(c, mysql.MySQLFlavor)

	t.testExecute(c, "RESET MASTER")

	for times := 1; times <= 2; times++ {
		t.testSync(c, nil)
		t.testExecute(c, "FLUSH LOGS")
	}

	binlogDir := "./var"

	os.RemoveAll(binlogDir)
	timeout := 2 * time.Second

	done := make(chan bool)

	go func() {
		err := t.b.StartBackup(binlogDir, mysql.Position{Name: "", Pos: uint32(0)}, timeout)
		c.Assert(err, IsNil)
		done <- true
	}()
	failTimeout := 5 * timeout
	ctx, _ := context.WithTimeout(context.Background(), failTimeout)
	select {
	case <-done:
		return
	case <-ctx.Done():
		c.Assert(errors.New("time out error"), IsNil)
	}
}
