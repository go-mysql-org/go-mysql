package replication

import (
	"context"
	"os"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"

	"github.com/go-mysql-org/go-mysql/mysql"
)

func (t *testSyncerSuite) TestStartBackupEndInGivenTime(c *C) {
	t.setupTest(c, mysql.MySQLFlavor)

	t.testExecute(c, "RESET MASTER")

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	go func() {
		defer wg.Done()

		t.testSync(c, nil)

		t.testExecute(c, "FLUSH LOGS")

		t.testSync(c, nil)
	}()

	os.RemoveAll("./var")
	timeout := 2 * time.Second

	done := make(chan bool)

	go func() {
		err := t.b.StartBackup("./var", mysql.Position{Name: "", Pos: uint32(0)}, timeout)
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
