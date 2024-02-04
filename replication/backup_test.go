package replication

import (
	"context"
	"os"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-mysql-org/go-mysql/mysql"
)

func (t *testSyncerSuite) TestStartBackupEndInGivenTime() {
	t.setupTest(mysql.MySQLFlavor)

	t.testExecute("RESET MASTER")

	for times := 1; times <= 2; times++ {
		t.testSync(nil)
		t.testExecute("FLUSH LOGS")
	}

	binlogDir := "./var"

	os.RemoveAll(binlogDir)
	timeout := 2 * time.Second

	done := make(chan bool)

	go func() {
		err := t.b.StartBackup(binlogDir, mysql.Position{Name: "", Pos: uint32(0)}, timeout)
		require.NoError(t.T(), err)
		done <- true
	}()
	failTimeout := 5 * timeout
	ctx, _ := context.WithTimeout(context.Background(), failTimeout)
	select {
	case <-done:
		return
	case <-ctx.Done():
		t.T().Fatal("time out error")
	}
}
