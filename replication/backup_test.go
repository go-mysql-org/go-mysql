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

	resetBinaryLogs := "RESET BINARY LOGS AND GTIDS"
	if eq, err := t.c.CompareServerVersion("8.4.0"); (err == nil) && (eq < 0) {
		resetBinaryLogs = "RESET MASTER"
	}

	t.testExecute(resetBinaryLogs)

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
	ctx, cancel := context.WithTimeout(context.Background(), failTimeout)
	defer cancel()
	select {
	case <-done:
		return
	case <-ctx.Done():
		t.T().Fatal("time out error")
	}
}
