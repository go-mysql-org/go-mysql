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

// TestAsyncBackup runs the backup process in asynchronous mode and verifies binlog file creation.
func (t *testSyncerSuite) TestAsyncBackup() {
	testSyncModeBackup(t, SyncModeAsync)
}

// TestSyncBackup runs the backup process in synchronous mode and verifies binlog file creation.
func (t *testSyncerSuite) TestSyncBackup() {
	testSyncModeBackup(t, SyncModeSync)
}

// testSyncModeBackup is a helper function that runs the backup process for a given sync mode and checks if binlog files are written correctly.
func testSyncModeBackup(t *testSyncerSuite, syncMode SyncMode) {
	t.setupTest(mysql.MySQLFlavor)
	t.b.cfg.SemiSyncEnabled = false // Ensure semi-sync is disabled
	t.b.cfg.SyncMode = syncMode     // Set the sync mode

	binlogDir := "./var"
	os.RemoveAll(binlogDir)
	timeout := 3 * time.Second

	done := make(chan bool)

	// Start the backup process in a goroutine
	go func() {
		err := t.b.StartBackup(binlogDir, mysql.Position{Name: "", Pos: uint32(0)}, timeout)
		require.NoError(t.T(), err)
		done <- true
	}()

	failTimeout := 2 * timeout
	ctx, cancel := context.WithTimeout(context.Background(), failTimeout)
	defer cancel()

	// Wait for the backup to complete or timeout
	select {
	case <-done:
		// Check if binlog files are written to the specified directory
		files, err := os.ReadDir(binlogDir)
		require.NoError(t.T(), err, "Failed to read binlog directory")
		require.Greater(t.T(), len(files), 0, "Binlog files were not written to the directory")
		t.T().Logf("Backup completed successfully in %v mode with %d binlog file(s).", syncMode, len(files))
	case <-ctx.Done():
		t.T().Fatalf("Timeout error during backup in %v mode.", syncMode)
	}
}
