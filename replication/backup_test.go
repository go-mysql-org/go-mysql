package replication

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/google/uuid"

	"github.com/stretchr/testify/require"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// TestStartBackupEndInGivenTime tests the backup process completes within a given time.
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

	done := make(chan struct{})

	go func() {
		err := t.b.StartBackup(binlogDir, mysql.Position{Name: "", Pos: uint32(0)}, timeout)
		require.NoError(t.T(), err)
		close(done)
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
	testBackup(t, false) // false indicates asynchronous mode
}

// TestSyncBackup runs the backup process in synchronous mode and verifies binlog file creation.
func (t *testSyncerSuite) TestSyncBackup() {
	testBackup(t, true) // true indicates synchronous mode
}

// TestAsyncBackupWithGTID runs the backup process in asynchronous mode with GTID and verifies binlog file creation.
func (t *testSyncerSuite) TestAsyncBackupWithGTID() {
	testBackUpWithGTID(t, false) // false indicates asynchronous mode
}

// TestSyncBackupWithGTID runs the backup process in synchronous mode with GTID and verifies binlog file creation.
func (t *testSyncerSuite) TestSyncBackupWithGTID() {
	testBackUpWithGTID(t, true) // true indicates synchronous mode
}

// testBackup is a helper function that runs the backup process in the specified mode and checks if binlog files are written correctly.
func testBackup(t *testSyncerSuite, isSynchronous bool) {
	t.setupTest(mysql.MySQLFlavor)
	t.b.cfg.SemiSyncEnabled = false // Ensure semi-sync is disabled

	binlogDir := "./var"
	os.RemoveAll(binlogDir)
	timeout := 3 * time.Second

	if isSynchronous {
		// Set up a BackupEventHandler for synchronous mode
		backupHandler := NewBackupEventHandler(
			func(filename string) (io.WriteCloser, error) {
				return os.OpenFile(path.Join(binlogDir, filename), os.O_CREATE|os.O_WRONLY, 0o644)
			},
		)
		t.b.cfg.SynchronousEventHandler = backupHandler
	} else {
		// Ensure SynchronousEventHandler is nil for asynchronous mode
		t.b.cfg.SynchronousEventHandler = nil
	}

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
		mode := modeLabel(isSynchronous)
		t.T().Logf("Backup completed successfully in %s mode with %d binlog file(s).", mode, len(files))
	case <-ctx.Done():
		mode := modeLabel(isSynchronous)
		t.T().Fatalf("Timeout error during backup in %s mode.", mode)
	}
}

func testBackUpWithGTID(t *testSyncerSuite, isSynchronous bool) {
	t.setupTest(mysql.MySQLFlavor)
	t.b.cfg.SemiSyncEnabled = false // Ensure semi-sync is disabled

	binlogDir := "./var"
	os.RemoveAll(binlogDir)
	timeout := 3 * time.Second

	if isSynchronous {
		// Set up a BackupEventHandler for synchronous mode
		backupHandler := NewBackupEventHandler(
			func(filename string) (io.WriteCloser, error) {
				return os.OpenFile(path.Join(binlogDir, filename), os.O_CREATE|os.O_WRONLY, 0o644)
			},
		)
		t.b.cfg.SynchronousEventHandler = backupHandler
	} else {
		// Ensure SynchronousEventHandler is nil for asynchronous mode
		t.b.cfg.SynchronousEventHandler = nil
	}

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
	done := make(chan bool)

	// Start the backup process in a goroutine
	go func() {
		err := t.b.StartBackupGTID(binlogDir, set, timeout)
		require.NoError(t.T(), err)
		done <- true
	}()

	failTimeout := 2 * timeout
	ctx, cancel := context.WithTimeout(context.Background(), failTimeout)
	defer cancel()

	// Wait for the backup to complete or timeout
	select {
	case <-done:
		files, err := os.ReadDir(binlogDir)
		require.NoError(t.T(), err, "Failed to read binlog directory")
		require.Greater(t.T(), len(files), 0, "Binlog files were not written to the directory")
		mode := modeLabel(isSynchronous)
		t.T().Logf("Backup completed successfully in %s mode using GTID with %d binlog file(s).", mode, len(files))
	case <-ctx.Done():
		mode := modeLabel(isSynchronous)
		t.T().Fatalf("Timeout error during backup in %s mode.", mode)
	}
}

func modeLabel(isSynchronous bool) string {
	if isSynchronous {
		return "synchronous"
	}
	return "asynchronous"
}
