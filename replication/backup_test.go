package replication

import (
	"context"
	"io"
	"os"
	"path"
	"sync"
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

	// Start the backup in a goroutine
	go func() {
		err := t.b.StartBackup(binlogDir, mysql.Position{Name: "", Pos: uint32(0)}, timeout)
		require.NoError(t.T(), err)
		done <- true
	}()

	// Wait for the backup to complete or timeout
	failTimeout := 5 * timeout
	ctx, cancel := context.WithTimeout(context.Background(), failTimeout)
	defer cancel()
	select {
	case <-done:
		// Backup completed; now verify the backup files
		files, err := os.ReadDir(binlogDir)
		require.NoError(t.T(), err)
		require.NotEmpty(t.T(), files, "No binlog files were backed up")

		for _, file := range files {
			fileInfo, err := os.Stat(path.Join(binlogDir, file.Name()))
			require.NoError(t.T(), err)
			require.NotZero(t.T(), fileInfo.Size(), "Binlog file %s is empty", file.Name())
		}

		return
	case <-ctx.Done():
		t.T().Fatal("time out error")
	}
}

type CountingEventHandler struct {
	count int
	mutex sync.Mutex
}

func (h *CountingEventHandler) HandleEvent(e *BinlogEvent) error {
	h.mutex.Lock()
	h.count++
	h.mutex.Unlock()
	return nil
}

func (t *testSyncerSuite) TestBackupEventHandlerInvocation() {
	t.setupTest(mysql.MySQLFlavor)

	// Define binlogDir and timeout
	binlogDir := "./var"
	os.RemoveAll(binlogDir)
	timeout := 2 * time.Second

	// Set up the CountingEventHandler
	handler := &CountingEventHandler{}
	t.b.SetEventHandler(handler)

	// Start the backup
	err := t.b.StartBackup(binlogDir, mysql.Position{Name: "", Pos: uint32(0)}, timeout)
	require.NoError(t.T(), err)

	// Verify that events were handled
	handler.mutex.Lock()
	eventCount := handler.count
	handler.mutex.Unlock()
	require.Greater(t.T(), eventCount, 0, "No events were handled by the EventHandler")
}

func (t *testSyncerSuite) TestACKSentAfterFsync() {
	t.setupTest(mysql.MySQLFlavor)

	// Define binlogDir and timeout
	binlogDir := "./var"
	os.RemoveAll(binlogDir)
	timeout := 5 * time.Second

	// Create channels for signaling
	fsyncedChan := make(chan struct{}, 1)
	ackedChan := make(chan struct{}, 1)

	// Set up the BackupEventHandler with fsyncedChan
	backupHandler := &BackupEventHandler{
		handler: func(binlogFilename string) (io.WriteCloser, error) {
			return os.OpenFile(path.Join(binlogDir, binlogFilename), os.O_CREATE|os.O_WRONLY, 0644)
		},
		fsyncedChan: fsyncedChan,
	}

	// Set the event handler in BinlogSyncer
	t.b.SetEventHandler(backupHandler)

	// Set the ackedChan in BinlogSyncer
	t.b.ackedChan = ackedChan
	t.b.cfg.SemiSyncEnabled = true // Ensure semi-sync is enabled

	// Start syncing
	pos := mysql.Position{Name: "", Pos: uint32(0)}
	go func() {
		// Start backup (this will block until timeout)
		err := t.b.StartBackupWithHandler(pos, timeout, backupHandler.handler)
		require.NoError(t.T(), err)
	}()

	// Wait briefly to ensure sync has started
	time.Sleep(1 * time.Second)

	// Execute a query to generate an event
	t.testExecute("FLUSH LOGS")

	// Wait for fsync signal
	select {
	case <-fsyncedChan:
		// fsync completed
	case <-time.After(2 * time.Second):
		t.T().Fatal("fsync did not complete in time")
	}

	// Record the time when fsync completed
	fsyncTime := time.Now()

	// Wait for ACK signal
	select {
	case <-ackedChan:
		// ACK sent
	case <-time.After(2 * time.Second):
		t.T().Fatal("ACK not sent in time")
	}

	// Record the time when ACK was sent
	ackTime := time.Now()

	// Assert that ACK was sent after fsync
	require.True(t.T(), ackTime.After(fsyncTime), "ACK was sent before fsync completed")
}
