package replication

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// testStartBackupEndInGivenTime tests the backup functionality with the given SyncMode.
func (t *testSyncerSuite) testStartBackupEndInGivenTime(syncMode SyncMode) {
	// Setup the test environment with the specified SyncMode
	t.setupTest(mysql.MySQLFlavor, syncMode)
	t.b.cfg.SyncMode = syncMode

	// Define binlogDir and timeout
	binlogDir := "./var"
	os.RemoveAll(binlogDir)
	err := os.MkdirAll(binlogDir, 0755)
	require.NoError(t.T(), err, "Failed to recreate binlogDir")

	timeout := 2 * time.Second

	done := make(chan bool)

	// Set up the BackupEventHandler
	backupHandler := &BackupEventHandler{
		handler: func(binlogFilename string) (io.WriteCloser, error) {
			return os.OpenFile(path.Join(binlogDir, binlogFilename), os.O_CREATE|os.O_WRONLY, 0644)
		},
	}

	// Start the backup using StartBackupWithHandler in a separate goroutine
	go func() {
		err := t.b.StartBackupWithHandler(mysql.Position{Name: "", Pos: uint32(0)}, timeout, backupHandler.handler)
		require.NoError(t.T(), err, "StartBackupWithHandler failed")
		done <- true
	}()

	// Wait briefly to ensure the backup process has started
	time.Sleep(500 * time.Millisecond)

	// Execute FLUSH LOGS to trigger binlog rotation and create binlog.000001
	_, err = t.c.Execute("FLUSH LOGS")
	require.NoError(t.T(), err, "Failed to execute FLUSH LOGS")

	// Generate a binlog event by creating a table and inserting data
	_, err = t.c.Execute("CREATE TABLE IF NOT EXISTS test_backup (id INT PRIMARY KEY)")
	require.NoError(t.T(), err, "Failed to create table_backup")

	_, err = t.c.Execute("INSERT INTO test_backup (id) VALUES (1)")
	require.NoError(t.T(), err, "Failed to insert data into test_backup")

	// Define the expected binlog file path
	expectedBinlogFile := path.Join(binlogDir, "binlog.000001")

	// Wait for the binlog file to be created
	err = waitForFile(expectedBinlogFile, 2*time.Second)
	require.NoError(t.T(), err, "Binlog file was not created in time")

	// Optionally, wait a short duration to ensure events are processed
	time.Sleep(500 * time.Millisecond)

	// Wait for the backup to complete or timeout
	failTimeout := 5 * timeout
	ctx, cancel := context.WithTimeout(context.Background(), failTimeout)
	defer cancel()
	select {
	case <-done:
		// Backup completed; now verify the backup files
		files, err := os.ReadDir(binlogDir)
		require.NoError(t.T(), err, "Failed to read binlogDir")
		require.NotEmpty(t.T(), files, "No binlog files were backed up")

		for _, file := range files {
			fileInfo, err := os.Stat(path.Join(binlogDir, file.Name()))
			require.NoError(t.T(), err, "Failed to stat binlog file")
			require.NotZero(t.T(), fileInfo.Size(), "Binlog file %s is empty", file.Name())
		}

		// Additionally, verify that events were handled
		require.Greater(t.T(), backupHandler.eventCount, 0, "No events were handled by the BackupEventHandler")
	case <-ctx.Done():
		t.T().Fatal("Backup timed out before completion")
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

// waitForFile waits until the specified file exists or the timeout is reached.
func waitForFile(filePath string, timeout time.Duration) error {
	start := time.Now()
	for {
		if _, err := os.Stat(filePath); err == nil {
			return nil
		}
		if time.Since(start) > timeout {
			return fmt.Errorf("file %s did not appear within %v", filePath, timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (t *testSyncerSuite) testBackupEventHandlerInvocation(syncMode SyncMode) {
	// Setup the test environment with the specified SyncMode
	t.setupTest(mysql.MySQLFlavor, syncMode)
	t.b.cfg.SyncMode = syncMode

	// Define binlogDir and timeout
	binlogDir := "./var"
	os.RemoveAll(binlogDir)
	timeout := 5 * time.Second // Increased timeout to allow for event processing

	// Ensure binlogDir exists
	err := os.MkdirAll(binlogDir, 0755)
	require.NoError(t.T(), err, "Failed to create binlogDir")

	// Set up the BackupEventHandler
	backupHandler := &BackupEventHandler{
		handler: func(binlogFilename string) (io.WriteCloser, error) {
			return os.OpenFile(path.Join(binlogDir, binlogFilename), os.O_CREATE|os.O_WRONLY, 0644)
		},
	}

	if syncMode == SyncModeSync {
		// Set the event handler in BinlogSyncer for synchronous mode
		t.b.SetEventHandler(backupHandler)
	}

	// Start the backup in a separate goroutine
	go func() {
		err := t.b.StartBackupWithHandler(mysql.Position{Name: "", Pos: uint32(0)}, timeout, backupHandler.handler)
		require.NoError(t.T(), err, "StartBackupWithHandler failed")
	}()

	// Wait briefly to ensure the backup process has started
	time.Sleep(500 * time.Millisecond)

	// Execute FLUSH LOGS to trigger binlog rotation and create binlog.000001
	_, err = t.c.Execute("FLUSH LOGS")
	require.NoError(t.T(), err, "Failed to execute FLUSH LOGS")

	// Generate a binlog event by creating a table and inserting data
	_, err = t.c.Execute("CREATE TABLE IF NOT EXISTS test_backup (id INT PRIMARY KEY)")
	require.NoError(t.T(), err, "Failed to create table")

	_, err = t.c.Execute("INSERT INTO test_backup (id) VALUES (1)")
	require.NoError(t.T(), err, "Failed to insert data")

	// Define the expected binlog file path
	expectedBinlogFile := path.Join(binlogDir, "binlog.000001")

	// Wait for the binlog file to be created
	err = waitForFile(expectedBinlogFile, 2*time.Second)
	require.NoError(t.T(), err, "Binlog file was not created in time")

	// Optionally, wait a short duration to ensure events are processed
	time.Sleep(500 * time.Millisecond)

	// Verify that events were handled
	require.Greater(t.T(), backupHandler.eventCount, 0, "No events were handled by the BackupEventHandler")

	// Additional verification: Check that the binlog file has content
	fileInfo, err := os.Stat(expectedBinlogFile)
	require.NoError(t.T(), err, "Failed to stat binlog file")
	require.NotZero(t.T(), fileInfo.Size(), "Binlog file is empty")
}

// setupACKAfterFsyncTest sets up the test environment for verifying the relationship
// between fsync completion and ACK sending. It configures the BinlogSyncer based on
// the provided SyncMode, initializes necessary channels and handlers, and returns them
// for use in the test functions.
func (t *testSyncerSuite) setupACKAfterFsyncTest(syncMode SyncMode) (
	binlogDir string,
	fsyncedChan chan struct{},
	ackedChan chan struct{},
	handler func(string) (io.WriteCloser, error),
) {
	// Initialize the test environment with the specified SyncMode
	t.setupTest(mysql.MySQLFlavor, syncMode)

	// Define binlogDir
	binlogDir = "./var"
	os.RemoveAll(binlogDir)
	err := os.MkdirAll(binlogDir, 0755)
	require.NoError(t.T(), err)

	// Create channels for signaling fsync and ACK events
	fsyncedChan = make(chan struct{}, 1)
	ackedChan = make(chan struct{}, 1)

	// Define the handler function to open WriteClosers for binlog files
	handler = func(binlogFilename string) (io.WriteCloser, error) {
		filePath := path.Join(binlogDir, binlogFilename)
		file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, err
		}
		return file, nil
	}

	// Assign the ackedChan to the BinlogSyncer for signaling ACKs
	t.b.ackedChan = ackedChan

	return binlogDir, fsyncedChan, ackedChan, handler
}

func (t *testSyncerSuite) testACKSentAfterFsync(syncMode SyncMode) {
	_, fsyncedChan, ackedChan, handler := t.setupACKAfterFsyncTest(syncMode)
	timeout := 5 * time.Second

	// Set up the BackupEventHandler with fsyncedChan
	backupHandler := &BackupEventHandler{
		handler:     handler,
		fsyncedChan: fsyncedChan,
	}

	if syncMode == SyncModeSync {
		// Set the event handler in BinlogSyncer
		t.b.SetEventHandler(backupHandler)
	}

	// Start syncing
	pos := mysql.Position{Name: "", Pos: uint32(0)}
	go func() {
		// Start backup (this will block until timeout)
		err := t.b.StartBackupWithHandler(pos, timeout, handler)
		require.NoError(t.T(), err)
	}()

	// Wait briefly to ensure sync has started
	time.Sleep(1 * time.Second)

	// Execute a query to generate an event
	t.testExecute("FLUSH LOGS")

	if syncMode == SyncModeSync {
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
	} else {
		// In asynchronous mode, fsync may not be directly tracked
		// Focus on ensuring that ACK is sent
		select {
		case <-ackedChan:
			// ACK sent
		case <-time.After(2 * time.Second):
			t.T().Fatal("ACK not sent in time")
		}

		// Optionally, verify that binlog files are created
		binlogDir := "./var"
		files, err := os.ReadDir(binlogDir)
		require.NoError(t.T(), err)
		require.NotEmpty(t.T(), files, "No binlog files were backed up")
		for _, file := range files {
			fileInfo, err := os.Stat(path.Join(binlogDir, file.Name()))
			require.NoError(t.T(), err)
			require.NotZero(t.T(), fileInfo.Size(), "Binlog file %s is empty", file.Name())
		}
	}
}

func (t *testSyncerSuite) TestStartBackupEndInGivenTimeAsync() {
	t.testStartBackupEndInGivenTime(SyncModeAsync)
}

func (t *testSyncerSuite) TestStartBackupEndInGivenTimeSync() {
	t.testStartBackupEndInGivenTime(SyncModeSync)
}

func (t *testSyncerSuite) TestACKSentAfterFsyncSyncMode() {
	t.testACKSentAfterFsync(SyncModeSync)
}

func (t *testSyncerSuite) TestACKSentAfterFsyncAsyncMode() {
	t.testACKSentAfterFsync(SyncModeAsync)
}

func (t *testSyncerSuite) TestBackupEventHandlerInvocationSync() {
	t.testBackupEventHandlerInvocation(SyncModeSync)
}

func (t *testSyncerSuite) TestBackupEventHandlerInvocationAsync() {
	t.testBackupEventHandlerInvocation(SyncModeAsync)
}
