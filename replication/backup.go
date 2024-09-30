package replication

import (
	"context"
	"io"
	"os"
	"path"
	"sync"
	"time"

	. "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
)

// StartBackup: Like mysqlbinlog remote raw backup
// Backup remote binlog from position (filename, offset) and write in backupDir
func (b *BinlogSyncer) StartBackup(backupDir string, p Position, timeout time.Duration) error {
	err := os.MkdirAll(backupDir, 0755)
	if err != nil {
		return errors.Trace(err)
	}
	return b.StartBackupWithHandler(p, timeout, func(filename string) (io.WriteCloser, error) {
		return os.OpenFile(path.Join(backupDir, filename), os.O_CREATE|os.O_WRONLY, 0644)
	})
}

// StartBackupWithHandler starts the backup process for the binary log using the specified position and handler.
// The process will continue until the timeout is reached or an error occurs.
//
// Parameters:
//   - p: The starting position in the binlog from which to begin the backup.
//   - timeout: The maximum duration to wait for new binlog events before stopping the backup process.
//     If set to 0, a default very long timeout (30 days) is used instead.
//   - handler: A function that takes a binlog filename and returns an WriteCloser for writing raw events to.
func (b *BinlogSyncer) StartBackupWithHandler(p Position, timeout time.Duration,
	handler func(binlogFilename string) (io.WriteCloser, error)) (retErr error) {
	if timeout == 0 {
		// a very long timeout here
		timeout = 30 * 3600 * 24 * time.Second
	}

	// Force use raw mode
	b.parser.SetRawMode(true)

	// Set up the backup event handler
	backupHandler := &BackupEventHandler{
		handler: handler,
	}

	// Set the event handler in BinlogSyncer
	b.SetEventHandler(backupHandler)

	// Start syncing
	s, err := b.StartSync(p)
	if err != nil {
		return errors.Trace(err)
	}

	defer func() {
		b.SetEventHandler(nil) // Reset the event handler
		if backupHandler.w != nil {
			closeErr := backupHandler.w.Close()
			if retErr == nil {
				retErr = closeErr
			}
		}
	}()

	// Wait until the context is done or an error occurs
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case <-ctx.Done():
		return nil
	case <-b.ctx.Done():
		return nil
	case err := <-s.ech:
		return errors.Trace(err)
	}
}

// BackupEventHandler handles writing events for backup
type BackupEventHandler struct {
	handler func(binlogFilename string) (io.WriteCloser, error)
	w       io.WriteCloser
	file    *os.File
	mutex   sync.Mutex
}

func (h *BackupEventHandler) HandleEvent(e *BinlogEvent) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	switch e.Header.EventType {
	case ROTATE_EVENT:
		rotateEvent := e.Event.(*RotateEvent)
		filename := string(rotateEvent.NextLogName)

		// Close existing file if open
		if h.w != nil {
			if err := h.w.Close(); err != nil {
				h.w = nil
				return errors.Trace(err)
			}
		}

		// Open new file
		var err error
		h.w, err = h.handler(filename)
		if err != nil {
			return errors.Trace(err)
		}

		// Ensure w is an *os.File to call Sync
		if f, ok := h.w.(*os.File); ok {
			h.file = f
		} else {
			return errors.New("handler did not return *os.File, cannot fsync")
		}

		// Write binlog header
		if _, err := h.w.Write(BinLogFileHeader); err != nil {
			return errors.Trace(err)
		}

		// fsync after writing header
		if err := h.file.Sync(); err != nil {
			return errors.Trace(err)
		}

	default:
		// Write raw event data
		if n, err := h.w.Write(e.RawData); err != nil {
			return errors.Trace(err)
		} else if n != len(e.RawData) {
			return errors.Trace(io.ErrShortWrite)
		}

		// fsync after writing event
		if err := h.file.Sync(); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}
