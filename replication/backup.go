package replication

import (
	"context"
	"io"
	"os"
	"path"
	"time"

	. "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
)

// StartBackup: Like mysqlbinlog remote raw backup
// Backup remote binlog from position (filename, offset) and write in backupDir
func (b *BinlogSyncer) StartBackupToFile(backupDir string, p Position, timeout time.Duration) error {
	return b.StartBackup(p, timeout, func(filename string) (io.WriteCloser, error) {
		err := os.MkdirAll(backupDir, 0755)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return os.OpenFile(path.Join(backupDir, filename), os.O_CREATE|os.O_WRONLY, 0644)
	})
}

// StartBackup initiates a backup process for binlog events starting from a specified position.
// It continuously fetches binlog events and writes them to files managed by a provided handler function.
// The backup process can be controlled with a timeout duration, after which the backup will stop if no new events are received.
//
// Parameters:
//   - p Position: The starting position in the binlog from which to begin the backup.
//   - timeout time.Duration: The maximum duration to wait for new binlog events before stopping the backup process.
//     If set to 0, a default very long timeout (30 days) is used instead.
//   - handler func(filename string) (io.WriteCloser, error): A function provided by the caller to handle file creation and writing.
//     This function is expected to return an io.WriteCloser for the specified filename, which will be used to write binlog events.
//
// The function first checks if a timeout is specified, setting a default if not. It then enables raw mode parsing for binlog events
// to ensure that events are not parsed but passed as raw data for backup. It starts syncing binlog events from the specified position
// and enters a loop to continuously fetch and write events.
//
// For each event, it checks the event type. If it's a ROTATE_EVENT, it updates the filename to the next log file as indicated by the event.
// If it's a FORMAT_DESCRIPTION_EVENT, it signifies the start of a new binlog file, and the function closes the current file (if open) and opens
// a new one using the handler function. It also writes the binlog file header to the new file.
//
// The function writes the raw data of each event to the current file and handles errors such as context deadline exceeded (timeout),
// write errors, or short writes.
func (b *BinlogSyncer) StartBackup(p Position, timeout time.Duration,
	handler func(filename string) (io.WriteCloser, error)) error {
	if timeout == 0 {
		// a very long timeout here
		timeout = 30 * 3600 * 24 * time.Second
	}

	// Force use raw mode
	b.parser.SetRawMode(true)

	s, err := b.StartSync(p)
	if err != nil {
		return errors.Trace(err)
	}

	var filename string
	var offset uint32

	var w io.WriteCloser
	defer func() {
		if w != nil {
			w.Close()
		}
	}()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		e, err := s.GetEvent(ctx)
		cancel()

		if err == context.DeadlineExceeded {
			return nil
		}

		if err != nil {
			return errors.Trace(err)
		}

		offset = e.Header.LogPos

		if e.Header.EventType == ROTATE_EVENT {
			rotateEvent := e.Event.(*RotateEvent)
			filename = string(rotateEvent.NextLogName)

			if e.Header.Timestamp == 0 || offset == 0 {
				// fake rotate event
				continue
			}
		} else if e.Header.EventType == FORMAT_DESCRIPTION_EVENT {
			// FormateDescriptionEvent is the first event in binlog, we will close old one and create a new

			if w != nil {
				_ = w.Close()
			}

			if len(filename) == 0 {
				return errors.Errorf("empty binlog filename for FormateDescriptionEvent")
			}

			w, err = handler(filename)
			if err != nil {
				return errors.Trace(err)
			}

			// write binlog header fe'bin'
			if _, err = w.Write(BinLogFileHeader); err != nil {
				return errors.Trace(err)
			}
		}

		if n, err := w.Write(e.RawData); err != nil {
			return errors.Trace(err)
		} else if n != len(e.RawData) {
			return errors.Trace(io.ErrShortWrite)
		}
	}
}
