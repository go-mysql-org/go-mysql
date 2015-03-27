package replication

import (
	"io"
	"os"
	"path"

	. "github.com/siddontang/go-mysql/mysql"
)

// Like mysqlbinlog remote raw backup
// Backup remote binlog from position (filename, offset) and write in backupDir
func (b *BinlogSyncer) StartBackup(backupDir string, p Position) error {
	b.SetRawMode(true)

	os.MkdirAll(backupDir, 0755)

	s, err := b.StartSync(p)
	if err != nil {
		return err
	}

	var filename string
	var offset uint32

	var f *os.File
	defer func() {
		if f != nil {
			f.Close()
		}
	}()

	for {
		e, err := s.GetEvent()
		if err != nil {
			return err
		}

		offset = e.Header.LogPos

		if rotateEvent, ok := e.Event.(*RotateEvent); ok {
			filename = string(rotateEvent.NextLogName)
			//offset = uint32(rotateEvent.Position)
			if offset == 0 {
				//this is the dummy RotateEvent, we will close old one and create a new

				if f != nil {
					f.Close()
				}

				f, err = os.OpenFile(path.Join(backupDir, filename), os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					return err
				}

				// write binlog header fe'bin'
				if _, err = f.Write(BinLogFileHeader); err != nil {
					return err
				}

				continue
			}
		}

		if n, err := f.Write(e.RawData); err != nil {
			return err
		} else if n != len(e.RawData) {
			return io.ErrShortWrite
		}
	}

	return nil
}
