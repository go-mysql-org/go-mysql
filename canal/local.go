package canal

import (
	"context"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
)

// BinlogFileDownload download the binlog file from cloud computing platform (etc. aliyun)
type BinlogFileDownload func(mysql.Position) (localBinFilePath string, err error)

// WithLocalBinlogDownload registers the local bin file download,
// that allows download the flushed binlog file to local (etc. aliyun)
func (c *Canal) WithLocalBinlogDownload(d BinlogFileDownload) {
	c.binFileDownload = d
}

func (c *Canal) adaptLocalBinFileStreamer(syncMasterStreamer *replication.BinlogStreamer, err error) (*LocalBinFileAdapterStreamer, error) {
	return &LocalBinFileAdapterStreamer{
		BinlogStreamer:     syncMasterStreamer,
		syncMasterStreamer: syncMasterStreamer,
		canal:              c,
		binFileDownload:    c.binFileDownload,
	}, err
}

// LocalBinFileAdapterStreamer will support to download flushed binlog file for continuous sync in cloud computing platform
type LocalBinFileAdapterStreamer struct {
	*replication.BinlogStreamer                             // the running streamer, it will be localStreamer or sync master streamer
	syncMasterStreamer          *replication.BinlogStreamer // syncMasterStreamer is the streamer from startSyncer
	canal                       *Canal
	binFileDownload             BinlogFileDownload
}

// GetEvent will auto switch  the running streamer and return replication.BinlogEvent
func (s *LocalBinFileAdapterStreamer) GetEvent(ctx context.Context) (*replication.BinlogEvent, error) {
	if s.binFileDownload == nil { // not support to use local bin file
		return s.BinlogStreamer.GetEvent(ctx)
	}

	ev, err := s.BinlogStreamer.GetEvent(ctx)

	if err == nil {
		switch ev.Event.(type) {
		case *replication.RotateEvent: // RotateEvent means need to change steamer back to sync master to retry sync
			s.BinlogStreamer = s.syncMasterStreamer
		}
		return ev, err
	}

	if err == replication.ErrNeedSyncAgain { // restart master if last sync master syncer has error
		s.canal.syncer.Close()
		_ = s.canal.prepareSyncer()

		newStreamer, startErr := s.canal.startSyncer()
		if startErr == nil {
			ev, err = newStreamer.GetEvent(ctx)
		}
		// set all streamer to the new sync master streamer
		s.BinlogStreamer = newStreamer
		s.syncMasterStreamer = newStreamer
	}

	if mysqlErr, ok := err.(*mysql.MyError); ok {
		// change to local binlog file streamer to adapter the steamer
		if mysqlErr.Code == mysql.ER_MASTER_FATAL_ERROR_READING_BINLOG &&
			mysqlErr.Message == "Could not find first log file name in binary log index file" {
			gset := s.canal.master.GTIDSet()
			if gset == nil || gset.String() == "" { // currently only support xid mode
				s.canal.cfg.Logger.Info("Could not find first log, try to download the local binlog for retry")
				pos := s.canal.master.Position()
				newStreamer := newLocalBinFileStreamer(s.binFileDownload, pos)

				s.syncMasterStreamer = s.BinlogStreamer
				s.BinlogStreamer = newStreamer

				return newStreamer.GetEvent(ctx)
			}
		}
	}

	return ev, err
}

func newLocalBinFileStreamer(download BinlogFileDownload, position mysql.Position) *replication.BinlogStreamer {
	streamer := replication.NewBinlogStreamer()
	binFilePath, err := download(position)
	if err != nil {
		streamer.CloseWithError(errors.New("local binlog file not exist"))
	}

	go func(binFilePath string, streamer *replication.BinlogStreamer) {
		beginFromHere := false
		_ = replication.NewBinlogParser().ParseFile(binFilePath, 0, func(be *replication.BinlogEvent) error {
			if be.Header.LogPos == position.Pos || position.Pos == 4 { // go ahead to check if begin
				beginFromHere = true
			}
			if beginFromHere {
				streamer.PutEvent(be)
			}
			return nil
		})
	}(binFilePath, streamer)

	return streamer
}
