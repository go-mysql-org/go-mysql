package canal

import (
	"context"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
)

// BinlogFileDownloader downloads the binlog file and return the path to it. It's often used to download binlog backup from RDS service.
type BinlogFileDownloader func(mysql.Position) (localBinFilePath string, err error)

// WithLocalBinlogDownloader registers the local bin file downloader,
// that allows download the backup binlog file from RDS service to local
func (c *Canal) WithLocalBinlogDownloader(d BinlogFileDownloader) {
	c.binFileDownloader = d
}

func (c *Canal) adaptLocalBinFileStreamer(remoteBinlogStreamer *replication.BinlogStreamer, err error) (*localBinFileAdapterStreamer, error) {
	return &localBinFileAdapterStreamer{
		BinlogStreamer:     remoteBinlogStreamer,
		syncMasterStreamer: remoteBinlogStreamer,
		canal:              c,
		binFileDownloader:  c.binFileDownloader,
	}, err
}

// localBinFileAdapterStreamer will support to download flushed binlog file for continuous sync in cloud computing platform
type localBinFileAdapterStreamer struct {
	*replication.BinlogStreamer                             // the running streamer, it will be localStreamer or sync master streamer
	syncMasterStreamer          *replication.BinlogStreamer // syncMasterStreamer is the streamer from canal startSyncer
	canal                       *Canal
	binFileDownloader           BinlogFileDownloader
}

// GetEvent will auto switch the local and remote streamer to get binlog event if possible.
func (s *localBinFileAdapterStreamer) GetEvent(ctx context.Context) (*replication.BinlogEvent, error) {
	if s.binFileDownloader == nil { // not support to use local bin file
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
		if startErr != nil {
			return nil, startErr
		}
		ev, err = newStreamer.GetEvent(ctx)
		// set all streamer to the new sync master streamer
		s.BinlogStreamer = newStreamer
		s.syncMasterStreamer = newStreamer
	}

	if mysqlErr, ok := err.(*mysql.MyError); ok {
		// change to local binlog file streamer to adapter the steamer
		if mysqlErr.Code == mysql.ER_MASTER_FATAL_ERROR_READING_BINLOG &&
			mysqlErr.Message == "Could not find first log file name in binary log index file" {
			gset := s.canal.master.GTIDSet()
			if gset == nil || gset.String() == "" { // currently only support position based replication
				s.canal.cfg.Logger.Info("Could not find first log, try to download the local binlog for retry")
				pos := s.canal.master.Position()
				newStreamer := newLocalBinFileStreamer(s.binFileDownloader, pos)

				s.syncMasterStreamer = s.BinlogStreamer
				s.BinlogStreamer = newStreamer

				return newStreamer.GetEvent(ctx)
			}
		}
	}

	return ev, err
}

func newLocalBinFileStreamer(download BinlogFileDownloader, position mysql.Position) *replication.BinlogStreamer {
	streamer := replication.NewBinlogStreamer()
	binFilePath, err := download(position)
	if err != nil {
		streamer.CloseWithError(errors.New("local binlog file not exist"))
	}

	go func(binFilePath string, streamer *replication.BinlogStreamer) {
		beginFromHere := false
		err := replication.NewBinlogParser().ParseFile(binFilePath, 0, func(be *replication.BinlogEvent) error {
			if be.Header.LogPos == position.Pos || position.Pos == 4 { // go ahead to check if begin
				beginFromHere = true
			}
			if beginFromHere {
				streamer.PutEvent(be)
			}
			return nil
		})
		if err != nil {
			streamer.CloseWithError(err)
		}
	}(binFilePath, streamer)

	return streamer
}
