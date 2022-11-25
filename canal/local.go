package canal

import (
	"context"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"time"
)

type LocalBinlogStreamerCreator func(mysql.Position) *replication.BinlogStreamer

type binlogFileDownload func(mysql.Position) (path string, err error)

func NewLocalBinlogStreamerCreator(download binlogFileDownload) LocalBinlogStreamerCreator {
	creator := func(position mysql.Position) *replication.BinlogStreamer {
		streamer := replication.NewBinlogStreamer()
		bf, err := download(position)
		if err != nil {
			streamer.CloseWithError(errors.New("Binlog File Not Exist"))
		}

		start := false
		go replication.NewBinlogParser().ParseFile(bf, 0, func(be *replication.BinlogEvent) error {
			if be.Header.LogPos == position.Pos || position.Pos == 4 {
				start = true
			}
			if start {
				streamer.PutEvent(be)
			}
			return nil
		})

		return streamer
	}

	return creator
}

// getEventAndSwitchStreamer 在线和本地binlog适配器(使用本地binlog文件适配replication.BinlogStreamer)的切换，并获取event
type getEventAndSwitchStreamer func(ctx context.Context, currentStreamer, prevStreamer *replication.BinlogStreamer) (
	curStreamer *replication.BinlogStreamer, preStreamer *replication.BinlogStreamer, evt *replication.BinlogEvent, err error)

func (c *Canal) getLocalBinlogSwitchFunc(creator LocalBinlogStreamerCreator) getEventAndSwitchStreamer {
	// this function enable to use localBinlogStreamerCreator set by SetLocalBinlogStreamer to created Streamer
	// lastStreamer is the streamer  startSyncer方式创建的streamer，当Rotate时将当前streamer退回到lastStreamer才能判断是否可以在线获取
	return func(ctx context.Context, currentStreamer, prevStreamer *replication.BinlogStreamer) (
		*replication.BinlogStreamer, *replication.BinlogStreamer, *replication.BinlogEvent, error) {
		var (
			ev  *replication.BinlogEvent
			err error
		)
		ev, err = currentStreamer.GetEvent(ctx)

		if err == nil {
			switch ev.Event.(type) {
			case *replication.RotateEvent: // 切换binlog事件，当前streamer切回上次streamer
				currentStreamer = prevStreamer
			}
			// 正常的event获取，不需要切换streamer
			return currentStreamer, prevStreamer, ev, err
		}

		if err == replication.ErrNeedSyncAgain { // 如果canal的syncer已关闭则重启syncer
			// reset syncer
			c.syncer.Close()
			_ = c.prepareSyncer()
			newStreamer, startErr := c.startSyncer()
			if startErr == nil {
				ev, err = newStreamer.GetEvent(ctx)
			}
			// 重启syncer后相当于重启了canal，此时prevStreamer需要更换为新的这个streamer
			// prevStreamer should change after reset syncer
			prevStreamer, currentStreamer = newStreamer, newStreamer
		}

		if mysqlErr, ok := err.(*mysql.MyError); ok {
			if mysqlErr.Code == 1236 && mysqlErr.Message == "Could not find first log file name in binary log index file" {
				gset := c.master.GTIDSet()
				if gset == nil || gset.String() == "" { // 暂时只支持xid模式
					c.cfg.Logger.Info("Could not find first log, try with local binlog")
					pos := c.master.Position()
					newStreamer := creator(pos) // 创建本地binlog文件适配器，并将当前streamer替换为这个适配器
					evt, e := newStreamer.GetEvent(ctx)
					return newStreamer, currentStreamer, evt, e
				}
			}
		}

		return currentStreamer, prevStreamer, ev, err
	}
}

// RunFromWithLocalBinlog will sync from the binlog position directly, ignore mysqldump.
func (c *Canal) RunFromWithLocalBinlog(pos mysql.Position, creator LocalBinlogStreamerCreator) error {
	c.master.Update(pos)

	return c.runWithLocalBinlog(creator)
}

// RunWithLocalBinlog will first try to dump all data from MySQL master `mysqldump`,
// then sync from the binlog position in the dump data.
// It will run forever until meeting an error or Canal closed.
func (c *Canal) RunWithLocalBinlog(creator LocalBinlogStreamerCreator) error {
	return c.runWithLocalBinlog(creator)
}

func (c *Canal) runWithLocalBinlog(creator LocalBinlogStreamerCreator) error {
	defer func() {
		c.cancel()
	}()

	c.master.UpdateTimestamp(uint32(time.Now().Unix()))

	if !c.dumped {
		c.dumped = true

		err := c.tryDump()
		close(c.dumpDoneCh)

		if err != nil {
			c.cfg.Logger.Errorf("canal dump mysql err: %v", err)
			return errors.Trace(err)
		}
	}

	if err := c.runSyncBinlog(c.getLocalBinlogSwitchFunc(creator)); err != nil {
		if errors.Cause(err) != context.Canceled {
			c.cfg.Logger.Errorf("canal start sync binlog err: %v", err)
			return errors.Trace(err)
		}
	}

	return nil
}
