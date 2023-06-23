package canal

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/ast"
)

func (c *Canal) startSyncer() (*replication.BinlogStreamer, error) {
	gset := c.master.GTIDSet()
	if gset == nil || gset.String() == "" {
		pos := c.master.Position()
		s, err := c.syncer.StartSync(pos)
		if err != nil {
			return nil, errors.Errorf("start sync replication at binlog %v error %v", pos, err)
		}
		c.cfg.Logger.Infof("start sync binlog at binlog file %v", pos)
		return s, nil
	} else {
		gsetClone := gset.Clone()
		s, err := c.syncer.StartSyncGTID(gset)
		if err != nil {
			return nil, errors.Errorf("start sync replication at GTID set %v error %v", gset, err)
		}
		c.cfg.Logger.Infof("start sync binlog at GTID set %v", gsetClone)
		return s, nil
	}
}

func (c *Canal) runSyncBinlog() error {
	s, err := c.startSyncer()
	if err != nil {
		return err
	}

	savePos := false
	force := false

	for {
		ev, err := s.GetEvent(c.ctx)
		if err != nil {
			return errors.Trace(err)
		}

		// Update the delay between the Canal and the Master before the handler hooks are called
		c.updateReplicationDelay(ev)

		// If log pos equals zero then the received event is a fake rotate event and
		// contains only a name of the next binlog file
		// See https://github.com/mysql/mysql-server/blob/8e797a5d6eb3a87f16498edcb7261a75897babae/sql/rpl_binlog_sender.h#L235
		// and https://github.com/mysql/mysql-server/blob/8cc757da3d87bf4a1f07dcfb2d3c96fed3806870/sql/rpl_binlog_sender.cc#L899
		if ev.Header.LogPos == 0 {
			switch e := ev.Event.(type) {
			case *replication.RotateEvent:
				fakeRotateLogName := string(e.NextLogName)
				c.cfg.Logger.Infof("received fake rotate event, next log name is %s", e.NextLogName)
				if fakeRotateLogName != c.master.Position().Name {
					c.cfg.Logger.Info("log name changed, the fake rotate event will be handled as a real rotate event")
				} else {
					continue
				}
			default:
				continue
			}
		}

		savePos = false
		force = false
		pos := c.master.Position()

		curPos := pos.Pos

		// next binlog pos
		pos.Pos = ev.Header.LogPos

		// We only save position with RotateEvent and XIDEvent.
		// For RowsEvent, we can't save the position until meeting XIDEvent
		// which tells the whole transaction is over.
		// TODO: If we meet any DDL query, we must save too.
		switch e := ev.Event.(type) {
		case *replication.RotateEvent:
			pos.Name = string(e.NextLogName)
			pos.Pos = uint32(e.Position)
			c.cfg.Logger.Infof("rotate binlog to %s", pos)
			savePos = true
			force = true
			if err = c.eventHandler.OnRotate(ev.Header, e); err != nil {
				return errors.Trace(err)
			}
		case *replication.RowsEvent:
			// we only focus row based event
			err = c.handleRowsEvent(ev)
			if err != nil {
				e := errors.Cause(err)
				// if error is not ErrExcludedTable or ErrTableNotExist or ErrMissingTableMeta, stop canal
				if e != ErrExcludedTable &&
					e != schema.ErrTableNotExist &&
					e != schema.ErrMissingTableMeta {
					c.cfg.Logger.Errorf("handle rows event at (%s, %d) error %v", pos.Name, curPos, err)
					return errors.Trace(err)
				}
			}
			continue
		case *replication.XIDEvent:
			savePos = true
			// try to save the position later
			if err := c.eventHandler.OnXID(ev.Header, pos); err != nil {
				return errors.Trace(err)
			}
			if e.GSet != nil {
				c.master.UpdateGTIDSet(e.GSet)
			}
		case *replication.MariadbGTIDEvent:
			// try to save the GTID later
			gtid, err := mysql.ParseMariadbGTIDSet(e.GTID.String())
			if err != nil {
				return errors.Trace(err)
			}
			if err := c.eventHandler.OnGTID(ev.Header, gtid); err != nil {
				return errors.Trace(err)
			}
		case *replication.GTIDEvent:
			u, _ := uuid.FromBytes(e.SID)
			gtid, err := mysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:%d", u.String(), e.GNO))
			if err != nil {
				return errors.Trace(err)
			}
			if err := c.eventHandler.OnGTID(ev.Header, gtid); err != nil {
				return errors.Trace(err)
			}
		case *replication.QueryEvent:
			stmts, _, err := c.parser.Parse(string(e.Query), "", "")
			if err != nil {
				c.cfg.Logger.Errorf("parse query(%s) err %v, will skip this event", e.Query, err)
				continue
			}
			posInfo := &Position{
				Position: pos,
				SavePos:  savePos,
				Force:    force,
			}
			for _, stmt := range stmts {
				nodes := parseDDLStmt(stmt)
				if len(nodes) > 0 {
					posInfo.SavePos = true
					posInfo.Force = true
					err := c.handleDDLEvent(ev, e, nodes, posInfo)
					if err != nil {
						c.cfg.Logger.Errorf("handle ddl event err %v", err)
					}
				} else {
					err := c.handleQueryEvent(ev, e, stmt, posInfo)
					if err != nil {
						c.cfg.Logger.Errorf("handle query event err %v", err)
					}
				}
			}
			savePos = posInfo.SavePos
			force = posInfo.Force
			if savePos && e.GSet != nil {
				c.master.UpdateGTIDSet(e.GSet)
			}
		default:
			continue
		}

		if savePos {
			c.master.Update(pos)
			c.master.UpdateTimestamp(ev.Header.Timestamp)

			if err := c.eventHandler.OnPosSynced(ev.Header, pos, c.master.GTIDSet(), force); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

type node struct {
	db    string
	table string
}

func parseDDLStmt(stmt ast.StmtNode) (ns []*node) {
	switch t := stmt.(type) {
	case *ast.RenameTableStmt:
		for _, tableInfo := range t.TableToTables {
			n := &node{
				db:    tableInfo.OldTable.Schema.String(),
				table: tableInfo.OldTable.Name.String(),
			}
			ns = append(ns, n)
		}
	case *ast.AlterTableStmt:
		n := &node{
			db:    t.Table.Schema.String(),
			table: t.Table.Name.String(),
		}
		ns = []*node{n}
	case *ast.DropTableStmt:
		for _, table := range t.Tables {
			n := &node{
				db:    table.Schema.String(),
				table: table.Name.String(),
			}
			ns = append(ns, n)
		}
	case *ast.CreateTableStmt:
		n := &node{
			db:    t.Table.Schema.String(),
			table: t.Table.Name.String(),
		}
		ns = []*node{n}
	case *ast.TruncateTableStmt:
		n := &node{
			db:    t.Table.Schema.String(),
			table: t.Table.Name.String(),
		}
		ns = []*node{n}
	}
	return ns
}

func (c *Canal) updateTable(header *replication.EventHeader, db, table string) (err error) {
	c.ClearTableCache([]byte(db), []byte(table))
	c.cfg.Logger.Infof("table structure changed, clear table cache: %s.%s\n", db, table)
	if err = c.eventHandler.OnTableChanged(header, db, table); err != nil && errors.Cause(err) != schema.ErrTableNotExist {
		return errors.Trace(err)
	}
	return
}

func (c *Canal) updateReplicationDelay(ev *replication.BinlogEvent) {
	var newDelay uint32
	now := uint32(time.Now().Unix())
	if now >= ev.Header.Timestamp {
		newDelay = now - ev.Header.Timestamp
	}
	atomic.StoreUint32(c.delay, newDelay)
}

func (c *Canal) handleRowsEvent(e *replication.BinlogEvent) error {
	ev := e.Event.(*replication.RowsEvent)

	// Caveat: table may be altered at runtime.
	schema := string(ev.Table.Schema)
	table := string(ev.Table.Table)

	t, err := c.GetTable(schema, table)
	if err != nil {
		return err
	}
	var action string
	switch e.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2, replication.MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1:
		action = InsertAction
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2, replication.MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1:
		action = DeleteAction
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2, replication.MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1:
		action = UpdateAction
	default:
		return errors.Errorf("%s not supported now", e.Header.EventType)
	}
	events := newRowsEvent(t, action, ev.Rows, e.Header)
	return c.eventHandler.OnRow(events)
}

func (c *Canal) FlushBinlog() error {
	_, err := c.Execute("FLUSH BINARY LOGS")
	return errors.Trace(err)
}

func (c *Canal) WaitUntilPos(pos mysql.Position, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	for {
		select {
		case <-timer.C:
			return errors.Errorf("wait position %v too long > %s", pos, timeout)
		default:
			err := c.FlushBinlog()
			if err != nil {
				return errors.Trace(err)
			}
			curPos := c.master.Position()
			if curPos.Compare(pos) >= 0 {
				return nil
			} else {
				c.cfg.Logger.Debugf("master pos is %v, wait catching %v", curPos, pos)
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

func (c *Canal) GetMasterPos() (mysql.Position, error) {
	rr, err := c.Execute("SHOW MASTER STATUS")
	if err != nil {
		return mysql.Position{}, errors.Trace(err)
	}

	name, _ := rr.GetString(0, 0)
	pos, _ := rr.GetInt(0, 1)

	return mysql.Position{Name: name, Pos: uint32(pos)}, nil
}

func (c *Canal) GetMasterGTIDSet() (mysql.GTIDSet, error) {
	query := ""
	switch c.cfg.Flavor {
	case mysql.MariaDBFlavor:
		query = "SELECT @@GLOBAL.gtid_current_pos"
	default:
		query = "SELECT @@GLOBAL.GTID_EXECUTED"
	}
	rr, err := c.Execute(query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	gx, err := rr.GetString(0, 0)
	if err != nil {
		return nil, errors.Trace(err)
	}
	gset, err := mysql.ParseGTIDSet(c.cfg.Flavor, gx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return gset, nil
}

func (c *Canal) CatchMasterPos(timeout time.Duration) error {
	pos, err := c.GetMasterPos()
	if err != nil {
		return errors.Trace(err)
	}

	return c.WaitUntilPos(pos, timeout)
}

type Position struct {
	mysql.Position
	SavePos bool
	Force   bool
}

// handleDDLEvent is handle DDL event
func (c *Canal) handleDDLEvent(ev *replication.BinlogEvent, e *replication.QueryEvent, nodes []*node, pos *Position) error {
	for _, node := range nodes {
		if node.db == "" {
			node.db = string(e.Schema)
		}
		if err := c.updateTable(ev.Header, node.db, node.table); err != nil {
			return errors.Trace(err)
		}
	}
	if len(nodes) > 0 {
		// Now we only handle Table Changed DDL, maybe we will support more later.
		if err := c.eventHandler.OnDDL(ev.Header, pos.Position, e); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// handleQueryEvent is handle some common query events (e.g., DDL,CREATE or DROP USER,GRANT)
// DDL event use handleDDLEvent, others use the handleQueryEvent
func (c *Canal) handleQueryEvent(ev *replication.BinlogEvent, e *replication.QueryEvent, stmt ast.StmtNode, pos *Position) error {
	if err := c.eventHandler.OnQueryEvent(ev, e, stmt, pos); err != nil {
		return errors.Trace(err)
	}
	return nil
}
