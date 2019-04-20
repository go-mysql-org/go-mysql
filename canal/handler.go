package canal

import (
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

type EventHandler interface {
	OnRotate(roateEvent *replication.RotateEvent) error
	// OnTableChanged is called when the table is created, altered, renamed or dropped.
	// You need to clear the associated data like cache with the table.
	// It will be called before OnDDL.
	OnTableChanged(schema string, table string) error
	OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error
	OnRow(e *RowsEvent) error
	OnXID(nextPos mysql.Position) error
	OnGTID(gtid mysql.GTIDSet) error
	// OnPosSynced Use your own way to sync position. When force is true, sync position immediately.
	OnPosSynced(pos mysql.Position, force bool) error
	// OnPosSynced Sync position and GTID if desired
	OnPosSyncedWithGTID(mysql.Position, mysql.GTIDSet, bool) error
	String() string
}

type DummyEventHandler struct {
}

func (h *DummyEventHandler) OnRotate(*replication.RotateEvent) error          { return nil }
func (h *DummyEventHandler) OnTableChanged(schema string, table string) error { return nil }
func (h *DummyEventHandler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	return nil
}
func (h *DummyEventHandler) OnRow(*RowsEvent) error                 { return nil }
func (h *DummyEventHandler) OnXID(mysql.Position) error             { return nil }
func (h *DummyEventHandler) OnGTID(mysql.GTIDSet) error             { return nil }
func (h *DummyEventHandler) OnPosSynced(mysql.Position, bool) error { return nil }

/* Add support for GTID at the end of a transaction and keep backward compatibility */
func (h *DummyEventHandler) OnPosSyncedWithGTID(position mysql.Position, set mysql.GTIDSet, force bool) error {
	return h.OnPosSynced(position, force)
}
func (h *DummyEventHandler) String() string                         { return "DummyEventHandler" }

// `SetEventHandler` registers the sync handler, you must register your
// own handler before starting Canal.
func (c *Canal) SetEventHandler(h EventHandler) {
	c.eventHandler = h
}
