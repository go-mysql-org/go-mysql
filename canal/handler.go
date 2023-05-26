package canal

import (
	"github.com/instructure/mc-go-mysql/mysql"
	"github.com/instructure/mc-go-mysql/replication"
)

type EventHandler interface {
	OnRotate(header *replication.EventHeader, rotateEvent *replication.RotateEvent) error
	// OnTableChanged is called when the table is created, altered, renamed or dropped.
	// You need to clear the associated data like cache with the table.
	// It will be called before OnDDL.
	OnTableChanged(header *replication.EventHeader, schema string, table string) error
	OnDDL(header *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error
	OnRow(e *RowsEvent) error
	OnXID(header *replication.EventHeader, nextPos mysql.Position) error
	OnGTID(header *replication.EventHeader, gtid mysql.GTIDSet) error
	// OnPosSynced Use your own way to sync position. When force is true, sync position immediately.
	OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error
	String() string
}

type DummyEventHandler struct {
}

func (h *DummyEventHandler) OnRotate(*replication.EventHeader, *replication.RotateEvent) error {
	return nil
}
func (h *DummyEventHandler) OnTableChanged(*replication.EventHeader, string, string) error {
	return nil
}
func (h *DummyEventHandler) OnDDL(*replication.EventHeader, mysql.Position, *replication.QueryEvent) error {
	return nil
}
func (h *DummyEventHandler) OnRow(*RowsEvent) error                               { return nil }
func (h *DummyEventHandler) OnXID(*replication.EventHeader, mysql.Position) error { return nil }
func (h *DummyEventHandler) OnGTID(*replication.EventHeader, mysql.GTIDSet) error { return nil }
func (h *DummyEventHandler) OnPosSynced(*replication.EventHeader, mysql.Position, mysql.GTIDSet, bool) error {
	return nil
}

func (h *DummyEventHandler) String() string { return "DummyEventHandler" }

// `SetEventHandler` registers the sync handler, you must register your
// own handler before starting Canal.
func (c *Canal) SetEventHandler(h EventHandler) {
	c.eventHandler = h
}
