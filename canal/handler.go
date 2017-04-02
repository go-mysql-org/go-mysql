package canal

import (
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"golang.org/x/net/context"
)

type EventHandler interface {
	OnRotate(ctx context.Context, roateEvent *replication.RotateEvent) error
	OnDDL(ctx context.Context, nextPos mysql.Position, queryEvent *replication.QueryEvent) error
	OnRow(ctx context.Context, e *RowsEvent) error
	OnXID(ctx context.Context, nextPos mysql.Position) error
	String() string
}

type DummyEventHandler struct {
}

func (h *DummyEventHandler) OnRotate(context.Context, *replication.RotateEvent) error { return nil }
func (h *DummyEventHandler) OnDDL(context.Context, mysql.Position, *replication.QueryEvent) error {
	return nil
}
func (h *DummyEventHandler) OnRow(context.Context, *RowsEvent) error     { return nil }
func (h *DummyEventHandler) OnXID(context.Context, mysql.Position) error { return nil }
func (h *DummyEventHandler) String() string                              { return "DummyEventHandler" }

// `SetEventHandler` registers the sync handler, you must register your
// own handler before starting Canal.
func (c *Canal) SetEventHandler(h EventHandler) {
	c.eventHandler = h
}
