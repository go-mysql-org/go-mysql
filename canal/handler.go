package canal

import (
	"errors"

	"github.com/siddontang/go-mysql/schema"
	"github.com/siddontang/go/log"
)

var (
	ErrHandleInterrupted = errors.New("do handler error, interrupted")
)

const (
	UpdateAction = "update"
	InsertAction = "insert"
	DeleteAction = "delete"
)

type RowsEvent struct {
	Table  *schema.Table
	Action string
	// event list
	// binlog has three update event version, v0, v1 and v2.
	// for v1 and v2, the events number must be even.
	// Two events for one row, format is [delete, insert]
	// for update v0, only one event for a row, and we don't support this version.
	Events [][]interface{}
}

func newRowsEvent(table *schema.Table, action string, events [][]interface{}) *RowsEvent {
	e := new(RowsEvent)

	e.Table = table
	e.Action = action
	e.Events = events

	return e
}

type RowsEventHandler interface {
	// Handle RowsEvent, if return ErrHandleInterrupted, canal will
	// stop the sync
	Do(e *RowsEvent) error
	String() string
}

func (c *Canal) RegRowsEventHandler(h RowsEventHandler) {
	c.rsLock.Lock()
	c.rsHandlers = append(c.rsHandlers, h)
	c.rsLock.Unlock()
}

func (c *Canal) travelRowsEventHandler(e *RowsEvent) error {
	c.rsLock.Lock()
	defer c.rsLock.Unlock()

	var err error
	for _, h := range c.rsHandlers {
		if err = h.Do(e); err != nil && err != ErrHandleInterrupted {
			log.Errorf("handle %v err: %v", h, err)
		} else if err == ErrHandleInterrupted {
			log.Errorf("handle %v err, interrupted", h)
			return err
		}

	}
	return nil
}
