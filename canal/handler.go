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
	// changed row list
	// binlog has three update event version, v0, v1 and v2.
	// for v1 and v2, the rows number must be even.
	// Two rows for one event, format is [before update row, after update row]
	// for update v0, only one row for a event, and we don't support this version.
	Rows [][]interface{}
}

func newRowsEvent(table *schema.Table, action string, rows [][]interface{}) *RowsEvent {
	e := new(RowsEvent)

	e.Table = table
	e.Action = action
	e.Rows = rows

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
