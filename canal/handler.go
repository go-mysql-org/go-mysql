package canal

import "github.com/juju/errors"

type RowsEventHandler interface {
	// Handle RowsEvent, if return error, canal will
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
		if err = h.Do(e); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
