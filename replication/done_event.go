package replication

import (
	"fmt"
	"io"
)

// FakeDoneEvent done event is a fake event to indicate the file parse is done
// because some binlog has no rotate_event or stop_event
type FakeDoneEvent struct {
	Data []byte
	Done bool
}

func (e *FakeDoneEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Event data: \n%v", e.Done)
	fmt.Fprintln(w)
}

func (e *FakeDoneEvent) Decode(data []byte) error {
	e.Data = data
	return nil
}
