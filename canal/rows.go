package canal

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
)

// The action name for sync.
const (
	UpdateAction = "update"
	InsertAction = "insert"
	DeleteAction = "delete"
)

// RowsEvent is the event for row replication.
type RowsEvent struct {
	Table  *schema.Table
	Action string
	// changed row list
	// binlog has three update event version, v0, v1 and v2.
	// for v1 and v2, the rows number must be even.
	// Two rows for one event, format is [before update row, after update row]
	// for update v0, only one row for a event, and we don't support this version.
	Rows [][]interface{}
	// Header can be used to inspect the event
	Header *replication.EventHeader
}

func newRowsEvent(table *schema.Table, action string, rows [][]interface{}, header *replication.EventHeader) *RowsEvent {
	e := new(RowsEvent)

	e.Table = table
	e.Action = action
	e.Rows = rows
	e.Header = header

	e.handleUnsigned()

	return e
}

const maxMediumintUnsigned int32 = 16777215

func (r *RowsEvent) handleUnsigned() {
	// Handle Unsigned Columns here, for binlog replication, we can't know the integer is unsigned or not,
	// so we use int type but this may cause overflow outside sometimes, so we must convert to the really .
	// unsigned type
	if len(r.Table.UnsignedColumns) == 0 {
		return
	}

	for i := 0; i < len(r.Rows); i++ {
		for _, columnIdx := range r.Table.UnsignedColumns {
			// ignore when current column index gets out of old data size.
			// when new fields added to the table but the data is in old schema.
			if columnIdx >= len(r.Rows[i]) {
				continue
			}

			// Best practice: new columns should be added to the the end of table existing columns
			//        like :  table(id,name,addr) => table(id,name,addr,age) , here age is added the the end
			// Bad practice: table(id,name,addr) => table(id,name,age,addr)  , here age is added before the addr
			// The result of bad practice will cause the following logic be problematic

			switch value := r.Rows[i][columnIdx].(type) {
			case int8:
				r.Rows[i][columnIdx] = uint8(value)
			case int16:
				r.Rows[i][columnIdx] = uint16(value)
			case int32:
				// problem with mediumint is that it's a 3-byte type. There is no compatible golang type to match that.
				// So to convert from negative to positive we'd need to convert the value manually
				if value < 0 && r.Table.Columns[columnIdx].Type == schema.TYPE_MEDIUM_INT {
					r.Rows[i][columnIdx] = uint32(maxMediumintUnsigned + value + 1)
				} else {
					r.Rows[i][columnIdx] = uint32(value)
				}
			case int64:
				r.Rows[i][columnIdx] = uint64(value)
			case int:
				r.Rows[i][columnIdx] = uint(value)
			default:
				// nothing to do
			}
		}
	}
}

// String implements fmt.Stringer interface.
func (r *RowsEvent) String() string {
	return fmt.Sprintf("%s %s %v", r.Action, r.Table, r.Rows)
}
