package canal

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/stretchr/testify/require"
)

func TestRowsEvent_handleUnsigned(t *testing.T) {
	type fields struct {
		Table  *schema.Table
		Action string
		Rows   [][]any
		Header *replication.EventHeader
	}
	tests := []struct {
		name     string
		fields   fields
		wantRows [][]any
	}{
		{
			name: "rows_event_handle_unsigned",
			fields: fields{
				Table: &schema.Table{
					// columns 1,3,5,7,9 should be converted from signed to unsigned,
					// column 10 is out of range and should be ignored, don't panic.
					UnsignedColumns: []int{1, 3, 5, 7, 9, 10},
				},
				Rows: [][]any{
					{
						int8(8), int8(8),
						int16(16), int16(16),
						int32(32), int32(32),
						int64(64), int64(64),
						int(128), int(128),
					},
				},
			},
			wantRows: [][]any{
				{
					int8(8), uint8(8),
					int16(16), uint16(16),
					int32(32), uint32(32),
					int64(64), uint64(64),
					int(128), uint(128),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RowsEvent{
				Table:  tt.fields.Table,
				Action: tt.fields.Action,
				Rows:   tt.fields.Rows,
				Header: tt.fields.Header,
			}
			r.handleUnsigned()
			require.Equal(t, tt.fields.Rows, tt.wantRows)
		})
	}
}

// TestRowsEvent_flags checks if Flags from replication's RowsEvent propagate to canal's RowsEvent.
func TestRowsEvent_flags(t *testing.T) {
	replEvent := &replication.RowsEvent{
		Flags: 0x15,
	}

	var table schema.Table
	canalEvent := newRowsEvent(&table, "update", nil, nil, replEvent)

	require.Equal(t, canalEvent.Flags, replEvent.Flags)
}

// TestRowsEvent_skippedColumns checks if SkippedColumns from replication's RowsEvent
// propagate to canal's RowsEvent, aligned with Rows.
func TestRowsEvent_skippedColumns(t *testing.T) {
	var table schema.Table

	t.Run("partial row image", func(t *testing.T) {
		replEvent := &replication.RowsEvent{
			// update with binlog_row_image=MINIMAL: before image has the key only,
			// after image has the changed column only.
			SkippedColumns: [][]int{{1, 2}, {0, 2}},
		}
		rows := [][]any{{1, nil, nil}, {nil, "a2", nil}}

		canalEvent := newRowsEvent(&table, UpdateAction, rows, nil, replEvent)

		require.Equal(t, replEvent.SkippedColumns, canalEvent.SkippedColumns)
		require.Len(t, canalEvent.SkippedColumns, len(canalEvent.Rows))
	})

	t.Run("full row image", func(t *testing.T) {
		replEvent := &replication.RowsEvent{
			SkippedColumns: [][]int{{}},
		}

		canalEvent := newRowsEvent(&table, InsertAction, [][]any{{1, "a", nil}}, nil, replEvent)

		require.Equal(t, [][]int{{}}, canalEvent.SkippedColumns)
	})

	t.Run("no replication event", func(t *testing.T) {
		canalEvent := newRowsEvent(&table, InsertAction, [][]any{{1, "a", nil}}, nil, nil)

		require.Nil(t, canalEvent.SkippedColumns)
	})
}
