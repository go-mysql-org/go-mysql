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
		Rows   [][]interface{}
		Header *replication.EventHeader
	}
	tests := []struct {
		name     string
		fields   fields
		wantRows [][]interface{}
	}{
		{
			name: "rows_event_handle_unsigned",
			fields: fields{
				Table: &schema.Table{
					// columns 1,3,5,7,9 should be converted from signed to unsigned,
					// column 10 is out of range and should be ignored, don't panic.
					UnsignedColumns: []int{1, 3, 5, 7, 9, 10},
				},
				Rows: [][]interface{}{{
					int8(8), int8(8),
					int16(16), int16(16),
					int32(32), int32(32),
					int64(64), int64(64),
					int(128), int(128)},
				},
			},
			wantRows: [][]interface{}{{
				int8(8), uint8(8),
				int16(16), uint16(16),
				int32(32), uint32(32),
				int64(64), uint64(64),
				int(128), uint(128)},
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
