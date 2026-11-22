package replication

import (
	"io"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-mysql-org/go-mysql/mysql"
)

func TestRowsEventPanicErrorOmitsData(t *testing.T) {
	table := &TableMapEvent{
		tableIDSize: 6,
		TableID:     108,
		Schema:      []byte("db"),
		Table:       []byte("tbl"),
		ColumnCount: 1,
		ColumnType:  []byte{mysql.MYSQL_TYPE_VARCHAR},
		ColumnMeta:  []uint16{255},
		NullBitmap:  []byte{0x01},
	}

	// WRITE_ROWS_EVENTv2 body with one VARCHAR column. The first row is
	// complete. The second row ends after its null bitmap, so decoding its
	// value panics.
	data := []byte{
		0x6c, 0x00, 0x00, 0x00, 0x00, 0x00, // table id 108
		0x01, 0x00, // flags: STMT_END_F
		0x02, 0x00, // extra data length
		0x01,       // column count
		0x01,       // columns present
		0x00, 0x10, // row 1: null bitmap, value length 16
	}
	data = append(data, "row-value-canary"...)
	data = append(data, 0x00) // row 2: null bitmap only

	e := &RowsEvent{
		Version:     2,
		tableIDSize: 6,
		tables:      map[uint64]*TableMapEvent{108: table},
		eventType:   WRITE_ROWS_EVENTv2,
	}
	err := e.Decode(data)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "row-value-canary")

	var rtErr runtime.Error
	require.ErrorAs(t, err, &rtErr)
	require.EqualError(t, err, `parse rows event panic (WriteRowsEventV2, table "db"."tbl", table id 108, `+
		"column count 1, row images decoded 1, row image offset 30, data length 31): "+rtErr.Error())

	// The rows decoded before the panic are still in the event.
	require.Equal(t, [][]any{{"row-value-canary"}}, e.Rows)
}

func TestRowsEventPanicErrorWithoutTableMap(t *testing.T) {
	// DecodeData before DecodeHeader has set Table.
	e := &RowsEvent{ColumnCount: 1, ColumnBitmap1: []byte{0x01}}
	err := e.DecodeData(0, []byte{0x00, 0x00})

	var rtErr runtime.Error
	require.ErrorAs(t, err, &rtErr)
	require.EqualError(t, err, "parse rows event panic (UnknownEvent, table <nil>, table id 0, "+
		"column count 1, row images decoded 0, row image offset 0, data length 2): "+rtErr.Error())
}

func TestRowsEventPanicErrorFormat(t *testing.T) {
	e := &RowsEvent{
		eventType:   UPDATE_ROWS_EVENTv2,
		TableID:     7,
		ColumnCount: 1,
		Table:       &TableMapEvent{Schema: []byte("db"), Table: []byte("t\nbl")},
		Rows:        [][]any{{"before"}, {"after"}},
	}
	const prefix = `parse rows event panic (UpdateRowsEventV2, table "db"."t\nbl", table id 7, ` +
		"column count 1, row images decoded 2, row image offset 40, data length 50): "

	// A panic value that is an error stays in the error chain.
	err := e.decodePanicError(io.ErrUnexpectedEOF, 40, 50)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.EqualError(t, err, prefix+"unexpected EOF")

	// Any other panic value is formatted with %v.
	err = e.decodePanicError("boom", 40, 50)
	require.EqualError(t, err, prefix+"boom")
}

func TestJSONDiffErrorOmitsPath(t *testing.T) {
	// A Replace diff at path $.canary_key whose value has an invalid JSON type.
	data := []byte{0x00, 0x0c} // operation Replace, path length 12
	data = append(data, "$.canary_key"...)
	data = append(data, 0x02, 0x63, 0x00) // value length 2: type 0x63, one byte

	e := &RowsEvent{}
	_, err := e.decodeJSONPartialBinary(data)
	require.Error(t, err)

	msg := err.Error()
	require.NotContains(t, msg, "canary_key")
	require.Equal(t, "cannot read json diff value (op Replace, 2 bytes): invalid json type 99", msg)
}
