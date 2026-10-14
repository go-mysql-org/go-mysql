package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColumnNumber(t *testing.T) {
	r := NewResultReserveResultset(0)
	// Make sure ColumnNumber doesn't panic when constructing a Result with 0
	// columns. https://github.com/go-mysql-org/go-mysql/issues/964
	r.ColumnNumber()
}

// TestGetInt tests GetInt with a negative value
func TestGetIntNeg(t *testing.T) {
	r := NewResultset(1)
	fv := NewFieldValue(FieldValueTypeString, 0, []uint8("-193"))
	r.Values = [][]FieldValue{{fv}}
	v, err := r.GetInt(0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(-193), v)
}

func TestBuildSimpleTextResultsetDistinguishesEmptyValuesFromNull(t *testing.T) {
	r, err := BuildSimpleTextResultset(
		[]string{"empty_string", "empty_bytes", "null", "bool"},
		[][]any{{"", []byte{}, nil, false}},
	)
	require.NoError(t, err)
	require.Equal(t, RowData{0x00, 0x00, 0xfb, 0x5, 0x66, 0x61, 0x6c, 0x73, 0x65}, r.RowDatas[0])
	require.Equal(t, MYSQL_TYPE_VAR_STRING, r.Fields[0].Type)
	require.Equal(t, MYSQL_TYPE_VAR_STRING, r.Fields[1].Type)
	require.Equal(t, MYSQL_TYPE_NULL, r.Fields[2].Type)
	require.Equal(t, MYSQL_TYPE_TINY, r.Fields[3].Type)
}

// A Resultset taken from the pool still has the previous result's *Field
// pointers in the backing array of Fields. BuildSimpleTextResultset must not
// treat them as fields of the new result.
// https://github.com/go-mysql-org/go-mysql/issues/1197
func TestBuildSimpleTextResultsetIgnoresPooledFields(t *testing.T) {
	for range 100 {
		stale := NewResultset(1)
		stale.Fields[0] = &Field{Name: []byte("stale"), Type: MYSQL_TYPE_LONGLONG}
		(&Result{Resultset: stale}).Close()

		r, err := BuildSimpleTextResultset([]string{"fresh"}, [][]any{{"x"}})
		require.NoError(t, err)
		require.Equal(t, "fresh", string(r.Fields[0].Name))
		require.Equal(t, MYSQL_TYPE_VAR_STRING, r.Fields[0].Type)
		(&Result{Resultset: r}).Close()
	}
}
