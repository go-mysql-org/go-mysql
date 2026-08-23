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
		[]string{"empty_string", "empty_bytes", "null"},
		[][]any{{"", []byte{}, nil}},
	)
	require.NoError(t, err)
	require.Equal(t, RowData{0x00, 0x00, 0xfb}, r.RowDatas[0])
	require.Equal(t, MYSQL_TYPE_VAR_STRING, r.Fields[0].Type)
	require.Equal(t, MYSQL_TYPE_VAR_STRING, r.Fields[1].Type)
	require.Equal(t, MYSQL_TYPE_NULL, r.Fields[2].Type)
}
