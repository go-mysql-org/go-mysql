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
