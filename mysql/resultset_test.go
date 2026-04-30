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

// Direct check: Reset clears Fields when re-slicing within capacity.
func TestResetClearsStaleFields(t *testing.T) {
	r := &Resultset{
		Fields: []*Field{{Name: []byte("stale")}},
	}
	r.Reset(1)
	require.Nil(t, r.Fields[0])
}

// End-to-end check: a Resultset returned to the pool must hand the next
// builder fresh nil Fields, not stale pointers from the previous caller.
func TestBuildSimpleTextResultsetReusesPoolWithFreshFields(t *testing.T) {
	// First call: populates Fields[0].Name = "old".
	r1, err := BuildSimpleTextResultset([]string{"old"}, [][]any{{int64(1)}})
	require.NoError(t, err)
	require.Equal(t, []byte("old"), r1.Fields[0].Name)

	// Hand r1 back to the pool, like Result.Close does.
	resultsetPool.Put(r1)

	// Second call: NewResultset pulls r1 back. Without the fix, Fields[0]
	// would still be the "old" Field and Name would silently stay "old".
	r2, err := BuildSimpleTextResultset([]string{"new"}, [][]any{{int64(2)}})
	require.NoError(t, err)
	require.Equal(t, []byte("new"), r2.Fields[0].Name)
}
