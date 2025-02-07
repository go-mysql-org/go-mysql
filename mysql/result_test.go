package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHasResultset_false(t *testing.T) {
	r := NewResultReserveResultset(0)
	b := r.HasResultset()
	require.Equal(t, false, b)
}

func TestHasResultset_true(t *testing.T) {
	r := NewResultReserveResultset(1)
	b := r.HasResultset()
	require.Equal(t, true, b)
}
