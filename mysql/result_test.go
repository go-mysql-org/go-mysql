package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHasResultset_false(t *testing.T) {
	r := NewResultReserveResultset(0)
	b := r.HasResultset()
	require.False(t, b)
}

func TestHasResultset_true(t *testing.T) {
	r := NewResultReserveResultset(1)
	b := r.HasResultset()
	require.True(t, b)
}

// this shouldn't happen after d02e79a, but test just in case
func TestHasResultset_nilset(t *testing.T) {
	r := NewResultReserveResultset(0)
	r.Resultset = nil
	b := r.HasResultset()
	require.False(t, b)
}

func TestHasResultset_nil(t *testing.T) {
	var r *Result
	b := r.HasResultset()
	require.False(t, b)
}
