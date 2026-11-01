package utils

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestStringToByteSlice(t *testing.T) {
	// Empty string returns nil slice (len == 0, cap == 0, b == nil).
	empty := ""
	bEmpty := StringToByteSlice(empty)
	require.Nil(t, bEmpty)
	require.Empty(t, bEmpty)
	require.Equal(t, 0, len(bEmpty))
	require.Equal(t, 0, cap(bEmpty))

	// In contrast, []byte("") or []byte{} produces a non-nil empty slice.
	literalEmpty := []byte{}
	require.NotNil(t, literalEmpty)
	require.NotEqual(t, bEmpty == nil, literalEmpty == nil)

	// Non-empty string: content matches, zero-copy pointer equivalence, round-trip.
	s := "hello, world"
	b := StringToByteSlice(s)
	require.NotNil(t, b)
	require.Equal(t, []byte(s), b)
	require.Equal(t, len(s), len(b))
	require.Equal(t, unsafe.StringData(s), unsafe.SliceData(b))

	// Round-trip with ByteSliceToString
	roundTrip := ByteSliceToString(b)
	require.Equal(t, s, roundTrip)
	require.Equal(t, unsafe.StringData(s), unsafe.StringData(roundTrip))
}

func TestByteSliceToString(t *testing.T) {
	// Empty slice
	var nilBytes []byte
	require.Equal(t, "", ByteSliceToString(nilBytes))

	emptyBytes := []byte{}
	require.Equal(t, "", ByteSliceToString(emptyBytes))

	// Non-empty slice
	data := []byte("testing zero alloc")
	str := ByteSliceToString(data)
	require.Equal(t, string(data), str)
	require.Equal(t, unsafe.SliceData(data), unsafe.StringData(str))
}
