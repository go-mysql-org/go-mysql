package utils

import (
	"math"
	"unsafe"
)

// StringToByteSlice performs zero-allocation conversion from string to []byte using unsafe.Slice.
//
// For an empty string (""), unsafe.StringData returns nil, so this function returns
// a nil slice (len == 0, cap == 0, b == nil), distinct from a non-nil empty slice ([]byte{}).
// Callers that rely on nil vs non-nil empty slices (such as MySQL length-encoded string
// serializers distinguishing NULL from empty string) should handle "" accordingly.
func StringToByteSlice(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func ByteSliceToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func Uint64ToInt64(val uint64) int64 {
	return int64(val)
}

func Uint64ToFloat64(val uint64) float64 {
	return math.Float64frombits(val)
}

func Int64ToUint64(val int64) uint64 {
	return uint64(val)
}

func Float64ToUint64(val float64) uint64 {
	return math.Float64bits(val)
}

func BoolToByteSlice(b bool) []byte {
	if b {
		return StringToByteSlice("true")
	}
	return StringToByteSlice("false")
}
