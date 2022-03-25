package utils

import "testing"

func BenchmarkByteSlicePool(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b := ByteSliceGet(16)
		b.B = append(b.B[:0], 0, 1)
		ByteSlicePut(b)
	}
}
