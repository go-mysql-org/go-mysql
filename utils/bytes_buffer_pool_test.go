package utils

import "testing"

func BenchmarkBytesBufferPool(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b := BytesBufferGet()
		b.WriteString("01")
		BytesBufferPut(b)
	}
}
