package compress

import (
	"bytes"
	"strings"
	"testing"
)

// TestGetPooledZlibReaderResetError verifies that when a zlib reader is taken
// from the pool and Reset fails (e.g. the source does not start with a valid
// zlib header), GetPooledZlibReader returns that error instead of swallowing it
// and returning a nil reader with a nil error.
func TestGetPooledZlibReaderResetError(t *testing.T) {
	// Produce a valid zlib stream so we can create, then pool, a reader.
	var compressed bytes.Buffer
	w, err := GetPooledZlibWriter(&compressed)
	if err != nil {
		t.Fatalf("GetPooledZlibWriter: %v", err)
	}
	if _, err := w.Write([]byte("hello world")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	// Borrow and return a reader so the pool is primed in this goroutine; the
	// next Get is then expected to hand back the pooled (Resetter) reader.
	r, err := GetPooledZlibReader(bytes.NewReader(compressed.Bytes()))
	if err != nil {
		t.Fatalf("GetPooledZlibReader (valid): %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}

	// Now request a reader over a source whose bytes are not a valid zlib
	// header. Reset must fail; the error must be propagated to the caller.
	bad, err := GetPooledZlibReader(strings.NewReader("not a zlib stream"))
	if err == nil {
		if bad != nil {
			_ = bad.Close()
		}
		t.Fatal("expected error from GetPooledZlibReader on invalid zlib header, got nil")
	}
	if bad != nil {
		t.Fatalf("expected nil reader on error, got %v", bad)
	}
}
