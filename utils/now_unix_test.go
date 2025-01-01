//go:build unix

package utils

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCustomTimeNow(t *testing.T) {
	precision := time.Millisecond

	for i := 0; i < 1000; i++ {
		timestamp := time.Now()
		customTimestamp := Now()

		// two timestamp should within 1 percistion
		assert.WithinDuration(t, timestamp, customTimestamp, precision, fmt.Sprintf("Loop: %d: customTimestamp should within %s. timestamp: %d, customTimestamp: %d", i, precision.String(), timestamp.UnixNano(), customTimestamp.UnixNano()))

		time.Sleep(time.Nanosecond)
	}
}

func BenchmarkGoTimeNow(t *testing.B) {
	t.ResetTimer()
	for n := 0; n < t.N; n++ {
		_ = time.Now()
	}
	t.StopTimer()
}

func BenchmarkCustomTimeNow(t *testing.B) {
	t.ResetTimer()
	for n := 0; n < t.N; n++ {
		_ = Now()
	}
	t.StopTimer()
}
