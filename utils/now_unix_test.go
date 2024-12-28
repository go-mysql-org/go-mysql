//go:build unix

package utils

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCustomTimeNow(t *testing.T) {
	precision = time.Millisecond

	for i := 0; i < 1000; i++ {
		timestamp := time.Now().UnixNano()
		customTimestamp := Now().UnixNano()

		// two timestamp should within 1 percistion
		assert.Equal(t, timestamp <= customTimestamp, true, fmt.Sprintf("Loop %d: timestamp <= customTimestamp should be true. timestamp: %d, customTimestamp: %d", i, timestamp, customTimestamp))
		assert.Equal(t, timestamp+int64(precision) >= customTimestamp, true, fmt.Sprintf("Loop: %d: customTimestamp should within %s. timestamp: %d, customTimestamp: %d", i, precision.String(), timestamp, customTimestamp))

		os.Setenv("TZ", fmt.Sprintf("UTC%d", 14-i%27))
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
