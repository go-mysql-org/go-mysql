package mysql

import (
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCompareServerVersions(t *testing.T) {
	tests := []struct {
		A      string
		B      string
		Expect int
	}{
		{A: "1.2.3", B: "1.2.3", Expect: 0},
		{A: "5.6-999", B: "8.0", Expect: -1},
		{A: "8.0.32-0ubuntu0.20.04.2", B: "8.0.28", Expect: 1},
	}

	for _, test := range tests {
		got, err := CompareServerVersions(test.A, test.B)
		require.NoError(t, err)
		require.Equal(t, test.Expect, got)
	}
}

func TestFormatBinaryTime(t *testing.T) {
	tests := []struct {
		Data   []byte
		Expect string
		Error  bool
	}{
		{Data: []byte{}, Expect: "00:00:00"},
		{Data: []byte{0, 0, 0, 0, 0, 0, 0, 10}, Expect: "00:00:10"},
		{Data: []byte{0, 0, 0, 0, 0, 0, 1, 40}, Expect: "00:01:40"},
		{Data: []byte{1, 0, 0, 0, 0, 0, 1, 40}, Expect: "-00:01:40"},
		{Data: []byte{1, 1, 0, 0, 0, 1, 1, 40}, Expect: "-25:01:40"},
		{Data: []byte{1, 1, 0, 0, 0, 1, 1, 40, 1, 2, 3, 0}, Expect: "-25:01:40.197121"},
		{Data: []byte{0}, Error: true},
	}

	for _, test := range tests {
		n := len(test.Data)

		got, err := FormatBinaryTime(n, test.Data)
		if test.Error {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, test.Expect, string(got), "test case %v", test.Data)
	}
}

// mysql driver parse binary datetime
func parseBinaryDateTime(num uint64, data []byte, loc *time.Location) (time.Time, error) {
	switch num {
	case 0:
		return time.Time{}, nil
	case 4:
		return time.Date(
			int(binary.LittleEndian.Uint16(data[:2])), // year
			time.Month(data[2]),                       // month
			int(data[3]),                              // day
			0, 0, 0, 0,
			loc,
		), nil
	case 7:
		return time.Date(
			int(binary.LittleEndian.Uint16(data[:2])), // year
			time.Month(data[2]),                       // month
			int(data[3]),                              // day
			int(data[4]),                              // hour
			int(data[5]),                              // minutes
			int(data[6]),                              // seconds
			0,
			loc,
		), nil
	case 11:
		return time.Date(
			int(binary.LittleEndian.Uint16(data[:2])), // year
			time.Month(data[2]),                       // month
			int(data[3]),                              // day
			int(data[4]),                              // hour
			int(data[5]),                              // minutes
			int(data[6]),                              // seconds
			int(binary.LittleEndian.Uint32(data[7:11]))*1000, // nanoseconds
			loc,
		), nil
	}
	return time.Time{}, fmt.Errorf("invalid DATETIME packet length %d", num)
}

func TestToBinaryDateTime(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Time
		expected string
	}{
		{
			name:     "Zero time",
			input:    time.Time{},
			expected: "",
		},
		{
			name:     "Date with nanoseconds",
			input:    time.Date(2023, 10, 10, 10, 10, 10, 123456000, time.UTC),
			expected: "2023-10-10 10:10:10.123456 +0000 UTC",
		},
		{
			name:     "Date with time",
			input:    time.Date(2023, 10, 10, 10, 10, 10, 0, time.UTC),
			expected: "2023-10-10 10:10:10 +0000 UTC",
		},
		{
			name:     "Date only",
			input:    time.Date(2023, 10, 10, 0, 0, 0, 0, time.UTC),
			expected: "2023-10-10 00:00:00 +0000 UTC",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := toBinaryDateTime(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(result) == 0 {
				return
			}
			num := uint64(result[0])
			data := result[1:]
			date, err := parseBinaryDateTime(num, data, time.UTC)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !strings.EqualFold(date.String(), tt.expected) {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}
