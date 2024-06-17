package mysql

import (
	"testing"

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

		// binary.LittleEndian.PutUint32(data[1:], uint64(test.Seconds))
		got, err := FormatBinaryTime(n, test.Data)
		if test.Error {
			require.Error(t, err)
		} else if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		require.Equal(t, test.Expect, string(got), "test case %v", test.Data)
	}
}
