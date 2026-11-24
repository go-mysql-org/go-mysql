package mysql

import (
	"bytes"
	"compress/zlib"
	"io"
	"runtime"
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
		{A: "5.6.3-999", B: "5.6", Expect: 0},
		{A: "5.6.3-999", B: "5.5-tag", Expect: 1},
		{A: "8.0.32-0ubuntu0.20.04.2", B: "8.0.28", Expect: 1},
		{A: "a.b.c", B: "8.0", Expect: 2},
	}

	for _, test := range tests {
		got, err := CompareServerVersions(test.A, test.B)
		if test.Expect == 2 {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, test.Expect, got)
		}

		// test logic is commutative
		got, err = CompareServerVersions(test.B, test.A)
		if test.Expect == 2 {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, -test.Expect, got)
		}
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

func TestToBinaryDateTime(t *testing.T) {
	var (
		DateTimeNano         = "2006-01-02 15:04:05.000000"
		formatBinaryDateTime = func(n int, data []byte) string {
			date, err := FormatBinaryDateTime(n, data)
			if err != nil {
				return ""
			}
			return string(date)
		}
	)

	tests := []struct {
		Name   string
		Data   time.Time
		Expect func(n int, data []byte) string
		Error  bool
	}{
		{
			Name:   "Zero time",
			Data:   time.Time{},
			Expect: nil,
		},
		{
			Name:   "Date with nanoseconds",
			Data:   time.Date(2023, 10, 10, 10, 10, 10, 123456000, time.UTC),
			Expect: formatBinaryDateTime,
		},
		{
			Name:   "Date with time",
			Data:   time.Date(2023, 10, 10, 10, 10, 10, 0, time.UTC),
			Expect: formatBinaryDateTime,
		},
		{
			Name:   "Date only",
			Data:   time.Date(2023, 10, 10, 0, 0, 0, 0, time.UTC),
			Expect: formatBinaryDateTime,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			got, err := toBinaryDateTime(test.Data)
			if test.Error {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			// A zero time.Time encodes as a single zero length byte (the
			// MySQL "0000-00-00 00:00:00" sentinel).
			if test.Data.IsZero() {
				require.Equal(t, []byte{0}, got)
				return
			}
			tmp := test.Expect(int(got[0]), got[1:])
			if int(got[0]) < 11 {
				require.Equal(t, tmp, test.Data.Format(time.DateTime), "test case %v", test.Data.String())
			} else {
				require.Equal(t, tmp, test.Data.Format(DateTimeNano), "test case %v", test.Data.String())
			}
		})
	}
}

// mariadbCompressed returns payload as MariaDB writes the body of a compressed
// event: a header byte with the number of length bytes, the declared length
// in that many big-endian bytes and the zlib stream.
func mariadbCompressed(t *testing.T, lenSize int, declared uint64, payload []byte) []byte {
	data := []byte{0x80 | byte(lenSize)}
	for i := lenSize - 1; i >= 0; i-- {
		data = append(data, byte(declared>>(8*i)))
	}
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	_, err := w.Write(payload)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return append(data, b.Bytes()...)
}

func TestDecompressMariadbData(t *testing.T) {
	tests := []struct {
		lenSize int
		size    int
	}{
		{1, 0},
		{1, 5},
		{2, 300},
		{3, 70000},
		{4, 5},       // MariaDB uses the fewest length bytes, but any of 1 to 4 is valid
		{3, 3 << 20}, // more than DecompressMariadbData allocates up front
	}
	for _, tc := range tests {
		payload := bytes.Repeat([]byte("abcdefgh"), tc.size/8+1)[:tc.size]
		got, err := DecompressMariadbData(mariadbCompressed(t, tc.lenSize, uint64(tc.size), payload))
		require.NoError(t, err)
		require.Equal(t, payload, got)
		// The row decoders rely on slicing past the data failing.
		require.Equal(t, len(got), cap(got))
	}
}

func TestDecompressMariadbDataMalformed(t *testing.T) {
	valid := mariadbCompressed(t, 1, 5, []byte("abcde"))
	badChecksum := bytes.Clone(valid)
	badChecksum[len(badChecksum)-1] ^= 0xff
	tests := []struct {
		name string
		data []byte
		err  string
	}{
		{"empty", nil, "compressed event data is empty"},
		{"no length bytes", append([]byte{0x80}, valid[2:]...), "compressed event data header 0x80 has 0 length bytes, want 1 to 4"},
		{"five length bytes", mariadbCompressed(t, 5, 5, []byte("abcde")), "compressed event data header 0x85 has 5 length bytes, want 1 to 4"},
		{"length cut short", []byte{0x84, 0x00, 0x00}, "compressed event data is 3 bytes, too short for its 5-byte header"},
		{"no zlib stream", valid[:2], "unexpected EOF"},
		{"not zlib", []byte{0x81, 0x05, 'a', 'b', 'c', 'd', 'e'}, "zlib: invalid header"},
		{"zlib stream cut short", valid[:len(valid)-6], "unexpected EOF"},
		{"less data than declared", mariadbCompressed(t, 1, 6, []byte("abcde")), "unexpected EOF"},
		{"bad checksum", badChecksum, "zlib: invalid checksum"},
		{"no checksum", valid[:len(valid)-4], "unexpected EOF"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecompressMariadbData(tc.data)
			require.EqualError(t, err, tc.err)
		})
	}
}

func TestDecompressMariadbDataDeclaredLength(t *testing.T) {
	// The header declares 4 GiB, the stream holds five bytes. The declared
	// length must not be allocated before the data is decompressed.
	// TotalAlloc counts the whole process, so this test must not run in
	// parallel with others.
	data := mariadbCompressed(t, 4, 0xffffffff, []byte("abcde"))

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := DecompressMariadbData(data)
	runtime.ReadMemStats(&after)

	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(16<<20))
}
