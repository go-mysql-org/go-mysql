package serialization

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTrailingOneBitCount(t *testing.T) {
	testcases := []struct {
		input  byte
		result int
	}{
		{0b00000000, 0},
		{0b00000001, 1},
		{0b00000011, 2},
		{0b00000111, 3},
		{0b00001111, 4},
		{0b00011111, 5},
		{0b00111111, 6},
		{0b01111111, 7},
		{0b11111111, 8},
		{0b10000000, 0},
		{0b11111101, 1},
	}

	for _, tc := range testcases {
		actual := trailingOneBitCount(tc.input)
		require.Equal(t, tc.result, actual)
	}
}

func TestDecodeFixed(t *testing.T) {
	testcases := []struct {
		input  []byte
		len    int
		result []byte
		err    string
	}{
		{
			[]byte{
				0xee, 0x81, 0x02, 0xc1, 0x02, 0x01, 0x03, 0x41, 0x03, 0x81, 0x03, 0xc1, 0x03, 0xc5, 0x03, 0x22,
				0x22, 0xee, 0xfd, 0x03, 0xee, 0xfd, 0x03, 0xee, 0xfd, 0x03,
			},
			16,
			[]byte{0x77, 0xa0, 0xb0, 0xc0, 0xd0, 0xe0, 0xf0, 0xf1, 0x11, 0x11, 0x77, 0xff, 0x77, 0xff, 0x77, 0xff},
			"",
		},
		{
			[]byte{0xee, 0x81},
			16,
			[]byte{},
			"data truncated",
		},
		{
			[]byte{},
			16,
			[]byte{},
			"data truncated",
		},
		{
			[]byte{
				0xee, 0x81, 0x04, 0xc1, 0x02, 0x01, 0x03, 0x41, 0x03, 0x81, 0x03, 0xc1, 0x03, 0xc5, 0x03, 0x22,
				0x22, 0xee, 0xfd, 0x03, 0xee, 0xfd, 0x03, 0xee, 0xfd, 0x03,
			},
			16,
			[]byte{},
			"unknown decoding for",
		},
	}

	for _, tc := range testcases {
		f := FieldIntFixed{
			Length: tc.len,
		}
		_, err := f.decode(tc.input, 0)
		if tc.err == "" {
			require.NoError(t, err)
			require.Equal(t, tc.result, f.Value)
			require.Equal(t, tc.len, len(f.Value))
		} else {
			require.ErrorContains(t, err, tc.err)
		}
	}
}

func TestDecodeString(t *testing.T) {
	testcases := []struct {
		input  []byte
		result string
		err    string
	}{
		{
			[]byte{0x18, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c},
			"abcdefghijkl",
			"",
		},
		{
			[]byte{0x18, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67},
			"",
			"string truncated",
		},
		{
			[]byte{},
			"",
			"string truncated, expected at least one byte",
		},
		{
			[]byte{0x18},
			"",
			"string truncated, expected length",
		},
	}

	for _, tc := range testcases {
		f := FieldString{}
		_, err := f.decode(tc.input, 0)
		if tc.err == "" {
			require.NoError(t, err)
			require.Equal(t, tc.result, f.Value)
		} else {
			require.ErrorContains(t, err, tc.err)
		}
	}
}

func TestDecodeVar(t *testing.T) {
	testcases := []struct {
		input    []byte
		unsigned bool
		result   interface{}
		err      string
	}{
		{
			[]byte{},
			false,
			0,
			"data truncated",
		},
		{
			[]byte{0xd9},
			false,
			0,
			"truncated data",
		},
		{
			[]byte{0x4},
			false,
			int64(1),
			"",
		},
		{
			[]byte{0xd9, 0x03},
			false,
			int64(123),
			"",
		},
		{
			[]byte{0xc3, 0o2, 0x0b},
			true,
			uint64(90200),
			"",
		},
		{
			// From the example on https://dev.mysql.com/doc/dev/mysql-server/latest/PageLibsMysqlhtml
			// But converted to LE
			// unsigned integer, 65535
			[]byte{0b11111011, 0b11111111, 0b00000111},
			true,
			uint64(65535),
			"",
		},
		{
			// From the example on https://dev.mysql.com/doc/dev/mysql-server/latest/PageLibsMysqlhtml
			// But converted to LE
			// signed integer, 65535
			[]byte{0b11110011, 0b11111111, 0b00001111},
			false,
			int64(65535),
			"",
		},
		{
			// From the example on https://dev.mysql.com/doc/dev/mysql-server/latest/PageLibsMysqlhtml
			// But converted to LE
			// signed integer, -65535
			[]byte{0b11101011, 0b11111111, 0b00001111},
			false,
			int64(-65535),
			"",
		},
		{
			// From the example on https://dev.mysql.com/doc/dev/mysql-server/latest/PageLibsMysqlhtml
			// But converted to LE
			// signed integer, 65536
			[]byte{0b11111011, 0b11111111, 0b00001111},
			false,
			int64(-65536),
			"",
		},
		{
			[]byte{0x5d, 0x03},
			true,
			uint64(215),
			"",
		},
		{
			[]byte{0x7f, 0x39, 0x7d, 0x89, 0x70, 0xdb, 0x2d, 0x06},
			true,
			uint64(1739270369410361),
			"",
		},
	}

	for _, tc := range testcases {
		r, _, err := decodeVar(tc.input, 0, tc.unsigned)
		if tc.err == "" {
			require.NoError(t, err)
			require.Equal(t, tc.result, r, tc.result)
		} else {
			require.ErrorContains(t, err, tc.err)
		}
	}
}

func TestUmarshal_event1(t *testing.T) {
	data := []byte{
		0x2, 0x76, 0x0, 0x0, 0x2, 0x2, 0x25, 0x2, 0xdc, 0xf0, 0x9, 0x2, 0x30, 0xf9, 0x3, 0x22, 0xbd, 0x3,
		0xad, 0x2, 0x21, 0x2, 0x44, 0x44, 0x5a, 0x68, 0x51, 0x3, 0x22, 0x4, 0x4, 0x6, 0xc, 0x66, 0x6f, 0x6f, 0x62,
		0x61, 0x7a, 0x8, 0x0, 0xa, 0x4, 0xc, 0x7f, 0x15, 0x83, 0x22, 0x2d, 0x5c, 0x2e, 0x6, 0x10, 0x49, 0x3, 0x12,
		0xc3, 0x2, 0xb,
	}

	msg := Message{
		Format: Format{
			Fields: []Field{
				{
					Name: "gtid_flags",
					Type: &FieldIntFixed{
						Length: 1,
					},
				},
				{
					Name: "uuid",
					Type: &FieldIntFixed{
						Length: 16,
					},
				},
				{
					Name: "gno",
					Type: &FieldIntVar{},
				},
				{
					Name: "tag",
					Type: &FieldString{},
				},
				{
					Name: "last_committed",
					Type: &FieldIntVar{},
				},
				{
					Name: "sequence_number",
					Type: &FieldIntVar{},
				},
				{
					Name: "immediate_commit_timestamp",
					Type: &FieldUintVar{},
				},
				{
					Name:     "original_commit_timestamp",
					Type:     &FieldUintVar{},
					Optional: true,
				},
				{
					Name: "transaction_length",
					Type: &FieldUintVar{},
				},
				{
					Name: "immediate_server_version",
					Type: &FieldUintVar{},
				},
				{
					Name:     "original_server_version",
					Type:     &FieldUintVar{},
					Optional: true,
				},
				{
					Name:     "commit_group_ticket",
					Optional: true,
				},
			},
		},
	}

	expected := Message{
		Version: 1,
		Format: Format{
			Size: 59,
			Fields: []Field{
				{
					Name: "gtid_flags",
					ID:   0,
					Type: &FieldIntFixed{
						Length: 1,
						Value:  []uint8{0o1},
					},
				},
				{
					Name: "uuid",
					ID:   1,
					Type: &FieldIntFixed{
						Length: 16,
						Value: []uint8{
							0x89, 0x6e, 0x78, 0x82, 0x18, 0xfe, 0x11, 0xef, 0xab,
							0x88, 0x22, 0x22, 0x2d, 0x34, 0xd4, 0x11,
						},
					},
				},
				{
					Name: "gno",
					ID:   2,
					Type: &FieldIntVar{
						Value: 1,
					},
				},
				{
					Name: "tag",
					ID:   3,
					Type: &FieldString{
						Value: "foobaz",
					},
				},
				{
					Name: "last_committed",
					ID:   4,
					Type: &FieldIntVar{
						Value: 0,
					},
				},
				{
					Name: "sequence_number",
					ID:   5,
					Type: &FieldIntVar{
						Value: 1,
					},
				},
				{
					Name: "immediate_commit_timestamp",
					ID:   6,
					Type: &FieldUintVar{
						Value: 1739823289369365,
					},
				},
				{
					Name:     "original_commit_timestamp",
					ID:       7,
					Type:     &FieldUintVar{},
					Optional: true,
					Skipped:  true,
				},
				{
					Name: "transaction_length",
					ID:   8,
					Type: &FieldUintVar{
						Value: 210,
					},
				},
				{
					Name: "immediate_server_version",
					ID:   9,
					Type: &FieldUintVar{
						Value: 90200,
					},
				},
				{
					Name:     "original_server_version",
					ID:       10,
					Type:     &FieldUintVar{},
					Optional: true,
					Skipped:  true,
				},
				{
					Name:     "commit_group_ticket",
					ID:       11,
					Optional: true,
					Skipped:  true,
				},
			},
		},
		fieldIndex: map[string]uint8{
			"gtid_flags":                 0,
			"uuid":                       1,
			"gno":                        2,
			"tag":                        3,
			"last_committed":             4,
			"sequence_number":            5,
			"immediate_commit_timestamp": 6,
			"original_commit_timestamp":  7,
			"transaction_length":         8,
			"immediate_server_version":   9,
			"original_server_version":    10,
			"commit_group_ticket":        11,
		},
	}

	err := Unmarshal(data, &msg)
	require.NoError(t, err)

	for i, f := range msg.Format.Fields {
		require.Equal(t, expected.Format.Fields[i], f)
	}

	require.Equal(t, expected, msg)

	sv, err := msg.GetFieldByName("immediate_server_version")
	require.NoError(t, err)
	require.Equal(t, uint8(9), sv.ID)
}
