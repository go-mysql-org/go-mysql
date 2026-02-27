package server

import (
	"encoding/binary"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

func TestParseBinlogDumpGTID_EmptyBinlogName(t *testing.T) {

	// Create a valid MySQL GTID set in binary format
	gtidSet, err := mysql.ParseMysqlGTIDSet("a9d88f83-c14e-11ec-be16-0242ac110002:1-10")
	require.NoError(t, err)
	gtidData := gtidSet.Encode()

	data := make([]byte, 0, 100)
	data = append(data, 0x00, 0x04)                                     // flags (2 bytes)
	data = append(data, 0x01, 0x00, 0x00, 0x00)                         // server_id (4 bytes)
	data = append(data, 0x00, 0x00, 0x00, 0x00)                         // name_len = 0 (4 bytes)
	data = append(data, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00) // pos (8 bytes)

	gtidLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(gtidLen, uint32(len(gtidData)))
	data = append(data, gtidLen...)  // gtid_len (4 bytes)
	data = append(data, gtidData...) // gtid_data (binary format)

	result, err := parseBinlogDumpGTID(data)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "a9d88f83-c14e-11ec-be16-0242ac110002:1-10", result.String())
}

func TestParseBinlogDumpGTID_WithBinlogName(t *testing.T) {
	binlogName := []byte("mysql-bin.000001")

	// Create a valid MySQL GTID set in binary format
	gtidSet, err := mysql.ParseMysqlGTIDSet("a9d88f83-c14e-11ec-be16-0242ac110002:1-10")
	require.NoError(t, err)
	gtidData := gtidSet.Encode()

	data := make([]byte, 0, 100)

	// flags (2 bytes)
	data = append(data, 0x00, 0x04)

	// server_id (4 bytes)
	data = append(data, 0x01, 0x00, 0x00, 0x00)

	// binlog-name-len (4 bytes) at offset 6
	nameLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(nameLen, uint32(len(binlogName)))
	data = append(data, nameLen...)

	// binlog-name (N bytes) at offset 10
	data = append(data, binlogName...)

	// binlog-pos (8 bytes)
	data = append(data, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)

	// gtid-data-len (4 bytes)
	gtidLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(gtidLen, uint32(len(gtidData)))
	data = append(data, gtidLen...)

	// gtid-data (N bytes) - binary format
	data = append(data, gtidData...)

	result, err := parseBinlogDumpGTID(data)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "a9d88f83-c14e-11ec-be16-0242ac110002:1-10", result.String())
}

func TestParseBinlogDumpGTID_MalformedPacket(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{
			name: "too short for header",
			data: []byte{0x00, 0x04},
		},
		{
			name: "too short for name length",
			data: []byte{0x00, 0x04, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00},
		},
		{
			name: "too short for binlog name",
			data: func() []byte {
				d := make([]byte, 10)
				binary.LittleEndian.PutUint32(d[6:10], 10) // name_len = 10
				return d
			}(),
		},
		{
			name: "too short for gtid data",
			data: func() []byte {
				d := make([]byte, 30)
				binary.LittleEndian.PutUint32(d[6:10], 0)    // name_len = 0
				binary.LittleEndian.PutUint32(d[18:22], 100) // gtid_len = 100
				return d
			}(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseBinlogDumpGTID(tc.data)
			require.Equal(t, mysql.ErrMalformPacket, err)
		})
	}
}

func TestParseBinlogDump(t *testing.T) {
	testCases := []struct {
		name    string
		data    []byte
		wantPos mysql.Position
		wantErr error
	}{
		{
			name: "valid packet",
			data: func() []byte {
				d := make([]byte, 11)
				binary.LittleEndian.PutUint32(d[0:4], 12345) // pos
				d[10] = 'm'                                  // start of filename
				d = append(d, []byte("ysql-bin.000001")...)
				return d
			}(),
			wantPos: mysql.Position{
				Pos:  12345,
				Name: "mysql-bin.000001",
			},
		},
		{
			name:    "too short packet",
			data:    []byte{0x01, 0x02, 0x03},
			wantErr: mysql.ErrMalformPacket,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pos, err := parseBinlogDump(tc.data)
			if tc.wantErr != nil {
				require.Equal(t, tc.wantErr, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.wantPos, pos)
			}
		})
	}
}
