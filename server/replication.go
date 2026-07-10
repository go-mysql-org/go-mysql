package server

import (
	"encoding/binary"

	"github.com/go-mysql-org/go-mysql/mysql"
)

func parseBinlogDump(data []byte) (mysql.Position, error) {
	if len(data) < 10 {
		return mysql.Position{}, mysql.ErrMalformPacket
	}
	var p mysql.Position
	p.Pos = binary.LittleEndian.Uint32(data[0:4])
	p.Name = string(data[10:])

	return p, nil
}

func parseBinlogDumpGTID(data []byte) (*mysql.MysqlGTIDSet, error) {
	if len(data) < 22 {
		return nil, mysql.ErrMalformPacket
	}

	pos := 0
	pos += 2 // flags
	pos += 4 // server_id

	// binlog filename length (at offset 6)
	nameSize := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
	pos += 4

	if len(data) < pos+nameSize+8+4 {
		return nil, mysql.ErrMalformPacket
	}

	pos += nameSize // binlog filename
	pos += 8        // binlog position

	// GTID data length
	dataSize := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
	pos += 4

	if len(data) < pos+dataSize {
		return nil, mysql.ErrMalformPacket
	}

	// parse GTID set
	return mysql.DecodeMysqlGTIDSet(data[pos : pos+dataSize])
}
