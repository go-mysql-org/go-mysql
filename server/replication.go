package server

import (
	"encoding/binary"
	"github.com/go-mysql-org/go-mysql/mysql"
)

func parseBinlogDump(data []byte) (*mysql.Position, error) {
	var p mysql.Position
	p.Pos = binary.LittleEndian.Uint32(data[0:4])
	p.Name = string(data[10:])

	return &p, nil
}

func parseBinlogDumpGTID(data []byte) (*mysql.MysqlGTIDSet, error) {
	lenPosName := binary.LittleEndian.Uint32(data[11:15])

	return mysql.DecodeMysqlGTIDSet(data[22+lenPosName:])
}
