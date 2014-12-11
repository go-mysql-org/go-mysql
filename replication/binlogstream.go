package replication

import (
	"encoding/binary"
	. "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/packet"
)

type BinlogStreamReader struct {
	*packet.Conn
	serverID uint32

	host     string
	port     uint16
	user     string
	password string

	masterID uint32
}

func (b *BinlogStreamReader) writeBinglogDumpCommand(binlogPos uint32, fileName string) error {
	data := make([]byte, 4+1+4+2+4+len(fileName))

	pos := 4
	data[pos] = COM_BINLOG_DUMP
	pos++

	binary.LittleEndian.PutUint32(data[pos:], binlogPos)
	pos += 4

	//only support 0x01 BINGLOG_DUMP_NON_BLOCK
	binary.LittleEndian.PutUint16(data[pos:], BINLOG_DUMP_NON_BLOCK)
	pos += 2

	binary.LittleEndian.PutUint32(data[pos:], b.serverID)
	pos += 4

	data = append(data, fileName...)

	return b.WritePacket(data)
}

func (b *BinlogStreamReader) writeBinlogDumpGTIDCommand(flags uint16, fileName string, gtidData []byte) error {
	data := make([]byte, 4+1+2+4+4+len(fileName)+4+4+len(gtidData))
	pos := 4
	data[pos] = COM_BINLOG_DUMP_GTID
	pos++

	binary.LittleEndian.PutUint16(data[pos:], flags)
	pos += 2

	binary.LittleEndian.PutUint32(data[pos:], b.serverID)
	pos += 4

	binary.LittleEndian.PutUint32(data[pos:], uint32(len(fileName)))
	pos += 4

	n := copy(data[pos:], fileName)
	pos += n

	if flags&BINLOG_THROUGH_GTID > 0 {
		binary.LittleEndian.PutUint32(data[pos:], uint32(len(gtidData)))
		pos += 4
		n = copy(data[pos:], gtidData)
		pos += n
	}
	data = data[0:pos]

	return b.WritePacket(data)
}

func (b *BinlogStreamReader) writeRegisterSlaveCommand() error {
	data := make([]byte, 4+1+4+1+len(b.host)+1+len(b.user)+1+len(b.password),
		+2+4+4)
	pos := 4

	data[pos] = COM_REGISTER_SLAVE
	pos++

	binary.LittleEndian.PutUint32(data[pos:], b.serverID)
	pos += 4

	data[pos] = uint8(len(b.host))
	pos++
	n := copy(data[pos:], b.host)
	pos += n

	data[pos] = uint8(len(b.user))
	pos++
	n = copy(data[pos:], b.user)
	pos += n

	data[pos] = uint8(len(b.password))
	pos++
	n = copy(data[pos:], b.password)
	pos += n

	binary.LittleEndian.PutUint16(data[pos:], b.port)
	pos += 2

	//replication rank, not used
	binary.LittleEndian.PutUint32(data[pos:], 0)
	pos += 4

	binary.LittleEndian.PutUint32(data[pos:], b.masterID)

	return b.WritePacket(data)
}
