package mysql

import (
	"encoding/binary"
)

func ParseBinaryInt8(data []byte) int64 {
	return int64(int8(data[0]))
}
func ParseBinaryUint8(data []byte) uint64 {
	return uint64((data[0]))
}

func ParseBinaryInt16(data []byte) int64 {
	return int64(int16(binary.LittleEndian.Uint16(data)))
}
func ParseBinaryUint16(data []byte) uint64 {
	return uint64(binary.LittleEndian.Uint16(data))
}

func ParseBinaryInt24(data []byte) int64 {
	u32 := uint32(ParseBinaryUint24(data))
	if u32&0x00800000 != 0 {
		u32 |= 0xFF000000
	}
	return int64(int32(u32))
}
func ParseBinaryUint24(data []byte) uint64 {
	return uint64(uint32(data[0]) + uint32(data[1])<<8 + uint32(data[2])<<16)
}

func ParseBinaryInt32(data []byte) int64 {
	return int64(int32(binary.LittleEndian.Uint32(data)))
}
func ParseBinaryUint32(data []byte) uint64 {
	return uint64(binary.LittleEndian.Uint32(data))
}

func ParseBinaryInt64(data []byte) int64 {
	return int64(binary.LittleEndian.Uint64(data))
}
func ParseBinaryUint64(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data)
}
