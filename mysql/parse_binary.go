package mysql

import (
	"encoding/binary"
	"math"
)

func ParseBinaryInt8(data []byte) int8 {
	return int8(data[0])
}
func ParseBinaryUint8(data []byte) uint8 {
	return data[0]
}

func ParseBinaryInt16(data []byte) int16 {
	return int16(binary.LittleEndian.Uint16(data))
}
func ParseBinaryUint16(data []byte) uint16 {
	return binary.LittleEndian.Uint16(data)
}

type Int24 int32

func (v Int24) ToInt32() int32 {
	return int32(v)
}
func (v Int24) ToUint24() Uint24 {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, uint32(v.ToInt32()))
	return Uint24(data[0]) | Uint24(data[1])<<8 | Uint24(data[2])<<16
}

type Uint24 uint32

func (v Uint24) ToUint32() uint32 {
	return uint32(v)
}
func (v Uint24) ToInt24() Int24 {
	if v&0x00800000 != 0 {
		v |= 0xFF000000
	}
	return Int24(v)
}

func ParseBinaryInt24(data []byte) Int24 {
	u32 := uint32(ParseBinaryUint24(data))
	if u32&0x00800000 != 0 {
		u32 |= 0xFF000000
	}
	return Int24(u32)
}
func ParseBinaryUint24(data []byte) Uint24 {
	return Uint24(data[0]) | Uint24(data[1])<<8 | Uint24(data[2])<<16
}

func ParseBinaryInt32(data []byte) int32 {
	return int32(binary.LittleEndian.Uint32(data))
}
func ParseBinaryUint32(data []byte) uint32 {
	return binary.LittleEndian.Uint32(data)
}

func ParseBinaryInt64(data []byte) int64 {
	return int64(binary.LittleEndian.Uint64(data))
}
func ParseBinaryUint64(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data)
}

func ParseBinaryFloat32(data []byte) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(data))
}

func ParseBinaryFloat64(data []byte) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(data))
}
