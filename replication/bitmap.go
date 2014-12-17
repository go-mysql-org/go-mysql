package replication

import (
	"encoding/binary"
	"reflect"
	"unsafe"
)

// refer from mysql my_bitmap.c, use for row based event
type Bitmap struct {
	Data         []byte
	LastWordMask uint32
	Nbits        uint32
}

var bits2Nbits = [256]uint8{
	0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
}

func countBitsUint32(v uint32) uint32 {
	return uint32(bits2Nbits[uint8(v)] + bits2Nbits[uint8(v>>8)] + bits2Nbits[uint8(v>>16)] + bits2Nbits[uint8(v>>24)])
}

func NewBitmap(buf []byte, nbits uint32) *Bitmap {
	b := new(Bitmap)
	b.Nbits = nbits

	b.Data = make([]byte, ((nbits+31)/32)*4)

	b.createLastWordMask()

	for i := range b.Data {
		b.Data[i] = byte(0)
	}

	copy(b.Data, buf)

	b.createLastWordMask()

	return b
}

func (b *Bitmap) createLastWordMask() {
	used := 1 + (b.Nbits-1)&0x7
	mask := uint8((^((1 << used) - 1)) & 255)

	var data []byte
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	pbytes.Data = uintptr(unsafe.Pointer(&b.LastWordMask))
	pbytes.Len = 4
	pbytes.Cap = 4

	switch ((b.Nbits + 7) / 8) & 3 {
	case 1:
		b.LastWordMask = ^uint32(0)
		data[0] = mask
	case 2:
		b.LastWordMask = ^uint32(0)
		data[0] = 0
		data[1] = mask
	case 3:
		b.LastWordMask = 0
		data[2] = mask
		data[3] = 0xFF
	case 0:
		b.LastWordMask = 0
		data[3] = mask
	}
}

func (b *Bitmap) BitsSet() uint32 {
	var res uint32 = 0

	for i := 0; i < len(b.Data)/4-1; i++ {
		res += countBitsUint32(binary.LittleEndian.Uint32(b.Data[i*4:]))
	}

	res += countBitsUint32(binary.LittleEndian.Uint32(b.Data[len(b.Data)-4:]) & ^b.LastWordMask)
	return res
}

func (b *Bitmap) IsSet(bit uint32) byte {
	return b.Data[bit/8] & (1 << (bit & 7))
}
