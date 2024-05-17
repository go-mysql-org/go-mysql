package packet

import "sync/atomic"

type Stats struct {
	packetTxCompressedSize   atomic.Uint64
	packetTxUncompressedSize atomic.Uint64
}

func (p *Stats) AddTxCompressedSize(size uint64) {
	p.packetTxCompressedSize.Add(size)
}

func (p *Stats) AddTxUncompressedSize(size uint64) {
	p.packetTxUncompressedSize.Add(size)
}

func (p *Stats) GetTxCompressedSize() uint64 {
	return p.packetTxCompressedSize.Load()
}

func (p *Stats) GetTxUncompressedSize() uint64 {
	return p.packetTxUncompressedSize.Load()
}
