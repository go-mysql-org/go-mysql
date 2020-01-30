package replication

func LittleEndianUint64(b []byte) uint64 {
	var ret uint64
	switch len(b) {
	case 8:
		ret |= (uint64(b[7]) << 56)
		fallthrough
	case 7:
		ret |= (uint64(b[6]) << 48)
		fallthrough
	case 6:
		ret |= (uint64(b[5]) << 40)
		fallthrough
	case 5:
		ret |= (uint64(b[4]) << 32)
		fallthrough
	case 4:
		ret |= (uint64(b[3]) << 24)
		fallthrough
	case 3:
		ret |= (uint64(b[2]) << 16)
		fallthrough
	case 2:
		ret |= (uint64(b[1]) << 8)
		fallthrough
	case 1:
		ret |= uint64(b[0])
	}
	return ret
}
