package replication

// BytesReplaceWithIndex replace bytes
// include from, no include to
func BytesReplaceWithIndex(dst []byte, from, to int, newBuf []byte) {
	tmpBuf := make([]byte, len(newBuf))
	copy(tmpBuf, newBuf)

	if from == to && len(newBuf) == 1 {
		dst[from] = tmpBuf[0]
	} else {
		dst = append(dst[0:from], tmpBuf...)
		dst = append(dst, dst[to:]...)
	}
}
