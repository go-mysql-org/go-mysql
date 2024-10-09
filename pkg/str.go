package pkg

import (
	"bytes"
	b64 "encoding/base64"
)

func Base64EncodeToFixedBytes(src []byte) []byte {
	dst := make([]byte, b64.StdEncoding.EncodedLen(len(src)))
	b64.StdEncoding.Encode(dst, src)
	s := splitByFixedLength(dst, 76)
	return bytes.Join(s, []byte("\n"))
}

func splitByFixedLength(s []byte, l int) [][]byte {
	if len(s) <= l || l <= 0 {
		return [][]byte{s}
	}
	length := len(s)
	var tmp [][]byte
	for i := 0; i <= length/l-1; i++ {
		tmp = append(tmp, s[i*l:(i+1)*l])
	}
	if length%l != 0 {
		tmp = append(tmp, s[length/l*l:])
	}
	return tmp
}
