package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTypeAndFlag_string(t *testing.T) {
	qattr := QueryAttribute{
		Name:  "attrname",
		Value: "attrvalue",
	}

	tf := qattr.TypeAndFlag()
	require.Equal(t, []byte{0xfe, 0x0}, tf)

	vb := qattr.ValueBytes()
	require.Equal(t, []byte{0x9, 0x61, 0x74, 0x74, 0x72, 0x76, 0x61, 0x6c, 0x75, 0x65}, vb)
}

func TestTypeAndFlag_uint64(t *testing.T) {
	qattr := QueryAttribute{
		Name:  "attrname",
		Value: uint64(12345),
	}

	tf := qattr.TypeAndFlag()
	require.Equal(t, []byte{0x08, 0x80}, tf)

	vb := qattr.ValueBytes()
	require.Equal(t, []byte{0x39, 0x30, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}, vb)
}
