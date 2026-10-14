package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"

	_ "github.com/go-mysql-org/go-mysql/test_util" // Will register common flags
)

func TestMysqlParseBinaryInt8(t *testing.T) {
	i8 := ParseBinaryInt8([]byte{128})
	require.Equal(t, int8(-128), i8)
}

func TestMysqlParseBinaryUint8(t *testing.T) {
	u8 := ParseBinaryUint8([]byte{128})
	require.Equal(t, uint8(128), u8)
}

func TestMysqlParseBinaryInt16(t *testing.T) {
	i16 := ParseBinaryInt16([]byte{1, 128})
	require.Equal(t, int16(-128*256+1), i16)
}

func TestMysqlParseBinaryUint16(t *testing.T) {
	u16 := ParseBinaryUint16([]byte{1, 128})
	require.Equal(t, uint16(128*256+1), u16)
}

func TestMysqlParseBinaryInt24(t *testing.T) {
	i32 := ParseBinaryInt24([]byte{1, 2, 128})
	require.Equal(t, int32(-128*65536+2*256+1), i32)
}

func TestMysqlParseBinaryUint24(t *testing.T) {
	u32 := ParseBinaryUint24([]byte{1, 2, 128})
	require.Equal(t, uint32(128*65536+2*256+1), u32)
}

func TestMysqlParseBinaryInt32(t *testing.T) {
	i32 := ParseBinaryInt32([]byte{1, 2, 3, 128})
	require.Equal(t, int32(-128*16777216+3*65536+2*256+1), i32)
}

func TestMysqlParseBinaryUint32(t *testing.T) {
	u32 := ParseBinaryUint32([]byte{1, 2, 3, 128})
	require.Equal(t, uint32(128*16777216+3*65536+2*256+1), u32)
}

func TestMysqlParseBinaryInt64(t *testing.T) {
	i64 := ParseBinaryInt64([]byte{1, 2, 3, 4, 5, 6, 7, 128})
	require.Equal(t, -128*int64(72057594037927936)+7*int64(281474976710656)+6*int64(1099511627776)+5*int64(4294967296)+4*16777216+3*65536+2*256+1, i64)
}

func TestMysqlParseBinaryUint64(t *testing.T) {
	u64 := ParseBinaryUint64([]byte{1, 2, 3, 4, 5, 6, 7, 128})
	require.Equal(t, 128*uint64(72057594037927936)+7*uint64(281474976710656)+6*uint64(1099511627776)+5*uint64(4294967296)+4*16777216+3*65536+2*256+1, u64)
}

func TestErrorCode(t *testing.T) {
	tbls := []struct {
		msg  string
		code int
	}{
		{"ERROR 1094 (HY000): Unknown thread id: 1094", 1094},
		{"error string", 0},
		{"abcdefg", 0},
		{"123455 ks094", 0},
		{"ERROR 1046 (3D000): Unknown error 1046", 1046},
	}
	for _, v := range tbls {
		require.Equal(t, v.code, ErrorCode(v.msg))
	}
}

func TestMysqlNullDecode(t *testing.T) {
	_, isNull, n := LengthEncodedInt([]byte{0xfb})

	require.True(t, isNull)
	require.Equal(t, 1, n)
}

func TestMysqlEmptyDecode(t *testing.T) {
	_, isNull, n := LengthEncodedInt(nil)
	require.True(t, isNull)
	require.Equal(t, 0, n)
}

func TestValidateFlavor(t *testing.T) {
	tbls := []struct {
		flavor string
		valid  bool
	}{
		{"mysql", true},
		{"mariadb", true},
		{"maria", false},
		{"MariaDB", true},
		{"msql", false},
		{"mArIAdb", true},
	}

	for _, f := range tbls {
		err := ValidateFlavor(f.flavor)
		if f.valid == true {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}
}
