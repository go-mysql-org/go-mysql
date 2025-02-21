package server

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHandleStmtExecute(t *testing.T) {
	c := Conn{}
	c.stmts = map[uint32]*Stmt{
		1: {},
	}
	testcases := []struct {
		data    []byte
		errtext string
	}{
		{
			[]byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
			"ERROR 1243 (HY000): Unknown prepared statement handler (0) given to stmt_execute",
		},
		{
			[]byte{0x1, 0x0, 0x0, 0x0, 0xff, 0x0, 0x0, 0x0, 0x0, 0x0},
			"ERROR 1105 (HY000): unsupported flags 0xff",
		},
		{
			[]byte{0x1, 0x0, 0x0, 0x0, 0x01, 0x0, 0x0, 0x0, 0x0, 0x0},
			"ERROR 1105 (HY000): unsupported flag CURSOR_TYPE_READ_ONLY",
		},
		{
			[]byte{0x1, 0x0, 0x0, 0x0, 0x02, 0x0, 0x0, 0x0, 0x0, 0x0},
			"ERROR 1105 (HY000): unsupported flag CURSOR_TYPE_FOR_UPDATE",
		},
		{
			[]byte{0x1, 0x0, 0x0, 0x0, 0x04, 0x0, 0x0, 0x0, 0x0, 0x0},
			"ERROR 1105 (HY000): unsupported flag CURSOR_TYPE_SCROLLABLE",
		},
	}

	for _, tc := range testcases {
		_, err := c.handleStmtExecute(tc.data)
		if tc.errtext == "" {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, tc.errtext)
		}
	}
}
