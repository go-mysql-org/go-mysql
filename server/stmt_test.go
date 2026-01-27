package server

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/stmt"
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

type mockPrepareHandler struct {
	EmptyHandler
	context                 any
	paramCount, columnCount int
}

func (h *mockPrepareHandler) HandleStmtPrepare(query string) (int, int, any, error) {
	return h.paramCount, h.columnCount, h.context, nil
}

func TestStmtPrepareWithoutPreparedStmt(t *testing.T) {
	c := &Conn{
		h:     &mockPrepareHandler{context: "plain string", paramCount: 1, columnCount: 1},
		stmts: make(map[uint32]*Stmt),
	}

	result := c.dispatch(append([]byte{mysql.COM_STMT_PREPARE}, "SELECT * FROM t"...))

	st := result.(*Stmt)
	require.Nil(t, st.RawParamFields)
	require.Nil(t, st.RawColumnFields)
}

func TestStmtPrepareWithPreparedStmt(t *testing.T) {
	paramField := &mysql.Field{Name: []byte("?"), Type: mysql.MYSQL_TYPE_LONG}
	columnField := &mysql.Field{Name: []byte("id"), Type: mysql.MYSQL_TYPE_LONGLONG}

	provider := &stmt.PreparedStmt{
		RawParamFields:  [][]byte{paramField.Dump()},
		RawColumnFields: [][]byte{columnField.Dump()},
	}
	c := &Conn{
		h:     &mockPrepareHandler{context: provider, paramCount: 1, columnCount: 1},
		stmts: make(map[uint32]*Stmt),
	}

	result := c.dispatch(append([]byte{mysql.COM_STMT_PREPARE}, "SELECT id FROM t WHERE id = ?"...))

	st := result.(*Stmt)
	require.NotNil(t, st.RawParamFields)
	require.NotNil(t, st.RawColumnFields)
	paramFields, err := st.GetParamFields()
	require.NoError(t, err)
	require.Equal(t, mysql.MYSQL_TYPE_LONG, paramFields[0].Type)
	columnFields, err := st.GetColumnFields()
	require.NoError(t, err)
	require.Equal(t, mysql.MYSQL_TYPE_LONGLONG, columnFields[0].Type)
}

func TestBindStmtArgsTypedBytes(t *testing.T) {
	testcases := []struct {
		name        string
		paramType   byte
		paramValue  []byte
		expectType  byte
		expectBytes []byte
	}{
		{
			name:        "DATETIME",
			paramType:   mysql.MYSQL_TYPE_DATETIME,
			paramValue:  []byte{0x07, 0xe8, 0x07, 0x06, 0x0f, 0x0e, 0x1e, 0x2d},
			expectType:  mysql.MYSQL_TYPE_DATETIME,
			expectBytes: []byte{0xe8, 0x07, 0x06, 0x0f, 0x0e, 0x1e, 0x2d},
		},
		{
			name:        "VARCHAR",
			paramType:   mysql.MYSQL_TYPE_VARCHAR,
			paramValue:  []byte{0x05, 'h', 'e', 'l', 'l', 'o'},
			expectType:  mysql.MYSQL_TYPE_VARCHAR,
			expectBytes: []byte("hello"),
		},
		{
			name:        "BLOB",
			paramType:   mysql.MYSQL_TYPE_BLOB,
			paramValue:  []byte{0x04, 0x00, 0x01, 0x02, 0x03},
			expectType:  mysql.MYSQL_TYPE_BLOB,
			expectBytes: []byte{0x00, 0x01, 0x02, 0x03},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Conn{}
			s := &Stmt{Args: make([]interface{}, 1)}
			s.Params = 1

			nullBitmap := []byte{0x00}
			paramTypes := []byte{tc.paramType, 0x00}

			err := c.bindStmtArgs(s, nullBitmap, paramTypes, tc.paramValue)
			require.NoError(t, err)

			tv, ok := s.Args[0].(mysql.TypedBytes)
			require.True(t, ok, "expected TypedBytes, got %T", s.Args[0])
			require.Equal(t, tc.expectType, tv.Type)
			require.Equal(t, tc.expectBytes, tv.Bytes)
		})
	}
}
