package server

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
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

func TestStmtPrepareWithoutFieldsProvider(t *testing.T) {
	c := &Conn{
		h:     &mockPrepareHandler{context: "plain string", paramCount: 1, columnCount: 1},
		stmts: make(map[uint32]*Stmt),
	}

	result := c.dispatch(append([]byte{mysql.COM_STMT_PREPARE}, "SELECT * FROM t"...))

	stmt := result.(*Stmt)
	require.Nil(t, stmt.ParamFields)
	require.Nil(t, stmt.ColumnFields)
}

type mockFieldsProvider struct {
	paramFields, columnFields []*mysql.Field
}

func (m *mockFieldsProvider) GetParamFields() []*mysql.Field  { return m.paramFields }
func (m *mockFieldsProvider) GetColumnFields() []*mysql.Field { return m.columnFields }

func TestStmtPrepareWithFieldsProvider(t *testing.T) {
	provider := &mockFieldsProvider{
		paramFields:  []*mysql.Field{{Name: []byte("?"), Type: mysql.MYSQL_TYPE_LONG}},
		columnFields: []*mysql.Field{{Name: []byte("id"), Type: mysql.MYSQL_TYPE_LONGLONG}},
	}
	c := &Conn{
		h:     &mockPrepareHandler{context: provider, paramCount: 1, columnCount: 1},
		stmts: make(map[uint32]*Stmt),
	}

	result := c.dispatch(append([]byte{mysql.COM_STMT_PREPARE}, "SELECT id FROM t WHERE id = ?"...))

	stmt := result.(*Stmt)
	require.NotNil(t, stmt.ParamFields)
	require.NotNil(t, stmt.ColumnFields)
	require.Equal(t, mysql.MYSQL_TYPE_LONG, stmt.ParamFields[0].Type)
	require.Equal(t, mysql.MYSQL_TYPE_LONGLONG, stmt.ColumnFields[0].Type)
}
