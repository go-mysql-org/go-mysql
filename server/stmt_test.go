package server

import (
	"net"
	"testing"

	"github.com/go-mysql-org/go-mysql/client"
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
			s := &Stmt{Args: make([]any, 1)}
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

type recordingStmtHandler struct {
	EmptyHandler
	args [][]any
}

func (h *recordingStmtHandler) HandleStmtExecute(context any, query string, args []any) (*mysql.Result, error) {
	h.args = append(h.args, append([]any(nil), args...))
	return nil, nil
}

// Clients only send the parameter types (new-params-bound = 1) when they change,
// so later executions must decode their values with the previously sent types.
func TestHandleStmtExecuteReusesParamTypes(t *testing.T) {
	h := &recordingStmtHandler{}
	s := &Stmt{}
	s.Rest(1, 0, nil)
	c := Conn{h: h, stmts: map[uint32]*Stmt{1: s}}

	header := []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0} // stmt id, flags, iteration count
	execute := func(nullBitmap, newParamsBound byte, rest ...byte) error {
		data := append(append([]byte(nil), header...), nullBitmap, newParamsBound)
		_, err := c.handleStmtExecute(append(data, rest...))
		return err
	}

	// Types sent: LONGLONG, value 2.
	require.NoError(t, execute(0x0, 0x1, mysql.MYSQL_TYPE_LONGLONG, 0x0, 0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0))
	// Types not sent again, value 7.
	require.NoError(t, execute(0x0, 0x0, 0x7, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0))
	// Types not sent again, NULL value.
	require.NoError(t, execute(0x1, 0x0))

	require.Equal(t, [][]any{{int64(2)}, {int64(7)}, {nil}}, h.args)
}

func TestHandleStmtExecuteWithoutParamTypes(t *testing.T) {
	h := &recordingStmtHandler{}
	s := &Stmt{}
	s.Rest(1, 0, nil)
	c := Conn{h: h, stmts: map[uint32]*Stmt{1: s}}

	// The first execution doesn't send the types, so the value can't be decoded.
	data := []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x7, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
	_, err := c.handleStmtExecute(data)
	require.ErrorContains(t, err, "Incorrect arguments to mysqld_stmt_execute")
	require.Empty(t, h.args)
}

// A first execution whose parameters are all NULL needs no types, and the go-mysql
// client sends none for it (new-params-bound = 0).
func TestHandleStmtExecuteFirstAllNullWithoutParamTypes(t *testing.T) {
	for _, params := range []int{1, 2, 9} {
		h := &recordingStmtHandler{}
		s := &Stmt{}
		s.Rest(params, 0, nil)
		c := Conn{h: h, stmts: map[uint32]*Stmt{1: s}}

		nullBitmap := make([]byte, (params+7)>>3)
		for i := range params {
			nullBitmap[i>>3] |= 1 << (uint(i) % 8)
		}
		data := append([]byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0}, nullBitmap...)
		_, err := c.handleStmtExecute(append(data, 0x0))
		require.NoError(t, err)
		require.Equal(t, [][]any{make([]any, params)}, h.args)
	}
}

func TestHandleStmtExecuteMixedNullWithoutParamTypes(t *testing.T) {
	h := &recordingStmtHandler{}
	s := &Stmt{}
	s.Rest(2, 0, nil)
	c := Conn{h: h, stmts: map[uint32]*Stmt{1: s}}

	// The first parameter is NULL, the second can't be decoded without its type.
	data := []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x1, 0x0, 0x7, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
	_, err := c.handleStmtExecute(data)
	require.ErrorContains(t, err, "Incorrect arguments to mysqld_stmt_execute")
	require.Empty(t, h.args)
}

func TestHandleStmtExecuteAllNullThenReusesParamTypes(t *testing.T) {
	h := &recordingStmtHandler{}
	s := &Stmt{}
	s.Rest(1, 0, nil)
	c := Conn{h: h, stmts: map[uint32]*Stmt{1: s}}

	header := []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0} // stmt id, flags, iteration count
	execute := func(nullBitmap, newParamsBound byte, rest ...byte) error {
		data := append(append([]byte(nil), header...), nullBitmap, newParamsBound)
		_, err := c.handleStmtExecute(append(data, rest...))
		return err
	}

	// NULL value, no types sent.
	require.NoError(t, execute(0x1, 0x0))
	// Types sent: LONGLONG, value 5.
	require.NoError(t, execute(0x0, 0x1, mysql.MYSQL_TYPE_LONGLONG, 0x0, 0x5, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0))
	// Types not sent again, value 7.
	require.NoError(t, execute(0x0, 0x0, 0x7, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0))

	require.Equal(t, [][]any{{nil}, {int64(5)}, {int64(7)}}, h.args)
}

// Long-data parameters have no inline value in COM_STMT_EXECUTE, also when the
// types sent by an earlier execution are reused (new-params-bound = 0).
func TestHandleStmtExecuteLongDataReusesParamTypes(t *testing.T) {
	h := &recordingStmtHandler{}
	s := &Stmt{}
	s.Rest(1, 0, nil)
	c := Conn{h: h, stmts: map[uint32]*Stmt{1: s}}

	header := []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0} // stmt id, flags, iteration count
	execute := func(nullBitmap, newParamsBound byte, rest ...byte) error {
		data := append(append([]byte(nil), header...), nullBitmap, newParamsBound)
		_, err := c.handleStmtExecute(append(data, rest...))
		return err
	}
	sendLongData := func(chunk string) {
		data := append([]byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x0}, chunk...) // stmt id, param id
		require.NoError(t, c.handleStmtSendLongData(data))
	}

	// Types sent: BLOB, inline value "x".
	require.NoError(t, execute(0x0, 0x1, mysql.MYSQL_TYPE_BLOB, 0x0, 0x1, 'x'))
	// Types not sent again, no inline value for the long-data parameter.
	sendLongData("lo")
	sendLongData("ng")
	require.NoError(t, execute(0x0, 0x0))
	// The long data is consumed by the execution above.
	require.NoError(t, execute(0x0, 0x0, 0x1, 'y'))
	// Types sent again, still no inline value for the long-data parameter.
	sendLongData("again")
	require.NoError(t, execute(0x0, 0x1, mysql.MYSQL_TYPE_BLOB, 0x0))

	require.Equal(t, [][]any{
		{mysql.TypedBytes{Type: mysql.MYSQL_TYPE_BLOB, Bytes: []byte("x")}},
		{[]byte("long")},
		{mysql.TypedBytes{Type: mysql.MYSQL_TYPE_BLOB, Bytes: []byte("y")}},
		{[]byte("again")},
	}, h.args)
}

type prepareStmtHandler struct {
	recordingStmtHandler
}

func (h *prepareStmtHandler) HandleStmtPrepare(query string) (int, int, any, error) {
	return 1, 0, nil, nil
}

func (h *prepareStmtHandler) HandleStmtExecute(context any, query string, args []any) (*mysql.Result, error) {
	h.args = append(h.args, append([]any(nil), args...))
	return &mysql.Result{}, nil
}

func TestClientStmtExecuteNullFirst(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	h := &prepareStmtHandler{}
	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		co, _ := NewConn(conn, "root", "", h)
		if co != nil {
			//nolint:revive // loop drains commands; work is in the condition
			for co.HandleCommand() == nil {
			}
		}
	}()

	conn, err := client.Connect(l.Addr().String(), "root", "", "")
	require.NoError(t, err)
	defer conn.Close()

	st, err := conn.Prepare("SELECT ?")
	require.NoError(t, err)
	defer st.Close()

	_, err = st.Execute(nil)
	require.NoError(t, err)
	_, err = st.Execute(int64(5))
	require.NoError(t, err)
	_, err = st.Execute(nil)
	require.NoError(t, err)

	require.Equal(t, [][]any{{nil}, {int64(5)}, {nil}}, h.args)
}
