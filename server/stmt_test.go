package server

import (
	"encoding/binary"
	"fmt"
	"net"
	"testing"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/packet"
	"github.com/go-mysql-org/go-mysql/stmt"
	mockconn "github.com/go-mysql-org/go-mysql/test_util/conn"
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

type dupEntryExecHandler struct {
	EmptyHandler
}

func (h dupEntryExecHandler) HandleStmtExecute(any, string, []any) (*mysql.Result, error) {
	return nil, mysql.NewError(mysql.ER_DUP_ENTRY, "Duplicate entry '1' for key 'PRIMARY'")
}

// errors.Trace around HandleStmtExecute must not turn ER_DUP_ENTRY into ER_UNKNOWN_ERROR.
func TestHandleStmtExecuteDupEntryOnWire(t *testing.T) {
	const msg = "Duplicate entry '1' for key 'PRIMARY'"
	c := &Conn{
		h: dupEntryExecHandler{},
		stmts: map[uint32]*Stmt{
			1: {},
		},
	}
	// COM_STMT_EXECUTE: stmt id 1, flags 0, iteration count 1, no params.
	_, err := c.handleStmtExecute([]byte{1, 0, 0, 0, 0, 1, 0, 0, 0})
	require.Error(t, err)

	clientConn := &mockconn.MockConn{}
	out := &Conn{Conn: packet.NewConn(clientConn)}
	out.SetCapability(mysql.CLIENT_PROTOCOL_41)
	require.NoError(t, out.writeError(err))

	payload := append([]byte{
		mysql.ERR_HEADER,
		0x26, 0x04, // 1062
		'#',
		'2', '3', '0', '0', '0',
	}, msg...)
	expected := append([]byte{byte(len(payload)), 0, 0, 0}, payload...)
	require.Equal(t, expected, clientConn.WriteBuffered)
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

type cursorStmtHandler struct {
	EmptyHandler
	rs *mysql.Resultset
}

func (h *cursorStmtHandler) HandleStmtExecute(context any, query string, args []any) (*mysql.Result, error) {
	var err error
	h.rs, err = mysql.BuildSimpleBinaryResultset([]string{"id", "name"},
		[][]any{{int64(1), "a"}, {int64(2), "b"}, {int64(3), "c"}})
	if err != nil {
		return nil, err
	}
	return mysql.NewResult(h.rs), nil
}

// splitPackets decodes the packets of a MySQL packet stream.
func splitPackets(t *testing.T, buf []byte) [][]byte {
	t.Helper()
	var packets [][]byte
	for len(buf) > 0 {
		require.GreaterOrEqual(t, len(buf), 4)
		n := int(buf[0]) | int(buf[1])<<8 | int(buf[2])<<16
		require.GreaterOrEqual(t, len(buf), 4+n)
		packets = append(packets, buf[4:4+n])
		buf = buf[4+n:]
	}
	return packets
}

// statusOf returns the status flags of an EOF packet, or of the EOF-headed OK
// packet sent instead of it under CLIENT_DEPRECATE_EOF.
func statusOf(t *testing.T, packet []byte) uint16 {
	t.Helper()
	require.Equal(t, mysql.EOF_HEADER, packet[0])
	return uint16(packet[3]) | uint16(packet[4])<<8
}

// A COM_STMT_EXECUTE asking for CURSOR_TYPE_READ_ONLY answers with the column
// definitions and SERVER_STATUS_CURSOR_EXISTS only; COM_STMT_FETCH then serves
// the rows in batches and ends the cursor with SERVER_STATUS_LAST_ROW_SEND.
func TestStmtCursor(t *testing.T) {
	for _, deprecateEOF := range []bool{false, true} {
		t.Run(fmt.Sprintf("deprecateEOF=%t", deprecateEOF), func(t *testing.T) {
			h := &cursorStmtHandler{}
			clientConn := &mockconn.MockConn{MultiWrite: true}
			c := &Conn{
				Conn:       packet.NewConn(clientConn),
				serverConf: &Server{},
				capability: mysql.CLIENT_PROTOCOL_41,
				h:          h,
				stmts:      make(map[uint32]*Stmt),
			}
			if deprecateEOF {
				c.SetCapability(mysql.CLIENT_DEPRECATE_EOF)
				require.NoError(t, c.serverConf.SetCapability(mysql.CLIENT_DEPRECATE_EOF))
			}

			st := &Stmt{}
			st.Rest(0, 2, nil)
			c.stmts[1] = st

			// run dispatches one command and returns the packets it answered with.
			run := func(cmd byte, args ...byte) [][]byte {
				clientConn.WriteBuffered = nil
				require.NoError(t, c.WriteValue(c.dispatch(append([]byte{cmd}, args...))))
				return splitPackets(t, clientConn.WriteBuffered)
			}
			// COM_STMT_EXECUTE: stmt id 1, flags, iteration count 1, no params.
			execute := func(flags byte) [][]byte {
				return run(mysql.COM_STMT_EXECUTE, 1, 0, 0, 0, flags, 1, 0, 0, 0)
			}
			// COM_STMT_FETCH: stmt id 1, num_rows.
			fetch := func(numRows byte) [][]byte {
				return run(mysql.COM_STMT_FETCH, 1, 0, 0, 0, numRows, 0, 0, 0)
			}

			// The cursor execution: column count, the column definitions, and a
			// terminator carrying SERVER_STATUS_CURSOR_EXISTS. No rows.
			packets := execute(mysql.CURSOR_TYPE_READ_ONLY)
			require.Len(t, packets, 4)
			require.Equal(t, []byte{2}, packets[0])
			require.Equal(t, h.rs.Fields[0].Dump(), packets[1])
			require.Equal(t, h.rs.Fields[1].Dump(), packets[2])
			require.Equal(t, mysql.SERVER_STATUS_CURSOR_EXISTS, statusOf(t, packets[3]))

			// The cursor drains the row buffer of h.rs, so keep a copy of it.
			var rows [][]byte
			for _, row := range h.rs.RowDatas {
				rows = append(rows, row)
			}

			// A batch smaller than the cursor: the cursor stays open.
			packets = fetch(2)
			require.Len(t, packets, 3)
			require.Equal(t, rows[0], packets[0])
			require.Equal(t, rows[1], packets[1])
			require.Equal(t, mysql.SERVER_STATUS_CURSOR_EXISTS, statusOf(t, packets[2]))

			// A zero-count fetch asks for no rows: an empty batch, the cursor
			// still open, and the remaining rows served afterwards.
			packets = fetch(0)
			require.Len(t, packets, 1)
			require.Equal(t, mysql.SERVER_STATUS_CURSOR_EXISTS, statusOf(t, packets[0]))
			require.NotNil(t, st.cursor)

			// The last row is sent with SERVER_STATUS_LAST_ROW_SEND.
			packets = fetch(2)
			require.Len(t, packets, 2)
			require.Equal(t, rows[2], packets[0])
			require.Equal(t, mysql.SERVER_STATUS_LAST_ROW_SEND, statusOf(t, packets[1]))

			// The flags describe this response only: the next command answers
			// without them.
			packets = run(mysql.COM_PING)
			require.Len(t, packets, 1)
			require.Zero(t, uint16(packets[0][3])|uint16(packets[0][4])<<8)
			require.False(t, c.HasStatus(mysql.SERVER_STATUS_LAST_ROW_SEND))

			// A drained cursor is closed, so fetching again is an error rather
			// than an empty batch that never ends.
			packets = fetch(1)
			require.Len(t, packets, 1)
			require.Equal(t, mysql.ERR_HEADER, packets[0][0])
			require.Equal(t, uint16(mysql.ER_STMT_HAS_NO_OPEN_CURSOR), binary.LittleEndian.Uint16(packets[0][1:3]))

			// An execution without the cursor flag answers inline again, and
			// closes the cursor left open by the previous one.
			packets = execute(mysql.CURSOR_TYPE_NO_CURSOR)
			require.Equal(t, mysql.EOF_HEADER, packets[len(packets)-1][0])
			require.False(t, c.HasStatus(mysql.SERVER_STATUS_CURSOR_EXISTS))
			require.False(t, c.HasStatus(mysql.SERVER_STATUS_LAST_ROW_SEND))
			require.Nil(t, st.cursor)

			// COM_STMT_RESET closes the cursor.
			execute(mysql.CURSOR_TYPE_READ_ONLY)
			require.True(t, c.HasStatus(mysql.SERVER_STATUS_CURSOR_EXISTS))
			run(mysql.COM_STMT_RESET, 1, 0, 0, 0)
			require.Nil(t, st.cursor)
			require.False(t, c.HasStatus(mysql.SERVER_STATUS_CURSOR_EXISTS))
			_, err := c.handleStmtFetch([]byte{1, 0, 0, 0, 1, 0, 0, 0})
			require.ErrorContains(t, err, "has no open cursor")

			// COM_STMT_CLOSE drops the cursor with the statement.
			execute(mysql.CURSOR_TYPE_READ_ONLY)
			run(mysql.COM_STMT_CLOSE, 1, 0, 0, 0)
			require.False(t, c.HasStatus(mysql.SERVER_STATUS_CURSOR_EXISTS))
			_, err = c.handleStmtFetch([]byte{1, 0, 0, 0, 1, 0, 0, 0})
			require.ErrorContains(t, err, "Unknown prepared statement handler")
		})
	}
}

type noResultStmtHandler struct {
	EmptyHandler
}

func (h noResultStmtHandler) HandleStmtExecute(context any, query string, args []any) (*mysql.Result, error) {
	return mysql.NewResultReserveResultset(0), nil
}

// CURSOR_TYPE_READ_ONLY on a statement that has no result set is a plain OK.
func TestStmtCursorWithoutResultset(t *testing.T) {
	clientConn := &mockconn.MockConn{}
	c := &Conn{
		Conn:       packet.NewConn(clientConn),
		serverConf: &Server{},
		capability: mysql.CLIENT_PROTOCOL_41,
		h:          noResultStmtHandler{},
		stmts:      map[uint32]*Stmt{1: {}},
	}

	require.NoError(t, c.WriteValue(c.dispatch([]byte{mysql.COM_STMT_EXECUTE, 1, 0, 0, 0, mysql.CURSOR_TYPE_READ_ONLY, 1, 0, 0, 0})))

	packets := splitPackets(t, clientConn.WriteBuffered)
	require.Len(t, packets, 1)
	require.Equal(t, mysql.OK_HEADER, packets[0][0])
	require.False(t, c.HasStatus(mysql.SERVER_STATUS_CURSOR_EXISTS))
}
