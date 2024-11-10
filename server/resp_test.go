package server

import (
	"errors"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/packet"
	mockconn "github.com/go-mysql-org/go-mysql/test_util/conn"
	"github.com/stretchr/testify/require"
)

func TestConnWriteOK(t *testing.T) {
	clientConn := &mockconn.MockConn{}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	result := &mysql.Result{
		AffectedRows: 1,
		InsertId:     2,
	}

	// write ok with insertid and affectedrows set
	err := conn.writeOK(result)
	require.NoError(t, err)
	expected := []byte{3, 0, 0, 0, mysql.OK_HEADER, 1, 2}
	require.Equal(t, expected, clientConn.WriteBuffered)

	// set capability for CLIENT_PROTOCOL_41
	conn.SetCapability(mysql.CLIENT_PROTOCOL_41)
	conn.SetStatus(mysql.SERVER_QUERY_WAS_SLOW)
	err = conn.writeOK(result)
	require.NoError(t, err)
	expected = []byte{7, 0, 0, 1, mysql.OK_HEADER, 1, 2, 0, 8, 0, 0}
	require.Equal(t, expected, clientConn.WriteBuffered)
}

func TestConnWriteEOF(t *testing.T) {
	clientConn := &mockconn.MockConn{}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	// write regular EOF
	err := conn.writeEOF()
	require.NoError(t, err)
	expected := []byte{1, 0, 0, 0, mysql.EOF_HEADER}
	require.Equal(t, expected, clientConn.WriteBuffered)

	// set capability for CLIENT_PROTOCOL_41
	conn.SetCapability(mysql.CLIENT_PROTOCOL_41)
	conn.SetStatus(mysql.SERVER_MORE_RESULTS_EXISTS)
	err = conn.writeEOF()
	require.NoError(t, err)
	expected = []byte{5, 0, 0, 1, mysql.EOF_HEADER, 0, 0, 8, 0}
	require.Equal(t, expected, clientConn.WriteBuffered)
}

func TestConnWriteError(t *testing.T) {
	clientConn := &mockconn.MockConn{}
	conn := &Conn{Conn: packet.NewConn(clientConn)}
	merr := mysql.NewDefaultError(mysql.ER_YES) // nice and short error message

	// write regular Error
	err := conn.writeError(merr)
	require.NoError(t, err)
	expected := []byte{6, 0, 0, 0, mysql.ERR_HEADER, 235, 3, 89, 69, 83}
	require.Equal(t, expected, clientConn.WriteBuffered)

	// set capability for CLIENT_PROTOCOL_41
	conn.SetCapability(mysql.CLIENT_PROTOCOL_41)
	err = conn.writeError(merr)
	require.NoError(t, err)
	expected = []byte{12, 0, 0, 1, mysql.ERR_HEADER, 235, 3, 35, 72, 89, 48, 48, 48, 89, 69, 83}
	require.Equal(t, expected, clientConn.WriteBuffered)

	// unknown error
	err = conn.writeError(errors.New("test"))
	require.NoError(t, err)
	expected = []byte{13, 0, 0, 2, mysql.ERR_HEADER, 81, 4, 35, 72, 89, 48, 48, 48, 116, 101, 115, 116}
	require.Equal(t, expected, clientConn.WriteBuffered)
}

func TestConnWriteAuthSwitchRequest(t *testing.T) {
	clientConn := &mockconn.MockConn{}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	err := conn.writeAuthSwitchRequest("test")
	require.NoError(t, err)
	// first 10 bytes are static, then there is a part random, ending with a \0
	expected := []byte{27, 0, 0, 0, mysql.EOF_HEADER, 116, 101, 115, 116, 0}
	require.Equal(t, expected, clientConn.WriteBuffered[:10])
	require.Equal(t, byte(0), clientConn.WriteBuffered[len(clientConn.WriteBuffered)-1])
}

func TestConnReadAuthSwitchRequestResponse(t *testing.T) {
	clientConn := &mockconn.MockConn{}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	// prepare response for \NUL
	clientConn.SetResponse([][]byte{{1, 0, 0, 0, 0}})
	data, err := conn.readAuthSwitchRequestResponse()
	require.NoError(t, err)
	require.Equal(t, []byte{}, data)

	// prepare response for some auth switch data
	clientConn.SetResponse([][]byte{{4, 0, 0, 0, 1, 2, 3, 4}})
	conn = &Conn{Conn: packet.NewConn(clientConn)}

	data, err = conn.readAuthSwitchRequestResponse()
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3, 4}, data)
}

func TestConnWriteAuthMoreDataPubkey(t *testing.T) {
	clientConn := &mockconn.MockConn{}
	conn := &Conn{
		Conn: packet.NewConn(clientConn),
		serverConf: &Server{
			pubKey: []byte{1, 2, 3, 4},
		},
	}

	err := conn.writeAuthMoreDataPubkey()
	require.NoError(t, err)
	expected := []byte{5, 0, 0, 0, mysql.MORE_DATE_HEADER, 1, 2, 3, 4}
	require.Equal(t, expected, clientConn.WriteBuffered)
}

func TestConnWriteAuthMoreDataFullAuth(t *testing.T) {
	clientConn := &mockconn.MockConn{}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	err := conn.writeAuthMoreDataFullAuth()
	require.NoError(t, err)
	expected := []byte{2, 0, 0, 0, mysql.MORE_DATE_HEADER, mysql.CACHE_SHA2_FULL_AUTH}
	require.Equal(t, expected, clientConn.WriteBuffered)
}

func TestConnWriteAuthMoreDataFastAuth(t *testing.T) {
	clientConn := &mockconn.MockConn{}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	err := conn.writeAuthMoreDataFastAuth()
	require.NoError(t, err)
	expected := []byte{2, 0, 0, 0, mysql.MORE_DATE_HEADER, mysql.CACHE_SHA2_FAST_AUTH}
	require.Equal(t, expected, clientConn.WriteBuffered)
}

func TestConnWriteResultset(t *testing.T) {
	clientConn := &mockconn.MockConn{MultiWrite: true}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	r := mysql.NewResultset(0)

	// write minimalistic resultset
	err := conn.writeResultset(r)
	require.NoError(t, err)
	// column length 0
	require.Equal(t, []byte{1, 0, 0, 0, 0}, clientConn.WriteBuffered[:5])
	// no fields and an EOF
	require.Equal(t, []byte{1, 0, 0, 1, mysql.EOF_HEADER}, clientConn.WriteBuffered[5:10])
	// no rows and another EOF
	require.Equal(t, []byte{1, 0, 0, 2, mysql.EOF_HEADER}, clientConn.WriteBuffered[10:])

	// reset write buffer and fill up the resultset with (little) data
	clientConn.WriteBuffered = []byte{}
	r, err = mysql.BuildSimpleTextResultset([]string{"a"}, [][]interface{}{{"b"}})
	require.NoError(t, err)
	err = conn.writeResultset(r)
	require.NoError(t, err)
	// column length 1
	require.Equal(t, []byte{1, 0, 0, 3, 1}, clientConn.WriteBuffered[:5])
	// fields and EOF
	require.Equal(t, []byte{23, 0, 0, 4, 3, 100, 101, 102, 0, 0, 0, 1, 'a', 0, 12, 33, 0, 0, 0, 0, 0, 253, 0, 0, 0, 0, 0}, clientConn.WriteBuffered[5:32])
	require.Equal(t, []byte{1, 0, 0, 5, mysql.EOF_HEADER}, clientConn.WriteBuffered[32:37])
	// rowdata and EOF
	require.Equal(t, []byte{2, 0, 0, 6, 1, 'b'}, clientConn.WriteBuffered[37:43])
	require.Equal(t, []byte{1, 0, 0, 7, mysql.EOF_HEADER}, clientConn.WriteBuffered[43:])
}

func TestConnWriteFieldList(t *testing.T) {
	clientConn := &mockconn.MockConn{MultiWrite: true}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	r, err := mysql.BuildSimpleTextResultset([]string{"c"}, [][]interface{}{{"d"}})
	require.NoError(t, err)
	err = conn.writeFieldList(r.Fields, nil)
	require.NoError(t, err)

	// column length 1
	require.Equal(t, []byte{23, 0, 0, 0, 3, 100, 101, 102, 0, 0, 0, 1, 'c', 0, 12, 33, 0, 0, 0, 0, 0, 253, 0, 0, 0, 0, 0}, clientConn.WriteBuffered[:27])
	require.Equal(t, []byte{1, 0, 0, 1, mysql.EOF_HEADER}, clientConn.WriteBuffered[27:])
}

func TestConnWriteFieldValues(t *testing.T) {
	clientConn := &mockconn.MockConn{MultiWrite: true}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	r, err := mysql.BuildSimpleTextResultset([]string{"c"}, [][]interface{}{
		{"d"},
		{nil},
	})

	require.NoError(t, err)
	err = conn.writeFieldList(r.Fields, nil)
	require.NoError(t, err)

	// fields and EOF
	require.Equal(t, []byte{23, 0, 0, 0, 3, 100, 101, 102, 0, 0, 0, 1, 'c', 0, 12, 33, 0, 0, 0, 0, 0, 253, 0, 0, 0, 0, 0}, clientConn.WriteBuffered[:27])
	require.Equal(t, []byte{1, 0, 0, 1, mysql.EOF_HEADER}, clientConn.WriteBuffered[27:32])

	r.Values = make([][]mysql.FieldValue, len(r.RowDatas))
	for i := range r.Values {
		r.Values[i], err = r.RowDatas[i].Parse(r.Fields, false, r.Values[i])

		require.NoError(t, err)

		err = conn.writeFieldValues(r.Values[i])
		require.NoError(t, err)
	}

	err = conn.writeEOF()
	require.NoError(t, err)

	// first row
	require.Equal(t, []byte{2, 0, 0, 2, 1, 'd'}, clientConn.WriteBuffered[32:38])

	// second row with NULL value
	require.Equal(t, []byte{1, 0, 0, 3, 251}, clientConn.WriteBuffered[38:43])

	// EOF
	require.Equal(t, []byte{1, 0, 0, 4, mysql.EOF_HEADER}, clientConn.WriteBuffered[43:])
}
