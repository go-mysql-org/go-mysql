package server

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
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

	result := mysql.NewResultReserveResultset(0)
	result.AffectedRows = 1
	result.InsertId = 2

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
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	conn := &Conn{
		Conn: packet.NewConn(clientConn),
		serverConf: &Server{
			rsaPrivateKey:     rsaKey,
			rsaPublicKeyBytes: rsaPublicKeyBytes(rsaKey),
		},
	}

	err = conn.writeAuthMoreDataPubkey()
	require.NoError(t, err)
	// Check that the packet starts with the expected header
	require.Equal(t, mysql.MORE_DATE_HEADER, clientConn.WriteBuffered[4])
	// Check that the public key PEM is included (starts after the 5-byte header)
	require.Contains(t, string(clientConn.WriteBuffered[5:]), "BEGIN PUBLIC KEY")
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

func TestWriteValue(t *testing.T) {
	clientConn := &mockconn.MockConn{MultiWrite: true}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	// simple OK
	err := conn.WriteValue(mysql.NewResultReserveResultset(0))
	require.NoError(t, err)
	expected := []byte{3, 0, 0, 0, mysql.OK_HEADER, 0, 0}
	require.Equal(t, expected, clientConn.WriteBuffered)

	// reset write buffer
	clientConn.WriteBuffered = []byte{}

	// resultset with no rows
	rs := mysql.NewResultReserveResultset(1)
	rs.Fields = []*mysql.Field{{Name: []byte("a")}}
	err = conn.WriteValue(rs)
	require.NoError(t, err)
	expected = []byte{1, 0, 0, 1, mysql.MORE_DATE_HEADER}
	require.Equal(t, expected, clientConn.WriteBuffered[:5])

	// reset write buffer
	clientConn.WriteBuffered = []byte{}

	// resultset with rows
	rs.Fields = []*mysql.Field{{Name: []byte("a")}}
	rs.RowDatas = []mysql.RowData{[]byte{1, 2, 3}}
	err = conn.WriteValue(rs)
	require.NoError(t, err)
	expected = []byte{1, 0, 0, 5, mysql.MORE_DATE_HEADER}
	require.Equal(t, expected, clientConn.WriteBuffered[:5])
}

// makeFields creates Field slice from column names for testing
func makeFields(names ...string) []*mysql.Field {
	fields := make([]*mysql.Field, len(names))
	for i, name := range names {
		fields[i] = &mysql.Field{
			Name: []byte(name),
			Type: mysql.MYSQL_TYPE_VAR_STRING,
		}
	}
	return fields
}

func TestStreamResultBasic(t *testing.T) {
	// test NewStreamResult with fields and buffer size
	sr := mysql.NewStreamResult(makeFields("col1", "col2"), 100, false)
	require.NotNil(t, sr)
	require.Len(t, sr.Fields, 2)
	require.Equal(t, []byte("col1"), sr.Fields[0].Name)
	require.Equal(t, []byte("col2"), sr.Fields[1].Name)
	require.False(t, sr.IsClosed())

	// test NewStreamResult with custom buffer size
	sr2 := mysql.NewStreamResult(makeFields("a"), 50, false)
	require.NotNil(t, sr2)
	require.Len(t, sr2.Fields, 1)

	// test AsResult
	result := sr.AsResult()
	require.NotNil(t, result)
	require.True(t, result.IsStreaming())
	require.Equal(t, sr, result.StreamResult)
}

func TestStreamResultWriteAndRead(t *testing.T) {
	sr := mysql.NewStreamResult(makeFields("id", "name"), 10, false)
	ctx := context.Background()

	// write rows in a goroutine
	go func() {
		defer sr.Close()
		ok := sr.WriteRow(ctx, []any{1, "alice"})
		require.True(t, ok)
		ok = sr.WriteRow(ctx, []any{2, "bob"})
		require.True(t, ok)
		ok = sr.WriteRow(ctx, []any{nil, "charlie"})
		require.True(t, ok)
	}()

	// read rows from channel
	var rows [][]any
	for row := range sr.RowsChan() {
		rows = append(rows, row)
	}

	require.Len(t, rows, 3)
	require.Equal(t, []any{1, "alice"}, rows[0])
	require.Equal(t, []any{2, "bob"}, rows[1])
	require.Equal(t, []any{nil, "charlie"}, rows[2])
	require.True(t, sr.IsClosed())
}

func TestStreamResultError(t *testing.T) {
	sr := mysql.NewStreamResult(makeFields("a"), 100, false)

	// initially no error
	require.NoError(t, sr.Err())

	// set an error
	testErr := errors.New("test error")
	sr.SetError(testErr)
	require.Equal(t, testErr, sr.Err())

	// error persists
	require.Equal(t, testErr, sr.Err())
}

func TestConnWriteStreamResultset(t *testing.T) {
	clientConn := &mockconn.MockConn{MultiWrite: true}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	sr := mysql.NewStreamResult(makeFields("col1"), 10, false)

	// write rows and close in a goroutine
	go func() {
		sr.WriteRow(context.Background(), []any{"value1"})
		sr.WriteRow(context.Background(), []any{"value2"})
		sr.Close()
	}()

	// write stream resultset
	err := conn.WriteValue(sr.AsResult())
	require.NoError(t, err)

	// verify the output contains expected data
	// column count (1)
	require.Equal(t, byte(1), clientConn.WriteBuffered[4])
	// last bytes should be EOF
	require.Equal(t, mysql.EOF_HEADER, clientConn.WriteBuffered[len(clientConn.WriteBuffered)-1])
}

func TestConnWriteStreamResultsetWithError(t *testing.T) {
	clientConn := &mockconn.MockConn{MultiWrite: true}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	sr := mysql.NewStreamResult(makeFields("col1"), 10, false)
	testErr := errors.New("stream error")

	// write rows, set error, and close in a goroutine
	go func() {
		sr.WriteRow(context.Background(), []any{"value1"})
		sr.SetError(testErr)
		sr.Close()
	}()

	// write stream resultset should return the error
	err := conn.WriteValue(sr.AsResult())
	require.Error(t, err)
	require.Equal(t, testErr, err)
}

func TestConnWriteStreamResultsetWithNullValue(t *testing.T) {
	clientConn := &mockconn.MockConn{MultiWrite: true}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	sr := mysql.NewStreamResult(makeFields("col1", "col2"), 10, false)

	// write rows with NULL values
	go func() {
		sr.WriteRow(context.Background(), []any{nil, "value"})
		sr.WriteRow(context.Background(), []any{"data", nil})
		sr.Close()
	}()

	err := conn.WriteValue(sr.AsResult())
	require.NoError(t, err)

	// verify output contains NULL marker (0xfb)
	found := false
	for _, b := range clientConn.WriteBuffered {
		if b == 0xfb {
			found = true
			break
		}
	}
	require.True(t, found, "NULL marker (0xfb) should be present in output")
}

func TestStreamResultEmptyResult(t *testing.T) {
	clientConn := &mockconn.MockConn{MultiWrite: true}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	sr := mysql.NewStreamResult(makeFields("col1"), 10, false)

	// close immediately without writing any rows
	go func() {
		sr.Close()
	}()

	err := conn.WriteValue(sr.AsResult())
	require.NoError(t, err)

	// should still have valid output with column info and EOF
	require.True(t, len(clientConn.WriteBuffered) > 0)
}

// TestConnWriteStreamResultsetBinary tests binary protocol streaming with data parsing.
func TestConnWriteStreamResultsetBinary(t *testing.T) {
	clientConn := &mockconn.MockConn{MultiWrite: true}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	// Create fields with proper types for binary protocol
	fields := []*mysql.Field{
		{Name: []byte("id"), Type: mysql.MYSQL_TYPE_LONGLONG},
		{Name: []byte("name"), Type: mysql.MYSQL_TYPE_VAR_STRING},
	}
	sr := mysql.NewStreamResult(fields, 10, true)

	go func() {
		defer sr.Close()
		ctx := context.Background()
		sr.WriteRow(ctx, []any{int64(1), "alice"})
		sr.WriteRow(ctx, []any{int64(2), "bob"})
	}()

	err := conn.WriteValue(sr.AsResult())
	require.NoError(t, err)

	// Parse packets from the buffer
	data := clientConn.WriteBuffered
	var rowPackets [][]byte
	pos := 0
	for pos < len(data) {
		if pos+4 > len(data) {
			break
		}
		// Packet header: 3 bytes length + 1 byte sequence
		pktLen := int(uint32(data[pos]) | uint32(data[pos+1])<<8 | uint32(data[pos+2])<<16)
		pos += 4
		if pos+pktLen > len(data) {
			break
		}
		pktData := data[pos : pos+pktLen]
		pos += pktLen

		// Binary row packets start with 0x00 header
		if len(pktData) > 0 && pktData[0] == 0x00 {
			rowPackets = append(rowPackets, pktData)
		}
	}

	require.Len(t, rowPackets, 2, "Should have 2 binary row packets")

	// Parse first row
	row1, err := mysql.RowData(rowPackets[0]).ParseBinary(fields, nil)
	require.NoError(t, err)
	require.Len(t, row1, 2)
	require.Equal(t, int64(1), row1[0].AsInt64())
	require.Equal(t, "alice", string(row1[1].AsString()))

	// Parse second row
	row2, err := mysql.RowData(rowPackets[1]).ParseBinary(fields, nil)
	require.NoError(t, err)
	require.Len(t, row2, 2)
	require.Equal(t, int64(2), row2[0].AsInt64())
	require.Equal(t, "bob", string(row2[1].AsString()))
}

// TestConnWriteStreamResultsetBinaryWithNull tests binary protocol with NULL values and parsing.
func TestConnWriteStreamResultsetBinaryWithNull(t *testing.T) {
	clientConn := &mockconn.MockConn{MultiWrite: true}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	fields := []*mysql.Field{
		{Name: []byte("col1"), Type: mysql.MYSQL_TYPE_VAR_STRING},
		{Name: []byte("col2"), Type: mysql.MYSQL_TYPE_VAR_STRING},
	}
	sr := mysql.NewStreamResult(fields, 10, true)

	go func() {
		defer sr.Close()
		ctx := context.Background()
		sr.WriteRow(ctx, []any{nil, "value"})
		sr.WriteRow(ctx, []any{"data", nil})
	}()

	err := conn.WriteValue(sr.AsResult())
	require.NoError(t, err)

	// Parse packets
	data := clientConn.WriteBuffered
	var rowPackets [][]byte
	pos := 0
	for pos < len(data) {
		if pos+4 > len(data) {
			break
		}
		pktLen := int(uint32(data[pos]) | uint32(data[pos+1])<<8 | uint32(data[pos+2])<<16)
		pos += 4
		if pos+pktLen > len(data) {
			break
		}
		pktData := data[pos : pos+pktLen]
		pos += pktLen

		if len(pktData) > 0 && pktData[0] == 0x00 {
			rowPackets = append(rowPackets, pktData)
		}
	}

	require.Len(t, rowPackets, 2)

	// Parse first row: NULL, "value"
	row1, err := mysql.RowData(rowPackets[0]).ParseBinary(fields, nil)
	require.NoError(t, err)
	require.Nil(t, row1[0].Value()) // NULL
	require.Equal(t, "value", string(row1[1].AsString()))

	// Parse second row: "data", NULL
	row2, err := mysql.RowData(rowPackets[1]).ParseBinary(fields, nil)
	require.NoError(t, err)
	require.Equal(t, "data", string(row2[0].AsString()))
	require.Nil(t, row2[1].Value()) // NULL
}
