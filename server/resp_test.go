package server

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"slices"
	"testing"
	"time"

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
	r, err = mysql.BuildSimpleTextResultset([]string{"a"}, [][]any{{"b"}})
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

	r, err := mysql.BuildSimpleTextResultset([]string{"c"}, [][]any{{"d"}})
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

	r, err := mysql.BuildSimpleTextResultset([]string{"c"}, [][]any{
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

	// write stream resultset should write the error packet
	// but leave the connection usable
	err := conn.WriteValue(sr.AsResult())
	require.NoError(t, err)
	require.Contains(t, clientConn.WriteBuffered, byte(mysql.ERR_HEADER))
	require.Contains(t, string(clientConn.WriteBuffered), testErr.Error())
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
	found := slices.Contains(clientConn.WriteBuffered, 0xfb)
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

// extractBinaryRowPackets parses the test write buffer and returns packets
// starting with the 0x00 binary-row header. The streaming path never emits
// an OK packet (also 0x00) inline, so the marker is unambiguous here.
func extractBinaryRowPackets(data []byte) [][]byte {
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
	return rowPackets
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

	require.NoError(t, conn.WriteValue(sr.AsResult()))

	rowPackets := extractBinaryRowPackets(clientConn.WriteBuffered)
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

	require.NoError(t, conn.WriteValue(sr.AsResult()))

	rowPackets := extractBinaryRowPackets(clientConn.WriteBuffered)
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

// TestConnWriteStreamResultsetBinaryNarrowIntegers verifies the streaming
// binary writer respects the declared column width for fixed-width integers
// (TINY/SHORT/INT24/LONG/LONGLONG, signed and unsigned). Pre-fix, the writer
// always emitted 8 bytes regardless of declared type.
func TestConnWriteStreamResultsetBinaryNarrowIntegers(t *testing.T) {
	clientConn := &mockconn.MockConn{MultiWrite: true}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	fields := []*mysql.Field{
		{Name: []byte("c_tiny"), Type: mysql.MYSQL_TYPE_TINY},
		{Name: []byte("c_utiny"), Type: mysql.MYSQL_TYPE_TINY, Flag: mysql.UNSIGNED_FLAG},
		{Name: []byte("c_short"), Type: mysql.MYSQL_TYPE_SHORT},
		{Name: []byte("c_year"), Type: mysql.MYSQL_TYPE_YEAR, Flag: mysql.UNSIGNED_FLAG},
		{Name: []byte("c_int24"), Type: mysql.MYSQL_TYPE_INT24},
		{Name: []byte("c_long"), Type: mysql.MYSQL_TYPE_LONG},
		{Name: []byte("c_longlong"), Type: mysql.MYSQL_TYPE_LONGLONG},
	}
	sr := mysql.NewStreamResult(fields, 1, true)

	go func() {
		defer sr.Close()
		ctx := context.Background()
		sr.WriteRow(ctx, []any{
			int8(-7), uint8(200),
			int16(-12345), uint16(2026),
			int32(-1234567), int32(2147483600),
			int64(9223372036854775000),
		})
	}()

	require.NoError(t, conn.WriteValue(sr.AsResult()))

	rowPackets := extractBinaryRowPackets(clientConn.WriteBuffered)
	require.Len(t, rowPackets, 1)

	row, err := mysql.RowData(rowPackets[0]).ParseBinary(fields, nil)
	require.NoError(t, err)
	require.Equal(t, int64(-7), row[0].AsInt64())
	require.Equal(t, uint64(200), row[1].AsUint64())
	require.Equal(t, int64(-12345), row[2].AsInt64())
	require.Equal(t, uint64(2026), row[3].AsUint64())
	require.Equal(t, int64(-1234567), row[4].AsInt64())
	require.Equal(t, int64(2147483600), row[5].AsInt64())
	require.Equal(t, int64(9223372036854775000), row[6].AsInt64())
}

// TestConnWriteStreamResultsetBinaryFloats verifies FLOAT is encoded as
// 4 bytes of Float32bits (not truncated Float64bits) and DOUBLE as 8 bytes
// of Float64bits.
func TestConnWriteStreamResultsetBinaryFloats(t *testing.T) {
	clientConn := &mockconn.MockConn{MultiWrite: true}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	fields := []*mysql.Field{
		{Name: []byte("c_float"), Type: mysql.MYSQL_TYPE_FLOAT},
		{Name: []byte("c_double"), Type: mysql.MYSQL_TYPE_DOUBLE},
	}
	sr := mysql.NewStreamResult(fields, 1, true)

	go func() {
		defer sr.Close()
		sr.WriteRow(context.Background(), []any{float32(3.5), float64(2.718281828459045)})
	}()

	require.NoError(t, conn.WriteValue(sr.AsResult()))

	rowPackets := extractBinaryRowPackets(clientConn.WriteBuffered)
	require.Len(t, rowPackets, 1)

	row, err := mysql.RowData(rowPackets[0]).ParseBinary(fields, nil)
	require.NoError(t, err)
	require.InDelta(t, 3.5, row[0].AsFloat64(), 1e-6)
	require.InDelta(t, 2.718281828459045, row[1].AsFloat64(), 1e-12)
}

// TestConnWriteStreamResultsetBinaryLengthEncodedStrings verifies that
// length-encoded variable-width types beyond MYSQL_TYPE_VAR_STRING (VARCHAR,
// STRING, BLOB, DECIMAL, JSON) are length-prefixed on the wire.
func TestConnWriteStreamResultsetBinaryLengthEncodedStrings(t *testing.T) {
	clientConn := &mockconn.MockConn{MultiWrite: true}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	fields := []*mysql.Field{
		{Name: []byte("c_varchar"), Type: mysql.MYSQL_TYPE_VARCHAR},
		{Name: []byte("c_string"), Type: mysql.MYSQL_TYPE_STRING},
		{Name: []byte("c_blob"), Type: mysql.MYSQL_TYPE_BLOB},
		{Name: []byte("c_decimal"), Type: mysql.MYSQL_TYPE_NEWDECIMAL},
		{Name: []byte("c_json"), Type: mysql.MYSQL_TYPE_JSON},
	}
	sr := mysql.NewStreamResult(fields, 1, true)

	go func() {
		defer sr.Close()
		sr.WriteRow(context.Background(), []any{
			"abc",
			"hello",
			[]byte{0x00, 0x01, 0x02},
			"1234.5678",
			[]byte(`{"k":"v"}`),
		})
	}()

	require.NoError(t, conn.WriteValue(sr.AsResult()))

	rowPackets := extractBinaryRowPackets(clientConn.WriteBuffered)
	require.Len(t, rowPackets, 1)

	row, err := mysql.RowData(rowPackets[0]).ParseBinary(fields, nil)
	require.NoError(t, err)
	require.Equal(t, "abc", string(row[0].AsString()))
	require.Equal(t, "hello", string(row[1].AsString()))
	require.Equal(t, []byte{0x00, 0x01, 0x02}, row[2].AsString())
	require.Equal(t, "1234.5678", string(row[3].AsString()))
	require.Equal(t, `{"k":"v"}`, string(row[4].AsString()))
}

// TestConnWriteStreamResultsetBinaryTemporals verifies that DATE / DATETIME /
// TIMESTAMP / TIME columns receive a length-prefixed packed binary
// representation when written from a time.Time value.
func TestConnWriteStreamResultsetBinaryTemporals(t *testing.T) {
	clientConn := &mockconn.MockConn{MultiWrite: true}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	fields := []*mysql.Field{
		{Name: []byte("c_date"), Type: mysql.MYSQL_TYPE_DATE},
		{Name: []byte("c_datetime"), Type: mysql.MYSQL_TYPE_DATETIME},
		{Name: []byte("c_timestamp"), Type: mysql.MYSQL_TYPE_TIMESTAMP},
	}
	sr := mysql.NewStreamResult(fields, 1, true)

	dt := time.Date(2026, time.April, 28, 9, 30, 15, 0, time.UTC)
	go func() {
		defer sr.Close()
		sr.WriteRow(context.Background(), []any{dt, dt, dt})
	}()

	require.NoError(t, conn.WriteValue(sr.AsResult()))

	rowPackets := extractBinaryRowPackets(clientConn.WriteBuffered)
	require.Len(t, rowPackets, 1)

	row, err := mysql.RowData(rowPackets[0]).ParseBinary(fields, nil)
	require.NoError(t, err)
	require.Equal(t, "2026-04-28", string(row[0].AsString()))
	require.Equal(t, "2026-04-28 09:30:15", string(row[1].AsString()))
	require.Equal(t, "2026-04-28 09:30:15", string(row[2].AsString()))
}

// TestConnWriteStreamResultsetBinaryTime verifies the streaming binary writer
// emits a length-prefixed packed TIME payload with no embedded date.
func TestConnWriteStreamResultsetBinaryTime(t *testing.T) {
	clientConn := &mockconn.MockConn{MultiWrite: true}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	fields := []*mysql.Field{
		{Name: []byte("c_time"), Type: mysql.MYSQL_TYPE_TIME},
	}
	sr := mysql.NewStreamResult(fields, 1, true)

	tm := time.Date(0, 1, 1, 12, 34, 56, 0, time.UTC)
	go func() {
		defer sr.Close()
		sr.WriteRow(context.Background(), []any{tm})
	}()

	require.NoError(t, conn.WriteValue(sr.AsResult()))

	rowPackets := extractBinaryRowPackets(clientConn.WriteBuffered)
	require.Len(t, rowPackets, 1)

	row, err := mysql.RowData(rowPackets[0]).ParseBinary(fields, nil)
	require.NoError(t, err)
	require.Equal(t, "12:34:56", string(row[0].AsString()))
}
