package server

import (
	"errors"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/packet"
	mockconn "github.com/go-mysql-org/go-mysql/test_util/conn"
	"github.com/pingcap/check"
)

type respConnTestSuite struct{}

var _ = check.Suite(&respConnTestSuite{})

func (t *respConnTestSuite) TestConnWriteOK(c *check.C) {
	clientConn := &mockconn.MockConn{}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	result := &mysql.Result{
		AffectedRows: 1,
		InsertId:     2,
	}

	// write ok with insertid and affectedrows set
	err := conn.writeOK(result)
	c.Assert(err, check.IsNil)
	c.Assert(clientConn.WriteBuffered, check.BytesEquals, []byte{3, 0, 0, 0, mysql.OK_HEADER, 1, 2})

	// set capability for CLIENT_PROTOCOL_41
	conn.SetCapability(mysql.CLIENT_PROTOCOL_41)
	conn.SetStatus(mysql.SERVER_QUERY_WAS_SLOW)
	err = conn.writeOK(result)
	c.Assert(err, check.IsNil)
	c.Assert(clientConn.WriteBuffered, check.BytesEquals, []byte{7, 0, 0, 1, mysql.OK_HEADER, 1, 2, 0, 8, 0, 0})
}

func (t *respConnTestSuite) TestConnWriteEOF(c *check.C) {
	clientConn := &mockconn.MockConn{}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	// write regular EOF
	err := conn.writeEOF()
	c.Assert(err, check.IsNil)
	c.Assert(clientConn.WriteBuffered, check.BytesEquals, []byte{1, 0, 0, 0, mysql.EOF_HEADER})

	// set capability for CLIENT_PROTOCOL_41
	conn.SetCapability(mysql.CLIENT_PROTOCOL_41)
	conn.SetStatus(mysql.SERVER_MORE_RESULTS_EXISTS)
	err = conn.writeEOF()
	c.Assert(err, check.IsNil)
	c.Assert(clientConn.WriteBuffered, check.BytesEquals, []byte{5, 0, 0, 1, mysql.EOF_HEADER,
		0, 0, 8, 0})
}

func (t *respConnTestSuite) TestConnWriteError(c *check.C) {
	clientConn := &mockconn.MockConn{}
	conn := &Conn{Conn: packet.NewConn(clientConn)}
	merr := mysql.NewDefaultError(mysql.ER_YES) // nice and short error message

	// write regular Error
	err := conn.writeError(merr)
	c.Assert(err, check.IsNil)
	c.Assert(clientConn.WriteBuffered, check.BytesEquals, []byte{6, 0, 0, 0, mysql.ERR_HEADER,
		235, 3, 89, 69, 83})

	// set capability for CLIENT_PROTOCOL_41
	conn.SetCapability(mysql.CLIENT_PROTOCOL_41)
	err = conn.writeError(merr)
	c.Assert(err, check.IsNil)
	c.Assert(clientConn.WriteBuffered, check.BytesEquals, []byte{12, 0, 0, 1, mysql.ERR_HEADER,
		235, 3, 35, 72, 89, 48, 48, 48, 89, 69, 83})

	// unknown error
	err = conn.writeError(errors.New("test"))
	c.Assert(err, check.IsNil)
	c.Assert(clientConn.WriteBuffered, check.BytesEquals, []byte{13, 0, 0, 2, mysql.ERR_HEADER,
		81, 4, 35, 72, 89, 48, 48, 48, 116, 101, 115, 116})
}

func (t *respConnTestSuite) TestConnWriteAuthSwitchRequest(c *check.C) {
	clientConn := &mockconn.MockConn{}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	err := conn.writeAuthSwitchRequest("test")
	c.Assert(err, check.IsNil)
	// first 10 bytes are static, then there is a part random, ending with a \0
	c.Assert(clientConn.WriteBuffered[:10], check.BytesEquals, []byte{27, 0, 0, 0, mysql.EOF_HEADER,
		116, 101, 115, 116, 0})
	c.Assert(clientConn.WriteBuffered[len(clientConn.WriteBuffered)-1:], check.BytesEquals, []byte{0})
}

func (t *respConnTestSuite) TestConnReadAuthSwitchRequestResponse(c *check.C) {
	clientConn := &mockconn.MockConn{}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	// prepare response for \NUL
	clientConn.SetResponse([][]byte{{1, 0, 0, 0, 0}})
	data, err := conn.readAuthSwitchRequestResponse()
	c.Assert(err, check.IsNil)
	c.Assert(data, check.BytesEquals, []byte{})

	// prepare response for some auth switch data
	clientConn.SetResponse([][]byte{{4, 0, 0, 0, 1, 2, 3, 4}})
	conn = &Conn{Conn: packet.NewConn(clientConn)}

	data, err = conn.readAuthSwitchRequestResponse()
	c.Assert(err, check.IsNil)
	c.Assert(data, check.BytesEquals, []byte{1, 2, 3, 4})
}

func (t *respConnTestSuite) TestConnWriteAuthMoreDataPubkey(c *check.C) {
	clientConn := &mockconn.MockConn{}
	conn := &Conn{
		Conn: packet.NewConn(clientConn),
		serverConf: &Server{
			pubKey: []byte{1, 2, 3, 4},
		},
	}

	err := conn.writeAuthMoreDataPubkey()
	c.Assert(err, check.IsNil)
	c.Assert(clientConn.WriteBuffered, check.BytesEquals, []byte{5, 0, 0, 0, mysql.MORE_DATE_HEADER,
		1, 2, 3, 4})
}

func (t *respConnTestSuite) TestConnWriteAuthMoreDataFullAuth(c *check.C) {
	clientConn := &mockconn.MockConn{}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	err := conn.writeAuthMoreDataFullAuth()
	c.Assert(err, check.IsNil)
	c.Assert(clientConn.WriteBuffered, check.BytesEquals, []byte{2, 0, 0, 0, mysql.MORE_DATE_HEADER,
		mysql.CACHE_SHA2_FULL_AUTH})
}

func (t *respConnTestSuite) TestConnWriteAuthMoreDataFastAuth(c *check.C) {
	clientConn := &mockconn.MockConn{}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	err := conn.writeAuthMoreDataFastAuth()
	c.Assert(err, check.IsNil)
	c.Assert(clientConn.WriteBuffered, check.BytesEquals, []byte{2, 0, 0, 0, mysql.MORE_DATE_HEADER,
		mysql.CACHE_SHA2_FAST_AUTH})
}

func (t *respConnTestSuite) TestConnWriteResultset(c *check.C) {
	clientConn := &mockconn.MockConn{MultiWrite: true}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	r := mysql.NewResultset(0)

	// write minimalistic resultset
	err := conn.writeResultset(r)
	c.Assert(err, check.IsNil)
	// column length 0
	c.Assert(clientConn.WriteBuffered[:5], check.BytesEquals, []byte{1, 0, 0, 0, 0})
	// no fields and an EOF
	c.Assert(clientConn.WriteBuffered[5:10], check.BytesEquals, []byte{1, 0, 0, 1, mysql.EOF_HEADER})
	// no rows and another EOF
	c.Assert(clientConn.WriteBuffered[10:], check.BytesEquals, []byte{1, 0, 0, 2, mysql.EOF_HEADER})

	// reset write buffer and fill up the resultset with (little) data
	clientConn.WriteBuffered = []byte{}
	r, err = mysql.BuildSimpleTextResultset([]string{"a"}, [][]interface{}{{"b"}})
	c.Assert(err, check.IsNil)
	err = conn.writeResultset(r)
	c.Assert(err, check.IsNil)
	// column length 1
	c.Assert(clientConn.WriteBuffered[:5], check.BytesEquals, []byte{1, 0, 0, 3, 1})
	// fields and EOF
	c.Assert(clientConn.WriteBuffered[5:32], check.BytesEquals, []byte{23, 0, 0, 4, 3, 100, 101, 102, 0, 0, 0, 1, 'a', 0, 12, 33, 0, 0, 0, 0, 0, 253, 0, 0, 0, 0, 0})
	c.Assert(clientConn.WriteBuffered[32:37], check.BytesEquals, []byte{1, 0, 0, 5, mysql.EOF_HEADER})
	// rowdata and EOF
	c.Assert(clientConn.WriteBuffered[37:43], check.BytesEquals, []byte{2, 0, 0, 6, 1, 'b'})
	c.Assert(clientConn.WriteBuffered[43:], check.BytesEquals, []byte{1, 0, 0, 7, mysql.EOF_HEADER})
}

func (t *respConnTestSuite) TestConnWriteFieldList(c *check.C) {
	clientConn := &mockconn.MockConn{MultiWrite: true}
	conn := &Conn{Conn: packet.NewConn(clientConn)}

	r, err := mysql.BuildSimpleTextResultset([]string{"c"}, [][]interface{}{{"d"}})
	c.Assert(err, check.IsNil)
	err = conn.writeFieldList(r.Fields, nil)
	c.Assert(err, check.IsNil)

	// column length 1
	c.Assert(clientConn.WriteBuffered[:27], check.BytesEquals, []byte{23, 0, 0, 0, 3, 100, 101, 102, 0, 0, 0, 1, 'c', 0, 12, 33, 0, 0, 0, 0, 0, 253, 0, 0, 0, 0, 0})
	c.Assert(clientConn.WriteBuffered[27:], check.BytesEquals, []byte{1, 0, 0, 1, mysql.EOF_HEADER})
}
