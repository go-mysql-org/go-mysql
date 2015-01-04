package failover

import (
	"github.com/siddontang/go-mysql/client"
	. "github.com/siddontang/go-mysql/mysql"
)

type Server struct {
	addr     string
	user     string
	password string

	conn *client.Conn
}

func (s *Server) Close() {
	if s.conn != nil {
		s.conn.Close()
	}
}

func (s *Server) Execute(cmd string, args ...interface{}) (r *Result, err error) {
	retryNum := 3
	for i := 0; i < retryNum; i++ {
		if s.conn != nil {
			s.conn, err = client.Connect(s.addr, s.user, s.password, "")
			if err != nil {
				return nil, err
			}
		}

		r, err = s.conn.Execute(cmd, args...)
		if err != nil && err != ErrBadConn {
			return
		} else if err == ErrBadConn {
			s.conn = nil
			continue
		} else {
			return
		}
	}
	return
}
