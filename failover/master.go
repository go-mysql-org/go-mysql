package failover

import (
	. "github.com/siddontang/go-mysql/mysql"
)

type Master struct {
	Server

	ReplUser     string
	ReplPassword string
}

func NewMaster(addr string, user string, password string) *Master {
	m := new(Master)

	m.addr = addr
	m.user = user
	m.password = password

	return m
}

func (s *Master) SetReplUser(user string, password string) {
	s.ReplUser = user
	s.ReplPassword = password
}

func (s *Master) Status() (*Resultset, error) {
	r, err := s.Execute("SHOW MASTER STATUS")
	if err != nil {
		return nil, err
	} else {
		return r.Resultset, nil
	}
}
