package failover

import (
	. "github.com/siddontang/go-mysql/mysql"
)

type Master struct {
	Server
}

func NewMaster(addr string, user string, password string) *Master {
	m := new(Master)

	m.addr = addr
	m.user = user
	m.password = password

	return m
}

func (s *Master) Status() (*Resultset, error) {
	r, err := s.Execute("SHOW MASTER STATUS")
	if err != nil {
		return nil, err
	} else {
		return r.Resultset, nil
	}
}
