package failover

import (
	. "github.com/siddontang/go-mysql/mysql"
)

type Master struct {
	*Server
}

func NewMaster(addr string, user User, replUser User) *Master {
	m := new(Master)

	m.Server = NewServer(addr, user, replUser)

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
