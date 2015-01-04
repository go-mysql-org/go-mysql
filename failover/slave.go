package failover

import (
	. "github.com/siddontang/go-mysql/mysql"
)

type Slave struct {
	Server
}

func NewSlave(addr string, user string, password string) *Slave {
	s := new(Slave)

	s.addr = addr
	s.user = user
	s.password = password

	return s
}

func (s *Slave) Close() {
	s.Server.Close()
}

func (s *Slave) Status() (*Resultset, error) {
	r, err := s.Execute("SHOW SLAVE STATUS")
	if err != nil {
		return nil, err
	} else {
		return r.Resultset, nil
	}
}
