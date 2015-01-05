package failover

import (
	"fmt"
)

type GTIDSlave struct {
	*Server
}

func NewGTIDSlave(addr string, user User, replUser User) *GTIDSlave {
	s := new(GTIDSlave)

	s.Server = NewServer(addr, user, replUser)

	return s
}

func (s *GTIDSlave) Promote() (*Master, error) {
	if err := s.waitRelayLogDone(); err != nil {
		return nil, err
	}

	if err := s.StopSlave(); err != nil {
		return nil, err
	}

	m := new(Master)
	m.Server, s.Server = s.Server, nil

	return m, nil
}

func (s *GTIDSlave) Compare(o *GTIDSlave) (int, error) {
	return 0, nil
}

const changeMasterToWithAuto = `CHANGE MASTER TO 
    MASTER_HOST = %s, MASTER_PORT = %s, 
    MASTER_USER = %s, MASTER_PASSWORD = %s, 
    MASTER_AUTO_POSITION = 1`

func (s *GTIDSlave) ChangeMasterTo(m *Master) error {
	if err := s.waitRelayLogDone(); err != nil {
		return err
	}

	if err := s.StopSlave(); err != nil {
		return err
	}

	if _, err := s.Execute(fmt.Sprintf(changeMasterToWithAuto, s.host, s.port, s.replUser.Name, s.replUser.Password)); err != nil {
		return err
	}

	if err := s.StartSlave(); err != nil {
		return err
	}

	return nil
}

func (s *GTIDSlave) waitRelayLogDone() error {
	r, err := s.SlaveStatus()
	if err != nil {
		return err
	}

	retrieved, _ := r.GetStringByName(0, "Retrieved_Gtid_Set")

	if _, err := s.Execute(fmt.Sprintf("SELECT WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS(%s)", retrieved)); err != nil {
		return err
	}

	return nil
}
