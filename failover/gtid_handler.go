package failover

import (
	"fmt"
	"github.com/siddontang/go-mysql/mysql"
)

type GTIDHandler struct {
	Handler
}

func (h *GTIDHandler) Promote(s *Server) error {
	if err := h.WaitRelayLogDone(s); err != nil {
		return err
	}

	if err := s.StopSlave(); err != nil {
		return err
	}

	return nil
}

func (h *GTIDHandler) Compare(s1 *Server, s2 *Server) (int, error) {
	set1, err := h.readExecutedGTIDSet(s1)
	if err != nil {
		return 0, err
	}

	set2, err := h.readExecutedGTIDSet(s2)
	if err != nil {
		return 0, err
	}

	// s1 and s2 has no data replicated from master
	if set1 == nil && set2 == nil {
		return 0, nil
	} else if set1 == nil {
		return -1, nil
	} else if set2 == nil {
		return 1, nil
	}

	if set1.SID.String() != set2.SID.String() {
		return 0, fmt.Errorf("%s, %s have different master", s1.addr, s2.addr)
	}

	if set1.Intervals.Equal(set2.Intervals) {
		return 0, nil
	} else if set1.Intervals.Subset(set2.Intervals) {
		return 1, nil
	} else {
		return -1, nil
	}
}

const changeMasterToWithAuto = `CHANGE MASTER TO 
    MASTER_HOST = %s, MASTER_PORT = %s, 
    MASTER_USER = %s, MASTER_PASSWORD = %s, 
    MASTER_AUTO_POSITION = 1`

func (h *GTIDHandler) ChangeMasterTo(s *Server, m *Server) error {
	if err := h.WaitRelayLogDone(s); err != nil {
		return err
	}

	if err := s.StopSlave(); err != nil {
		return err
	}

	if err := s.ResetSlave(); err != nil {
		return err
	}

	if _, err := s.Execute(fmt.Sprintf(changeMasterToWithAuto, m.host, m.port, m.replUser.Name, m.replUser.Password)); err != nil {
		return err
	}

	if err := s.StartSlave(); err != nil {
		return err
	}

	return nil
}

func (h *GTIDHandler) WaitRelayLogDone(s *Server) error {
	if err := s.StopSlaveIOThread(); err != nil {
		return err
	}

	r, err := s.SlaveStatus()
	if err != nil {
		return err
	}

	retrieved, _ := r.GetStringByName(0, "Retrieved_Gtid_Set")

	// may only support MySQL version >= 5.6.9
	// see http://dev.mysql.com/doc/refman/5.6/en/gtid-functions.html
	if _, err := s.Execute(fmt.Sprintf("SELECT WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS(%s)", retrieved)); err != nil {
		return err
	}

	return nil
}

func (h *GTIDHandler) readExecutedGTIDSet(s *Server) (*mysql.UUIDSet, error) {
	r, err := s.SlaveStatus()
	if err != nil {
		return nil, err
	}

	masterUUID, _ := r.GetStringByName(0, "Master_UUID")
	executed, _ := r.GetStringByName(0, "Executed_Gtid_Set")

	g, err := mysql.ParseGTIDSet(executed)
	if err != nil {
		return nil, err
	}

	set, _ := g.Sets[masterUUID]
	return set, nil
}
