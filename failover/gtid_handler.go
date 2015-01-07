package failover

import (
	"fmt"
	"github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/mysql"
	"net"
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

	if !uuid.Equal(set1.SID, set2.SID) {
		return 0, fmt.Errorf("%s, %s have different master", s1.Addr, s2.Addr)
	}

	return set1.Intervals.Compare(set2.Intervals), nil
}

func (h *GTIDHandler) FindBestSlaves(slaves []*Server) ([]*Server, error) {
	bestSlaves := []*Server{}

	sets := make([]*mysql.UUIDSet, len(slaves))

	var lastSet *mysql.UUIDSet = nil
	for i, slave := range slaves {
		set, err := h.readExecutedGTIDSet(slave)
		if err != nil {
			return nil, err
		}

		sets[i] = set

		if lastSet == nil {
			lastSet = set
			bestSlaves = []*Server{slave}
		} else if !uuid.Equal(lastSet.SID, set.SID) {
			return nil, fmt.Errorf("%s, %s have different master", slaves[0].Addr, slave.Addr)
		} else {
			switch lastSet.Intervals.Compare(set.Intervals) {
			case 1:
				//do nothing
			case -1:
				lastSet = set
				bestSlaves = []*Server{slave}
			case 0:
				// these two slaves have same data,
				bestSlaves = append(bestSlaves, slave)
			}
		}
	}

	return bestSlaves, nil
}

const changeMasterToWithAuto = `CHANGE MASTER TO 
    MASTER_HOST = "%s", MASTER_PORT = %s, 
    MASTER_USER = "%s", MASTER_PASSWORD = "%s", 
    MASTER_AUTO_POSITION = 1`

func (h *GTIDHandler) ChangeMasterTo(s *Server, m *Server) error {
	if err := h.WaitRelayLogDone(s); err != nil {
		return err
	}

	if err := s.StopSlave(); err != nil {
		return err
	}

	// if err := s.ResetSlave(); err != nil {
	// 	return err
	// }

	host, port, _ := net.SplitHostPort(m.Addr)

	if _, err := s.Execute(fmt.Sprintf(changeMasterToWithAuto,
		host, port, m.ReplUser.Name, m.ReplUser.Password)); err != nil {
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
	return h.waitUntilAfterGTIDs(s, retrieved)
}

func (h *GTIDHandler) WaitCatchMaster(s *Server, m *Server) error {
	r, err := m.MasterStatus()
	if err != nil {
		return err
	}

	masterGTIDSet, _ := r.GetStringByName(0, "Executed_Gtid_Set")

	return h.waitUntilAfterGTIDs(s, masterGTIDSet)
}

func (h *GTIDHandler) waitUntilAfterGTIDs(s *Server, gtids string) error {
	_, err := s.Execute(fmt.Sprintf("SELECT WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS('%s')", gtids))
	return err
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

	set, ok := g.Sets[masterUUID]
	if ok {
		return set, nil
	} else {
		u, _ := uuid.FromString(masterUUID)
		return &mysql.UUIDSet{u, mysql.IntervalSlice{}}, nil
	}
}
