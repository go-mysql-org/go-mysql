package failover

import (
	"fmt"
	. "github.com/siddontang/go-mysql/mysql"
)

type PseudoGTIDHandler struct {
	Handler
}

// Promote to master, you must not use this slave after Promote
func (h *PseudoGTIDHandler) Promote(s *Server) error {
	if err := h.WaitRelayLogDone(s); err != nil {
		return err
	}

	if err := s.StopSlave(); err != nil {
		return err
	}

	// todo.....

	return nil
}

const changeMasterToWithPos = `CHANGE MASTER TO
    MASTER_HOST="%s", MASTER_PORT=%s,
    MASTER_USER="%s", MASTER_PASSWORD="%s",
    MASTER_LOG_FILE="%s", MASTER_LOG_POS=%d`

func (h *PseudoGTIDHandler) ChangeMasterTo(s *Server, m *Server) error {
	// Wait all relay logs done with last master
	if err := h.WaitRelayLogDone(s); err != nil {
		return err
	}

	// Stop slave
	if err := s.StopSlave(); err != nil {
		return err
	}

	// Reset slave
	if err := s.ResetSlave(); err != nil {
		return err
	}

	// Change master to with position

	// Start slave
	if err := s.StartSlave(); err != nil {
		return err
	}

	return nil
}

func (h *PseudoGTIDHandler) WaitRelayLogDone(s *Server) error {
	if err := s.StopSlaveIOThread(); err != nil {
		return err
	}

	pos, err := h.fetchReadPos(s)
	if err != nil {
		return err
	}

	if err = h.waitUntilPosition(s, pos); err != nil {
		return err
	}

	return nil
}

func (h *PseudoGTIDHandler) Compare(s1 *Server, s2 *Server) (int, error) {
	// todo, check same master

	p1, err := h.fetchReadPos(s1)
	if err != nil {
		return 0, err
	}

	p2, err := h.fetchReadPos(s2)
	if err != nil {
		return 0, err
	}

	return p1.Compare(p2), nil
}

func (h *PseudoGTIDHandler) FindBestSlaves(slaves []*Server) ([]*Server, error) {
	// todo, check same master

	bestSlaves := []*Server{}

	ps := make([]Position, len(slaves))

	lastIndex := -1

	for i, slave := range slaves {
		pos, err := h.fetchReadPos(slave)
		if err != nil {
			return nil, err
		}

		ps[i] = pos

		if lastIndex == -1 {
			lastIndex = i
			bestSlaves = []*Server{slave}
		} else {
			switch ps[lastIndex].Compare(pos) {
			case 1:
				//do nothing
			case -1:
				lastIndex = i
				bestSlaves = []*Server{slave}
			case 0:
				// these two slaves have same data,
				bestSlaves = append(bestSlaves, slave)
			}
		}
	}

	return bestSlaves, nil
}

func (h *PseudoGTIDHandler) WaitCatchMaster(s *Server, m *Server) error {
	r, err := m.MasterStatus()
	if err != nil {
		return err
	}

	fname, _ := r.GetStringByName(0, "File")
	pos, _ := r.GetIntByName(0, "Position")

	return h.waitUntilPosition(s, Position{fname, uint32(pos)})
}

// Get current binlog filename and position read from master
func (h *PseudoGTIDHandler) fetchReadPos(s *Server) (Position, error) {
	r, err := s.SlaveStatus()
	if err != nil {
		return Position{}, err
	}

	fname, _ := r.GetStringByName(0, "Master_Log_File")
	pos, _ := r.GetIntByName(0, "Read_Master_Log_Pos")

	return Position{fname, uint32(pos)}, nil
}

func (h *PseudoGTIDHandler) waitUntilPosition(s *Server, pos Position) error {
	_, err := s.Execute(fmt.Sprintf("SELECT MASTER_POS_WAIT('%s', %s)", pos.Name, pos.Pos))
	return err
}
