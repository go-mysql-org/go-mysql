package failover

import (
	"fmt"
	. "github.com/siddontang/go-mysql/mysql"
)

type Slave struct {
	*Server
}

func NewSlave(addr string, user User, replUser User) *Slave {
	s := new(Slave)

	s.Server = NewServer(addr, user, replUser)

	return s
}

// Promote to master, you must not use this slave after Promote
func (s *Slave) Promote() (*Master, error) {
	if err := s.waitRelayLogDone(); err != nil {
		return nil, err
	}

	if err := s.StopSlave(); err != nil {
		return nil, err
	}

	// todo.....

	m := new(Master)
	m.Server, s.Server = s.Server, nil

	return m, nil
}

const changeMasterToWithPos = `CHANGE MASTER TO
    MASTER_HOST=%s, MASTER_PORT=%s,
    MASTER_USER=%s, MASTER_PASSWORD=%s,
    MASTER_LOG_FILE=%s, MASTER_LOG_POS=%s`

// Change to master m and replicate from it
func (s *Slave) ChangeMasterTo(m *Master) error {
	// First, wait all relay logs done with last master
	if err := s.waitRelayLogDone(); err != nil {
		return err
	}

	// Stop slave
	if err := s.StopSlave(); err != nil {
		return err
	}

	// Change master to with position

	// Start slave
	if err := s.StartSlave(); err != nil {
		return err
	}

	return nil
}

// Compare with slave o and decide which has more replicated data from master
// 1, s has more
// 0, equal
// -1, o has more
func (s *Slave) Compare(o *Slave) (int, error) {
	p1, err := s.fetchReadPos()
	if err != nil {
		return 0, err
	}

	p2, err := o.fetchReadPos()
	if err != nil {
		return 0, err
	}

	// First compare binlog name, format is xxx-bin.000000
	if p1.Name > p2.Name {
		return 1, nil
	} else if p1.Name < p2.Name {
		return -1, nil
	} else {
		// Same binlog file, compare position
		if p1.Pos > p2.Pos {
			return 1, nil
		} else if p1.Pos < p2.Pos {
			return -1, nil
		} else {
			return 0, nil
		}
	}
}

// Get current binlog filename and position read from master
func (s *Slave) fetchReadPos() (Position, error) {
	r, err := s.SlaveStatus()
	if err != nil {
		return Position{}, err
	}

	fname, _ := r.GetStringByName(0, "Master_Log_File")
	pos, _ := r.GetIntByName(0, "Read_Master_Log_Pos")

	return Position{fname, uint32(pos)}, nil
}

func (s *Slave) waitUntilPosition(pos Position) error {
	_, err := s.Execute(fmt.Sprintf("SELECT MASTER_POS_WAIT(%s, %s)", pos.Name, pos.Pos))
	return err
}

func (s *Slave) waitRelayLogDone() error {
	pos, err := s.fetchReadPos()
	if err != nil {
		return err
	}

	if err = s.waitUntilPosition(pos); err != nil {
		return err
	}

	return nil
}
