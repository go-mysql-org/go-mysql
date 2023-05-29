package canal

import (
	"sync"

	"github.com/instructure/mc-go-mysql/mysql"
	"github.com/siddontang/go-log/loggers"
)

type masterInfo struct {
	sync.RWMutex

	pos mysql.Position

	gset mysql.GTIDSet

	timestamp uint32

	logger loggers.Advanced

	infoLoader MasterInfoLoader
}

// abstract the way in which the master info is loaded and saved
type MasterInfoSetter func(addr, name string, position uint32) error
type MasterInfoLoader interface {
	Load(setValues MasterInfoSetter) error
	Save(addr, name string, position uint32, force bool) error
}

func (m *masterInfo) Setter(addr, name string, position uint32) error {
	m.Addr = addr
	m.pos = mysql.Position{Name: name, Pos: position}
	return nil
}

func (m *masterInfo) Update(pos mysql.Position) {
	m.logger.Debugf("update master position %s", pos)

	m.Lock()
	m.pos = pos
	m.Unlock()
}

func (m *masterInfo) UpdateTimestamp(ts uint32) {
	m.logger.Debugf("update master timestamp %d", ts)

	m.Lock()
	m.timestamp = ts
	m.Unlock()
}

func (m *masterInfo) UpdateGTIDSet(gset mysql.GTIDSet) {
	m.logger.Debugf("update master gtid set %s", gset)

	m.Lock()
	m.gset = gset
	m.Unlock()
}

func (m *masterInfo) Position() mysql.Position {
	m.RLock()
	defer m.RUnlock()

	return m.pos
}

func (m *masterInfo) Timestamp() uint32 {
	m.RLock()
	defer m.RUnlock()

	return m.timestamp
}

func (m *masterInfo) GTIDSet() mysql.GTIDSet {
	m.RLock()
	defer m.RUnlock()

	if m.gset == nil {
		return nil
	}
	return m.gset.Clone()
}
