package canal

import (
	"log/slog"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
)

type masterInfo struct {
	sync.RWMutex

	pos mysql.Position

	gset mysql.GTIDSet

	timestamp uint32

	logger *slog.Logger
}

func (m *masterInfo) Update(pos mysql.Position) {
	m.logger.Debug("update master position", slog.Any("pos", pos))

	m.Lock()
	m.pos = pos
	m.Unlock()
}

func (m *masterInfo) UpdateTimestamp(ts uint32) {
	m.logger.Debug("update master timestamp", slog.Int64("ts", int64(ts)))

	m.Lock()
	m.timestamp = ts
	m.Unlock()
}

func (m *masterInfo) UpdateGTIDSet(gset mysql.GTIDSet) {
	m.logger.Debug("update master gtid set", slog.Any("gset", gset))

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
