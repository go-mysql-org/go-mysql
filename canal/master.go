package canal

import (
	"sync"

	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/mysql"
)

type masterInfo struct {
	sync.RWMutex

	pos mysql.Position
}

func (m *masterInfo) Update(pos mysql.Position) {
	log.Debugf("update master position %s", pos)

	m.Lock()
	m.pos = pos
	m.Unlock()
}

func (m *masterInfo) Position() mysql.Position {
	m.RLock()
	defer m.RUnlock()

	return m.pos
}
