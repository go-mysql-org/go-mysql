package mysql

import (
	"github.com/google/uuid"
	"github.com/pingcap/errors"
)

type GTIDSet interface {
	String() string

	// Encode GTID set into binary format used in binlog dump commands
	Encode() []byte

	Equal(o GTIDSet) bool

	Contain(o GTIDSet) bool

	Update(GTIDStr string) error

	Clone() GTIDSet

	AddGTID(uuid.UUID, int64, uint32, uint32, uint64) error
}

func ParseGTIDSet(flavor string, s string) (GTIDSet, error) {
	switch flavor {
	case MySQLFlavor:
		return ParseMysqlGTIDSet(s)
	case MariaDBFlavor:
		return ParseMariadbGTIDSet(s)
	default:
		return nil, errors.Errorf("invalid flavor %s", flavor)
	}
}
