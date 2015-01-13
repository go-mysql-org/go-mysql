package mysql

import (
	"fmt"
	"strconv"
	"strings"
)

type MariadbGTID struct {
	DomainID       uint32
	ServerID       uint32
	SequenceNumber uint64
}

func ParseMariadbGTID(str string) (MariadbGTID, error) {
	seps := strings.Split(str, "-")

	var gtid MariadbGTID

	if len(seps) != 3 {
		return gtid, fmt.Errorf("invalid Mariadb GTID %v, must domain-server-sequence", str)
	}

	domainID, err := strconv.ParseUint(seps[0], 10, 32)
	if err != nil {
		return gtid, fmt.Errorf("invalid MariaDB GTID Domain ID (%v): %v", seps[0], err)
	}

	serverID, err := strconv.ParseUint(seps[1], 10, 32)
	if err != nil {
		return gtid, fmt.Errorf("invalid MariaDB GTID Server ID (%v): %v", seps[1], err)
	}

	sequenceID, err := strconv.ParseUint(seps[2], 10, 64)
	if err != nil {
		return gtid, fmt.Errorf("invalid MariaDB GTID Sequence number (%v): %v", seps[2], err)
	}

	return MariadbGTID{
		DomainID:       uint32(domainID),
		ServerID:       uint32(serverID),
		SequenceNumber: sequenceID}, nil
}

func (gtid MariadbGTID) String() string {
	return fmt.Sprintf("%d-%d-%d", gtid.DomainID, gtid.ServerID, gtid.SequenceNumber)
}
