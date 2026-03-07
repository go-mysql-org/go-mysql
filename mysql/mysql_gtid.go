package mysql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"

	"github.com/go-mysql-org/go-mysql/utils"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
)

// MysqlGTIDSet has a TSID (UUID+Tag) as key
type MysqlGTIDSet struct {
	Sets map[TSID]*UUIDSet
}

var _ GTIDSet = &MysqlGTIDSet{}

func ParseMysqlGTIDSet(str string) (GTIDSet, error) {
	s := new(MysqlGTIDSet)
	s.Sets = make(map[TSID]*UUIDSet)
	if str == "" {
		return s, nil
	}

	sp := strings.Split(str, ",")

	// todo, handle redundant same uuid
	for i := range sp {
		if sets, err := ParseUUIDSets(sp[i]); err != nil {
			return nil, errors.Trace(err)
		} else {
			s.AddSets(sets)
		}
	}
	return s, nil
}

func DecodeMysqlGTIDSet(data []byte) (*MysqlGTIDSet, error) {
	s := new(MysqlGTIDSet)

	if len(data) < 8 {
		return nil, errors.Errorf("invalid gtid set buffer, expected 8 or more but got %d", len(data))
	}

	format, n := DecodeSid(data)
	s.Sets = make(map[TSID]*UUIDSet, n)
	pos := 8

	for range n {
		if format == GtidFormatTagged && pos >= len(data) {
			break
		}
		set := new(UUIDSet)
		if n, err := set.decode(data[pos:], format); err != nil {
			return nil, errors.Trace(err)
		} else {
			pos += n

			s.AddSet(set)
		}
	}
	return s, nil
}

func (s *MysqlGTIDSet) AddSet(set *UUIDSet) {
	if set == nil {
		return
	}
	o, ok := s.Sets[set.TSID]
	if ok {
		o.AddInterval(set.Intervals)
	} else {
		s.Sets[set.TSID] = set
	}
}

func (s *MysqlGTIDSet) AddSets(sets []UUIDSet) {
	if sets == nil {
		return
	}
	for _, set := range sets {
		o, ok := s.Sets[set.TSID]
		if ok {
			o.AddInterval(set.Intervals)
		} else {
			s.Sets[set.TSID] = &set
		}
	}
}

func (s *MysqlGTIDSet) MinusSet(set *UUIDSet) {
	if set == nil {
		return
	}
	uuidSet, ok := s.Sets[set.TSID]
	if ok {
		uuidSet.MinusInterval(set.Intervals)
		if uuidSet.Intervals == nil {
			delete(s.Sets, set.TSID)
		}
	}
}

func (s *MysqlGTIDSet) Update(GTIDStr string) error {
	gtidSet, err := ParseMysqlGTIDSet(GTIDStr)
	if err != nil {
		return err
	}
	for _, uuidSet := range gtidSet.(*MysqlGTIDSet).Sets {
		s.AddSet(uuidSet)
	}
	return nil
}

func (s *MysqlGTIDSet) AddGTID(uuid uuid.UUID, gno int64) {
	s.AddGTIDWithTag(TSID{SID: uuid}, gno)
}

func (s *MysqlGTIDSet) AddGTIDWithTag(tsid TSID, gno int64) {
	o, ok := s.Sets[tsid]
	if ok {
		o.Intervals.InsertInterval(Interval{gno, gno + 1})
	} else {
		s.Sets[tsid] = &UUIDSet{tsid, IntervalSlice{Interval{gno, gno + 1}}}
	}
}

func (s *MysqlGTIDSet) Add(addend MysqlGTIDSet) error {
	for _, uuidSet := range addend.Sets {
		s.AddSet(uuidSet)
	}
	return nil
}

func (s *MysqlGTIDSet) Minus(subtrahend MysqlGTIDSet) error {
	for _, uuidSet := range subtrahend.Sets {
		s.MinusSet(uuidSet)
	}
	return nil
}

func (s *MysqlGTIDSet) Contain(o GTIDSet) bool {
	sub, ok := o.(*MysqlGTIDSet)
	if !ok {
		return false
	}

	for key, set := range sub.Sets {
		o, ok := s.Sets[key]
		if !ok {
			return false
		}

		if !o.Contain(set) {
			return false
		}
	}

	return true
}

func (s *MysqlGTIDSet) Equal(o GTIDSet) bool {
	sub, ok := o.(*MysqlGTIDSet)
	if !ok {
		return false
	}

	if len(sub.Sets) != len(s.Sets) {
		return false
	}

	for key, set := range sub.Sets {
		o, ok := s.Sets[key]
		if !ok {
			return false
		}

		if !o.Intervals.Equal(set.Intervals) {
			return false
		}
	}

	return true
}

func (s *MysqlGTIDSet) String() string {
	// there is only one element in gtid set
	if len(s.Sets) == 1 {
		for _, set := range s.Sets {
			return set.String()
		}
	}

	// sort multi set
	sets := make([]string, 0, len(s.Sets))
	for _, set := range s.Sets {
		sets = append(sets, set.String())
	}
	slices.Sort(sets)

	var buf bytes.Buffer
	sep := ""
	for i, set := range sets {
		if i > 0 && sets[i-1][1:36] == set[i:36] {
			sep = ":"
			buf.WriteString(sep)
			buf.WriteString(set[37:])
			sep = ","
		} else {
			buf.WriteString(sep)
			buf.WriteString(set)
			sep = ","
		}
	}

	return utils.ByteSliceToString(buf.Bytes())
}

// Encode is encoding the GTID Set in the format of COM_BINLOG_DUMP_GTID
func (s *MysqlGTIDSet) Encode() []byte {
	var buf bytes.Buffer

	format := GtidFormatClassic
	for _, set := range s.Sets {
		if set.TSID.Tag != "" {
			format = GtidFormatTagged
		}
	}
	sidcount := uint64(len(s.Sets))
	sid := encodeSid(format, sidcount)
	buf.Write(sid)

	sets := make([]TSID, 0, len(s.Sets))
	for k := range s.Sets {
		sets = append(sets, k)
	}
	slices.SortFunc(sets, func(a, b TSID) int {
		abin, _ := a.SID.MarshalBinary()
		bbin, _ := b.SID.MarshalBinary()
		bcmp := bytes.Compare(abin, bbin)
		if bcmp == 0 {
			return strings.Compare(a.Tag, b.Tag)
		}
		return bcmp
	})

	for _, tsid := range sets {
		s.Sets[tsid].encode(format, &buf)
	}

	return buf.Bytes()
}

func (gtid *MysqlGTIDSet) Clone() GTIDSet {
	clone := &MysqlGTIDSet{
		Sets: make(map[TSID]*UUIDSet),
	}
	for tsid, uuidSet := range gtid.Sets {
		clone.Sets[tsid] = uuidSet.Clone()
	}

	return clone
}

func (s *MysqlGTIDSet) IsEmpty() bool {
	return len(s.Sets) == 0
}

// Decode the number of sids (source identifiers) and if it is using
// tagged GTIDs or classic (non-tagged) GTIDs.
//
// Note that each gtid tag increases the sidno here, so a single UUID
// might turn up multiple times if there are multipl tags.
//
// see also:
// decode_nsids_format in mysql/mysql-server
// https://github.com/mysql/mysql-server/blob/61a3a1d8ef15512396b4c2af46e922a19bf2b174/sql/rpl_gtid_set.cc#L1363-L1378
func DecodeSid(data []byte) (format GtidFormat, sidnr uint64) {
	if len(data) < 8 {
		// input too short, the function signature doesn't allow us to return an error here.
		return format, sidnr
	}
	if data[7] == 0x1 {
		format = GtidFormatTagged
	}

	if format == GtidFormatTagged {
		masked := make([]byte, 8)
		copy(masked, data[1:7])
		sidnr = binary.LittleEndian.Uint64(masked)
		return format, sidnr
	}
	sidnr = binary.LittleEndian.Uint64(data[:8])
	return format, sidnr
}

func encodeSid(format GtidFormat, sidnr uint64) []byte {
	sid := make([]byte, 8)
	if format == GtidFormatClassic {
		_, _ = binary.Encode(sid, binary.LittleEndian, sidnr)
		return sid
	}
	_, _ = binary.Encode(sid, binary.LittleEndian, sidnr<<8)

	sid[0] = 0x01
	sid[7] = 0x01 // Format marker
	return sid
}

func (f GtidFormat) String() string {
	switch f {
	case GtidFormatClassic:
		return "GtidFormatClassic"
	case GtidFormatTagged:
		return "GtidFormatTagged"
	}
	return fmt.Sprintf("GtidFormat{%d}", int(f))
}
