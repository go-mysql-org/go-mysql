package mysql

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"regexp"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
)

// Note that MySQL normalized the value set by `SET GTID_NEXT='AUTOMATIC:<tag>`
// by:
// - Removing any length of leading and trailing whitespace (tabs, spaces).
// - Lowercasing the tag
var tagRegexp = regexp.MustCompile(`^\s*[a-zA-Z_][a-zA-Z0-9_]{0,31}\s*$`)

// Normalized tags should match: `^[a-z_][a-z0-9_]{0,31}$`

// Tag is a GTID Tag
type Tag string

// This ensures that Tag implements the encoding.BinaryMarshaler interface
var _ encoding.BinaryMarshaler = Tag("")

func (t Tag) MarshalBinary() ([]byte, error) {
	if len(t) > 32 {
		return nil, errors.New("tag length too long")
	}
	tagLen := uint8(len(t) << 1)
	return append([]byte{tagLen}, []byte(t)...), nil
}

// NewTag is taking a string and removes leading and trailing whitespace and changes the case to lowercase
func NewTag(str string) Tag {
	if str == "" {
		return Tag(str)
	}

	return Tag(strings.TrimSpace(strings.ToLower(str)))
}

// MysqlGTIDSet is storing a map of SIDs (UUIDs), each with one or more tags.
// And each tag has one or more Intervals.
type MysqlGTIDSet map[uuid.UUID]map[Tag]IntervalSlice

// This ensures that MysqlGTIDSet implements the GTIDSet interface
var _ GTIDSet = &MysqlGTIDSet{}

func DecodeMysqlGTIDSet(data []byte) (*MysqlGTIDSet, error) {
	if len(data) < 8 {
		return nil, errors.Errorf("invalid gtid set buffer, expected 8 or more but got %d", len(data))
	}
	s := NewMysqlGTIDSet()
	format, n := DecodeSid(data)
	tag := NewTag("")
	pos := 8
	for range n {
		if len(data) < pos+16 {
			return nil, errors.Errorf("invalid gtid set buffer, expected %d or more but got %d", pos+16, len(data))
		}
		sid, err := uuid.FromBytes(data[pos : pos+16])
		if err != nil {
			// This can't happen as uuid.FromBytes() only returns an error if the buffer is less than 16 bytes
			// and we already check for that.
			return nil, err
		}
		pos += 16

		if format == GtidFormatTagged {
			if pos >= len(data) {
				return nil, errors.New("invalid gtid set buffer, tag length expected")
			}
			taglen := int(data[pos] >> 1)
			pos++

			if pos+taglen > len(data) {
				return nil, errors.New("invalid gtid set buffer, tag extends beyond data")
			}
			tag = Tag(data[pos : pos+taglen])
			pos += taglen
		}

		if len(data) < pos+8 {
			return nil, errors.Errorf("invalid gtid set buffer, expected %d or more but got %d", pos+8, len(data))
		}
		intervalCount := binary.LittleEndian.Uint64(data[pos : pos+8])
		pos += 8
		if intervalCount == 0 {
			return nil, errors.New("invalid gtid set buffer, got zero interval count")
		}
		if intervalCount > math.MaxInt/16 { // 16 = minimum interval size of start+stop (8+8)
			return nil, errors.Errorf("invalid gtid set buffer, too many intervals: %d", intervalCount)
		}
		if len(data) < pos+(int(intervalCount)*16) {
			return nil, errors.Errorf("invalid gtid set buffer, expected %d or more but got %d", pos+(int(intervalCount)*16), len(data))
		}
		var intervals IntervalSlice
		for range intervalCount {
			start := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
			pos += 8
			stop := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
			pos += 8

			intervals = append(intervals, Interval{Start: start, Stop: stop})
		}
		if _, ok := s[sid]; ok {
			s[sid][tag] = append(s[sid][tag], intervals...)
		} else {
			s[sid] = map[Tag]IntervalSlice{
				tag: intervals,
			}
		}
	}

	for sid := range s {
		for tag := range s[sid] {
			s[sid][tag] = s[sid][tag].Normalize()
		}
	}

	if pos < len(data) {
		return &s, errors.Errorf("invalid gtid set buffer, found %d trailing bytes", len(data)-pos)
	}
	return &s, nil
}

func NewMysqlGTIDSet() MysqlGTIDSet {
	return make(map[uuid.UUID]map[Tag]IntervalSlice)
}

func ParseMysqlGTIDSet(str string) (GTIDSet, error) {
	s := NewMysqlGTIDSet()
	if str == "" {
		return &s, nil
	}

	// Each sp has single UUID/SID, but might have multiple sets, each with a unique tag
	sp := strings.SplitSeq(str, ",")

	for part := range sp {
		// Handle UUID/SID
		sep := strings.Split(strings.TrimSpace(part), ":")
		if len(sep) < 2 {
			return nil, errors.Errorf("invalid GTID format, must UUID[:tag]:interval[[:tag]:interval]")
		}

		u, err := uuid.Parse(sep[0])
		if err != nil {
			return nil, errors.Trace(err)
		}

		if _, ok := s[u]; !ok {
			s[u] = make(map[Tag]IntervalSlice, 1)
		}
		// Handle interval(s) and tags
		tag := NewTag("")
		for i := 1; i < len(sep); i++ {
			if tagRegexp.MatchString(sep[i]) {
				tag = NewTag(sep[i])
				if _, ok := s[u][tag]; !ok {
					s[u][tag] = nil
				}
			} else {
				if in, err := parseInterval(sep[i]); err != nil {
					return nil, errors.Trace(err)
				} else {
					s[u][tag] = append(s[u][tag], in)
				}
			}
		}
		for tag, val := range s[u] {
			if val == nil {
				return nil, errors.Errorf("invalid GTID format, missing interval for tag %s", tag)
			}
			s[u][tag] = val.Normalize()
		}
	}
	return &s, nil
}

func (s *MysqlGTIDSet) AddGTID(uuid uuid.UUID, gno int64) {
	s.AddGTIDWithTag(uuid, Tag(""), gno)
}

func (s *MysqlGTIDSet) AddGTIDWithTag(uuid uuid.UUID, tag Tag, gno int64) {
	_, ok := (*s)[uuid]
	if ok {
		(*s)[uuid][tag] = append((*s)[uuid][tag], Interval{gno, gno + 1}).Normalize()
	} else {
		(*s)[uuid] = map[Tag]IntervalSlice{
			tag: {
				Interval{gno, gno + 1},
			},
		}
	}
}

func (s *MysqlGTIDSet) Clone() GTIDSet {
	g := NewMysqlGTIDSet()
	for k, v := range *s {
		newInnerMap := make(map[Tag]IntervalSlice, len(v))
		for k2, v2 := range v {
			newInnerMap[k2] = slices.Clone(v2)
		}
		g[k] = newInnerMap
	}
	return &g
}

func (s *MysqlGTIDSet) Contain(o GTIDSet) bool {
	if om, ok := o.(*MysqlGTIDSet); ok {
		for k := range *om {
			if _, ok := (*s)[k]; !ok {
				return false
			}
			for k2 := range (*om)[k] {
				if i, ok := (*s)[k][k2]; !ok {
					return false
				} else {
					if !i.Contain((*om)[k][k2]) {
						return false
					}
				}
			}
		}
		return true
	}
	return false
}

func (s *MysqlGTIDSet) Encode() []byte {
	var buf bytes.Buffer

	format := GtidFormatClassic
	sidcount := uint64(0)
	var uuids []uuid.UUID
	for uuid := range *s {
		uuids = append(uuids, uuid)
		for tag := range (*s)[uuid] {
			sidcount++
			if format != GtidFormatTagged && tag != Tag("") {
				format = GtidFormatTagged
			}
		}
	}

	sid := encodeSid(format, sidcount)
	buf.Write(sid)

	slices.SortFunc(uuids, func(a, b uuid.UUID) int {
		return bytes.Compare(a[:], b[:])
	})
	for _, uuid := range uuids {
		tags := slices.Collect(maps.Keys((*s)[uuid]))
		slices.Sort(tags)

		for _, tag := range tags {
			ubin, err := uuid.MarshalBinary()
			if err != nil {
				// should never happen
				slog.Warn("encoding uuid failed", "error", err)
			}
			buf.Write(ubin)

			if format == GtidFormatTagged {
				tbin, err := tag.MarshalBinary()
				if err != nil {
					slog.Warn("encoding tag failed", "error", err)
				}
				buf.Write(tbin)
			}

			_ = binary.Write(&buf, binary.LittleEndian, uint64(len((*s)[uuid][tag])))
			for _, interval := range (*s)[uuid][tag] {
				_ = binary.Write(&buf, binary.LittleEndian, interval.Start)
				_ = binary.Write(&buf, binary.LittleEndian, interval.Stop)
			}
		}
	}

	return buf.Bytes()
}

func (s *MysqlGTIDSet) Equal(o GTIDSet) bool {
	if om, ok := o.(*MysqlGTIDSet); ok {
		if len(*s) != len(*om) {
			return false
		}
		for u, sm := range *s {
			omm, ok := (*om)[u]
			if !ok || len(sm) != len(omm) {
				return false
			}
			for k, i := range sm {
				if !i.Equal(omm[k]) {
					return false
				}
			}
		}
		return true
	}
	return false
}

func (s *MysqlGTIDSet) IsEmpty() bool {
	return len(*s) == 0
}

func (s *MysqlGTIDSet) String() string {
	var sb strings.Builder
	sep := ""
	var uuids []uuid.UUID
	for uuid := range *s {
		uuids = append(uuids, uuid)
	}
	slices.SortFunc(uuids, func(a, b uuid.UUID) int {
		return bytes.Compare(a[:], b[:])
	})
	for _, uuid := range uuids {
		sb.WriteString(sep)
		sb.WriteString(uuid.String())
		sep = ","
		var tags []Tag
		for tag := range (*s)[uuid] {
			tags = append(tags, tag)
		}
		// Tags are sorted, empty tag first
		slices.Sort(tags)
		for _, tag := range tags {
			if tag != "" {
				sb.WriteString(":")
				sb.WriteString(string(tag))
			}
			for _, interval := range (*s)[uuid][tag] {
				sb.WriteString(":")
				sb.WriteString(interval.String())
			}
		}
	}
	return sb.String()
}

func (s *MysqlGTIDSet) Update(GTIDStr string) error {
	o, err := ParseMysqlGTIDSet(GTIDStr)
	if err != nil {
		return err
	}
	if om, ok := o.(*MysqlGTIDSet); ok {
		for k, v := range *om {
			if _, ok := (*s)[k]; ok {
				for k2, v2 := range (*om)[k] {
					if _, ok := (*s)[k][k2]; ok {
						(*s)[k][k2] = append((*s)[k][k2], (*om)[k][k2]...).Normalize()
					} else {
						(*s)[k][k2] = v2
					}
				}
			} else {
				(*s)[k] = v
			}
		}
	} else {
		// This can't happen as ParseMysqlGTIDSet() always returns a MysqlGTIDSet
		return errors.New("incompatible GTID types")
	}
	return nil
}

// DecodeSid the number of sids (source identifiers) and if it is using
// tagged GTIDs or classic (non-tagged) GTIDs.
//
// Note that each gtid tag increases the sidnr here, so a single UUID
// might turn up multiple times if there are multiple tags.
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
