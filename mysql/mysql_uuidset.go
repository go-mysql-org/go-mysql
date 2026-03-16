package mysql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"regexp"
	"strings"

	"github.com/go-mysql-org/go-mysql/utils"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
)

// Note that MySQL normalized the value set by `SET GTID_NEXT='AUTOMATIC:<tag>`
// by:
// - Removing any length of leading and trailing whitespace (tabs, spaces).
// - Lowercasing the tag
var tagRegexp = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,31}$`)

// TSID is a Tagged SID
type TSID struct {
	SID uuid.UUID
	Tag string
}

func (t *TSID) String() string {
	if t.Tag == "" {
		return t.SID.String()
	}
	return fmt.Sprintf("%s:%s", t.SID.String(), t.Tag)
}

// MarshalBinary is returning the TSID in binary format
//
// if there is no tag:
// <uuid[16]>
//
// if there is a tag:
// <uuid[16]><length[1]><tag[length]>
func (t *TSID) MarshalBinary() ([]byte, error) {
	b, _ := t.SID.MarshalBinary()
	if t.Tag != "" {
		b = append(b, byte(len(t.Tag)<<1))
	}
	return append(b, []byte(t.Tag)...), nil
}

// Refer:
// - https://dev.mysql.com/doc/refman/8.4/en/replication-gtids-concepts.html
// - https://bugs.mysql.com/bug.php?id=116789
type UUIDSet struct {
	TSID      TSID
	Intervals IntervalSlice
}

// ParseUUIDSet is returning the first UUIDSet from str
func ParseUUIDSet(str string) (*UUIDSet, error) {
	str = strings.TrimSpace(str)
	sep := strings.Split(str, ":")
	if len(sep) < 2 {
		return nil, errors.Errorf("invalid GTID format, must UUID[:tag]:interval[[:tag]:interval]")
	}

	var err error
	s := new(UUIDSet)
	if s.TSID.SID, err = uuid.Parse(sep[0]); err != nil {
		return nil, errors.Trace(err)
	}

	start := 1
	if tagRegexp.MatchString(sep[1]) {
		s.TSID.Tag = sep[1]
		start++
	}

	// Handle interval
	for i := start; i < len(sep); i++ {
		if tagRegexp.MatchString(sep[i]) {
			// We found another tag, so that's the next set
			break
		} else {
			if in, err := parseInterval(sep[i]); err != nil {
				return nil, errors.Trace(err)
			} else {
				s.Intervals = append(s.Intervals, in)
			}
		}
	}

	if len(s.Intervals) < 1 {
		return nil, errors.New("invalid GTID format, missing interval")
	}

	s.Intervals = s.Intervals.Normalize()

	return s, nil
}

// ParseUUIDSets is returning all UUIDSets from str
// str must contain sets with the same UUID, but different tags
func ParseUUIDSets(str string) ([]UUIDSet, error) {
	str = strings.TrimSpace(str)
	sep := strings.Split(str, ":")
	if len(sep) < 2 {
		return nil, errors.Errorf("invalid GTID format, must UUID[:tag]:interval[[:tag]:interval]")
	}

	u, err := uuid.Parse(sep[0])
	if err != nil {
		return nil, errors.Trace(err)
	}

	var sets []UUIDSet

	s := UUIDSet{
		TSID: TSID{SID: u},
	}
	// Handle interval
	for i := 1; i < len(sep); i++ {
		if tagRegexp.MatchString(sep[i]) {
			if s.Intervals != nil {
				sets = append(sets, s)
				s = UUIDSet{
					TSID: TSID{SID: u},
				}
			} else if s.Intervals == nil {
				return nil, errors.New("invalid GTID format, consecutive tags")
			}
			s.TSID.Tag = sep[i]
		} else {
			if in, err := parseInterval(sep[i]); err != nil {
				return nil, errors.Trace(err)
			} else {
				s.Intervals = append(s.Intervals, in)
			}
		}

		if s.Intervals != nil {
			s.Intervals = s.Intervals.Normalize()
		}

		if i+1 == len(sep) {
			sets = append(sets, s)
		}
	}

	return sets, nil
}

func NewUUIDSet(tsid TSID, in ...Interval) *UUIDSet {
	s := new(UUIDSet)
	s.TSID = tsid

	s.Intervals = in
	s.Intervals = s.Intervals.Normalize()

	return s
}

func (s *UUIDSet) Contain(sub *UUIDSet) bool {
	if s.TSID != sub.TSID {
		return false
	}

	return s.Intervals.Contain(sub.Intervals)
}

func (s *UUIDSet) Bytes() []byte {
	var buf bytes.Buffer

	buf.WriteString(s.TSID.String())

	for _, i := range s.Intervals {
		buf.WriteString(":")
		buf.WriteString(i.String())
	}

	return buf.Bytes()
}

func (s *UUIDSet) AddInterval(in IntervalSlice) {
	s.Intervals = append(s.Intervals, in...)
	s.Intervals = s.Intervals.Normalize()
}

func (s *UUIDSet) MinusInterval(in IntervalSlice) {
	var n IntervalSlice
	in = in.Normalize()

	i, j := 0, 0
	var minuend Interval
	var subtrahend Interval
	for i < len(s.Intervals) {
		if minuend.Stop != s.Intervals[i].Stop { // `i` changed?
			minuend = s.Intervals[i]
		}
		if j < len(in) {
			subtrahend = in[j]
		} else {
			subtrahend = Interval{Start: math.MaxInt64, Stop: math.MaxInt64}
		}

		if minuend.Stop <= subtrahend.Start {
			// no overlapping
			n = append(n, minuend)
			i++
		} else if minuend.Start >= subtrahend.Stop {
			// no overlapping
			j++
		} else {
			if minuend.Start < subtrahend.Start && minuend.Stop <= subtrahend.Stop {
				n = append(n, Interval{minuend.Start, subtrahend.Start})
				i++
			} else if minuend.Start >= subtrahend.Start && minuend.Stop > subtrahend.Stop {
				minuend = Interval{subtrahend.Stop, minuend.Stop}
				j++
			} else if minuend.Start >= subtrahend.Start && minuend.Stop <= subtrahend.Stop {
				// minuend is completely removed
				i++
			} else if minuend.Start < subtrahend.Start && minuend.Stop > subtrahend.Stop {
				n = append(n, Interval{minuend.Start, subtrahend.Start})
				minuend = Interval{subtrahend.Stop, minuend.Stop}
				j++
			} else {
				panic("should never be here")
			}
		}
	}

	s.Intervals = n.Normalize()
}

func (s *UUIDSet) String() string {
	return utils.ByteSliceToString(s.Bytes())
}

func (s *UUIDSet) encode(format GtidFormat, w io.Writer) {
	b, _ := s.TSID.SID.MarshalBinary()
	if format == GtidFormatTagged {
		b = append(b, byte(len(s.TSID.Tag)<<1))
		b = append(b, []byte(s.TSID.Tag)...)
	}
	_, _ = w.Write(b)

	n := int64(len(s.Intervals))
	_ = binary.Write(w, binary.LittleEndian, n)

	for _, i := range s.Intervals {
		_ = binary.Write(w, binary.LittleEndian, i.Start)
		_ = binary.Write(w, binary.LittleEndian, i.Stop)
	}
}

// Encode is encoding the GTID Set in the format of COM_BINLOG_DUMP_GTID
func (s *UUIDSet) Encode(format GtidFormat) []byte {
	var buf bytes.Buffer

	s.encode(format, &buf)

	return buf.Bytes()
}

func (s *UUIDSet) decode(data []byte, format GtidFormat) (int, error) {
	if len(data) < 24 {
		return 0, errors.Errorf("invalid uuid set buffer, expected 24 or more but got %d", len(data))
	}
	pos := 0
	var err error

	if s.TSID.SID, err = uuid.FromBytes(data[0:16]); err != nil {
		return 0, err
	}
	pos += 16

	if format == GtidFormatTagged {
		if pos >= len(data) {
			return 0, errors.New("invalid uuid set buffer, tag length expected")
		}
		taglen := int(data[pos] >> 1)
		pos++
		if pos+taglen > len(data) {
			return 0, errors.New("invalid uuid set buffer, tag extends beyond data")
		}
		s.TSID.Tag = string(data[pos : pos+taglen])
		pos += taglen
	}

	if pos+8 > len(data) {
		return 0, errors.New("invalid uuid set buffer, truncated interval length")
	}
	n := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
	pos += 8
	if len(data) < int(16*n)+pos {
		return 0, errors.Errorf("invalid uuid set buffer, expected %d, but got %d", pos+int(16*n), len(data))
	}
	if n < 0 {
		return 0, errors.New("invalid uuid set buffer, interval count can't be negative")
	}
	if n > math.MaxInt64/16 { // 16 = minimum interval size of start+stop (8+8)
		return 0, errors.Errorf("invalid uuid set buffer, too many intervals: %d", n)
	}
	if len(s.Intervals) == 0 {
		s.Intervals = make([]Interval, 0, n)
	} else {
		s.Intervals = append(make([]Interval, 0, len(s.Intervals)+int(n)), s.Intervals...)
	}

	var in Interval
	for range n {
		in.Start = int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
		pos += 8
		in.Stop = int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
		pos += 8
		s.Intervals = append(s.Intervals, in)
	}

	return pos, nil
}

// Decode is decoding the GTID Set in the format of COM_BINLOG_DUMP_GTID
func (s *UUIDSet) Decode(data []byte, format GtidFormat) error {
	n, err := s.decode(data, format)
	if n != len(data) {
		return errors.Errorf("invalid uuid set buffer, decoded %d bytes, but data length is %d bytes", n, len(data))
	}
	return err
}

func (s *UUIDSet) Clone() *UUIDSet {
	clone := new(UUIDSet)
	clone.TSID = s.TSID
	clone.Intervals = make([]Interval, len(s.Intervals))
	copy(clone.Intervals, s.Intervals)
	return clone
}
