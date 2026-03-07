package mysql

import (
	"bytes"
	"encoding/binary"
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

// Refer:
// - https://dev.mysql.com/doc/refman/8.4/en/replication-gtids-concepts.html
// - https://bugs.mysql.com/bug.php?id=116789
type UUIDSet struct {
	SID       uuid.UUID
	Intervals IntervalSlice
}

func ParseUUIDSet(str string) (*UUIDSet, error) {
	str = strings.TrimSpace(str)
	sep := strings.Split(str, ":")
	if len(sep) < 2 {
		return nil, errors.Errorf("invalid GTID format, must UUID[:tag]:interval[[:tag]:interval]")
	}

	var err error
	s := new(UUIDSet)
	if s.SID, err = uuid.Parse(sep[0]); err != nil {
		return nil, errors.Trace(err)
	}

	// Handle interval
	tag := ""
	for i := 1; i < len(sep); i++ {
		if tagRegexp.MatchString(sep[i]) {
			if i > len(sep)-2 { // `UUID:tag` without interval start/stop
				return nil, errors.Errorf("invalid GTID format, must UUID[:tag]:interval[[:tag]:interval]")
			}
			if tagRegexp.MatchString(sep[i+1]) {
				return nil, errors.Errorf("invalid GTID format, consecutive tags found")
			}
			tag = sep[i]
			continue
		} else {
			if in, err := parseInterval(tag, sep[i]); err != nil {
				return nil, errors.Trace(err)
			} else {
				s.Intervals = append(s.Intervals, in)
			}
		}
	}

	s.Intervals = s.Intervals.Normalize()

	return s, nil
}

func NewUUIDSet(sid uuid.UUID, in ...Interval) *UUIDSet {
	s := new(UUIDSet)
	s.SID = sid

	s.Intervals = in
	s.Intervals = s.Intervals.Normalize()

	return s
}

func (s *UUIDSet) Contain(sub *UUIDSet) bool {
	if s.SID != sub.SID {
		return false
	}

	return s.Intervals.Contain(sub.Intervals)
}

func (s *UUIDSet) Bytes() []byte {
	var buf bytes.Buffer

	buf.WriteString(s.SID.String())

	lasttag := ""
	for _, i := range s.Intervals {
		buf.WriteString(":")
		if i.Tag == lasttag {
			buf.WriteString(i.StringWithoutTag())
		} else {
			buf.WriteString(i.String())
			lasttag = i.Tag
		}
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

		if minuend.Tag != subtrahend.Tag {
			// different tags, so no overlap
			n = append(n, minuend)
			i++
		} else if minuend.Stop <= subtrahend.Start {
			// no overlapping
			n = append(n, minuend)
			i++
		} else if minuend.Start >= subtrahend.Stop {
			// no overlapping
			j++
		} else {
			if minuend.Start < subtrahend.Start && minuend.Stop <= subtrahend.Stop {
				n = append(n, Interval{minuend.Start, subtrahend.Start, s.Intervals[i].Tag})
				i++
			} else if minuend.Start >= subtrahend.Start && minuend.Stop > subtrahend.Stop {
				minuend = Interval{subtrahend.Stop, minuend.Stop, s.Intervals[i].Tag}
				j++
			} else if minuend.Start >= subtrahend.Start && minuend.Stop <= subtrahend.Stop {
				// minuend is completely removed
				i++
			} else if minuend.Start < subtrahend.Start && minuend.Stop > subtrahend.Stop {
				n = append(n, Interval{minuend.Start, subtrahend.Start, s.Intervals[i].Tag})
				minuend = Interval{subtrahend.Stop, minuend.Stop, s.Intervals[i].Tag}
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
	if format == GtidFormatClassic {
		b, _ := s.SID.MarshalBinary()
		_, _ = w.Write(b)

		n := int64(len(s.Intervals))
		_ = binary.Write(w, binary.LittleEndian, n)

		for _, i := range s.Intervals {
			_ = binary.Write(w, binary.LittleEndian, i.Start)
			_ = binary.Write(w, binary.LittleEndian, i.Stop)
		}

		return
	}

	taglens := make(map[string]int64)
	for _, i := range s.Intervals {
		taglens[i.Tag]++
	}

	lasttag := ""
	for j, i := range s.Intervals {
		if j == 0 || i.Tag != lasttag {
			b, _ := s.SID.MarshalBinary()
			_, _ = w.Write(b)

			taglen := uint8(len(i.Tag) << 1)
			_, _ = w.Write([]byte{taglen})
			_, _ = w.Write([]byte(i.Tag))
			lasttag = i.Tag

			n := taglens[i.Tag]
			_ = binary.Write(w, binary.LittleEndian, n)
		}

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

	for pos < len(data) {
		if s.SID == uuid.Nil {
			if s.SID, err = uuid.FromBytes(data[0:16]); err != nil {
				return 0, err
			}
		} else if pos+16 > len(data) {
			return pos, errors.New("invalid uuid set buffer, expected 16 bytes beyond pos")
		} else if !bytes.Equal(data[0:16], data[pos:pos+16]) {
			// If the first UUID matches the current one then we continue
			// as that's a different TSID (tagged sid), so the same UUID,
			// but a different tag.
			//
			// If the UUIDs are different, then we return as that's the next (t)sid.
			return pos, nil
		}
		pos += 16

		var tag string
		if format == GtidFormatTagged {
			if pos >= len(data) {
				return 0, errors.New("invalid uuid set buffer, tag length expected")
			}
			taglen := int(data[pos] >> 1)
			pos++
			if pos+taglen > len(data) {
				return 0, errors.New("invalid uuid set buffer, tag extends beyond data")
			}
			tag = string(data[pos : pos+taglen])
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
			if tag != "" {
				in.Tag = tag
			}
			in.Start = int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
			pos += 8
			in.Stop = int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
			pos += 8
			s.Intervals = append(s.Intervals, in)
		}
	}

	return pos, nil
}

// Decode is decoding the GTID Set in the format of COM_BINLOG_DUMP_GTID
func (s *UUIDSet) Decode(data []byte, format GtidFormat) error {
	n, err := s.decode(data, format)
	if n != len(data) {
		return errors.Errorf("invalid uuid set buffer, must %d, but %d", n, len(data))
	}
	return err
}

func (s *UUIDSet) Clone() *UUIDSet {
	clone := new(UUIDSet)
	clone.SID = s.SID
	clone.Intervals = make([]Interval, len(s.Intervals))
	copy(clone.Intervals, s.Intervals)
	return clone
}
