package mysql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/satori/go.uuid"
	"github.com/siddontang/go/hack"
	"io"
	"strconv"
	"strings"
)

type Interval struct {
	Start int64
	Stop  int64
}

func parseInterval(str string) (i Interval, err error) {
	p := strings.Split(str, "-")
	switch len(p) {
	case 1:
		i.Start, err = strconv.ParseInt(p[0], 10, 64)
	case 2:
		i.Start, err = strconv.ParseInt(p[0], 10, 64)
		i.Stop, err = strconv.ParseInt(p[1], 10, 64)
	default:
		err = fmt.Errorf("invalid interval format, must n[-n]")
	}
	return
}

func (i Interval) String() string {
	//if stop = 0, use start only
	if i.Stop == 0 {
		return fmt.Sprintf("%d", i.Start)
	} else {
		return fmt.Sprintf("%d-%d", i.Start, i.Stop)
	}
}

// Refer http://dev.mysql.com/doc/refman/5.6/en/replication-gtids-concepts.html
type UUIDSet struct {
	SID uuid.UUID

	Intervals []Interval
}

func ParseUUIDSet(str string) (*UUIDSet, error) {
	sep := strings.Split(str, ":")
	if len(sep) < 2 {
		return nil, fmt.Errorf("invalid GTID format, must UUID:interval[:interval]")
	}

	var err error
	s := new(UUIDSet)
	if s.SID, err = uuid.FromString(sep[0]); err != nil {
		return nil, err
	}

	// Handle interval
	for i := 1; i < len(sep); i++ {
		if in, err := parseInterval(sep[i]); err != nil {
			return nil, err
		} else {
			s.Intervals = append(s.Intervals, in)
		}
	}

	return s, nil
}

func NewUUIDSet(sid uuid.UUID, in ...Interval) *UUIDSet {
	s := new(UUIDSet)
	s.SID = sid

	s.Intervals = in

	return s
}

func (s *UUIDSet) Bytes() []byte {
	var buf bytes.Buffer

	buf.WriteString(s.SID.String())

	for _, i := range s.Intervals {
		buf.WriteString(":")
		buf.WriteString(i.String())
	}

	return buf.Bytes()
}

func (s *UUIDSet) String() string {
	return hack.String(s.Bytes())
}

func (s *UUIDSet) encode(w io.Writer) {
	w.Write(s.SID.Bytes())
	n := int64(len(s.Intervals))

	binary.Write(w, binary.LittleEndian, n)

	for _, i := range s.Intervals {
		binary.Write(w, binary.LittleEndian, i.Start)

		//if no stop, use start + 1, see python_mysql_replication
		if i.Stop == 0 {
			binary.Write(w, binary.LittleEndian, i.Start+1)
		} else {
			binary.Write(w, binary.LittleEndian, i.Stop)
		}
	}
}

func (s *UUIDSet) Encode() []byte {
	var buf bytes.Buffer

	s.encode(&buf)

	return buf.Bytes()
}

func (s *UUIDSet) decode(data []byte) (int, error) {
	if len(data) < 24 {
		return 0, fmt.Errorf("invalid uuid set buffer, less 24")
	}

	pos := 0
	var err error
	if s.SID, err = uuid.FromBytes(data[0:16]); err != nil {
		return 0, err
	}
	pos += 16

	n := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
	pos += 8
	if len(data) < int(16*n)+pos {
		return 0, fmt.Errorf("invalid uuid set buffer, must %d, but %d", pos+int(16*n), len(data))
	}

	s.Intervals = make([]Interval, 0, n)

	var in Interval
	for i := int64(0); i < n; i++ {
		in.Start = int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
		pos += 8
		in.Stop = int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
		pos += 8
		s.Intervals = append(s.Intervals, in)
	}

	return pos, nil
}

func (s *UUIDSet) Decode(data []byte) error {
	n, err := s.decode(data)
	if n != len(data) {
		return fmt.Errorf("invalid uuid set buffer, must %d, but %d", n, len(data))
	}
	return err
}

type GTIDSet struct {
	Sets []*UUIDSet
}

func ParseGTIDSet(str string) (*GTIDSet, error) {
	s := new(GTIDSet)

	sp := strings.Split(str, ",")

	//todo, handle redundant same uuid
	for i := 0; i < len(sp); i++ {
		if set, err := ParseUUIDSet(sp[i]); err != nil {
			return nil, err
		} else {
			s.Sets = append(s.Sets, set)
		}

	}
	return s, nil
}

func (s *GTIDSet) String() string {
	var buf bytes.Buffer
	sep := ""
	for _, set := range s.Sets {
		buf.WriteString(sep)
		buf.WriteString(set.String())
		sep = ","
	}

	return hack.String(buf.Bytes())
}

func (s *GTIDSet) Encode() []byte {
	var buf bytes.Buffer

	binary.Write(&buf, binary.LittleEndian, uint64(len(s.Sets)))

	for i, _ := range s.Sets {
		s.Sets[i].encode(&buf)
	}

	return buf.Bytes()
}

func (s *GTIDSet) Decode(data []byte) error {
	if len(data) < 8 {
		return fmt.Errorf("invalid gtid set buffer, less 4")
	}

	n := int(binary.LittleEndian.Uint64(data))
	s.Sets = make([]*UUIDSet, 0, n)

	pos := 8

	for i := 0; i < n; i++ {
		set := new(UUIDSet)
		if n, err := set.decode(data[pos:]); err != nil {
			return err
		} else {
			pos += n

			s.Sets = append(s.Sets, set)
		}
	}
	return nil
}
