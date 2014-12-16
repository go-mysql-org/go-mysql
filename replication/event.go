package replication

import (
	"encoding/binary"
	//"encoding/hex"
	"fmt"
	"github.com/satori/go.uuid"
	. "github.com/siddontang/go-mysql/mysql"
	"io"
	"time"
)

const (
	EventHeaderSize = 19
)

type BinlogEvent struct {
	//raw binlog package, don't include last 4 bytes CRC32 checkout
	Data   []byte
	Header *EventHeader
	Event  Event
}

func (e *BinlogEvent) Dump(w io.Writer) {
	e.Header.Dump(w)
	e.Event.Dump(w)
}

type Event interface {
	//Dump Event, format like python-mysql-replication
	Dump(w io.Writer)

	Decode(data []byte) error
}

type EventError struct {
	Header *EventHeader

	//Error message
	Err string

	//Event data
	Data []byte
}

func (e *EventError) Error() string {
	return e.Err
}

type EventHeader struct {
	Timestamp uint32
	EventType EventType
	ServerID  uint32
	EventSize uint32
	LogPos    uint32
	Flags     uint16
}

func (h *EventHeader) Decode(data []byte) error {
	if len(data) < EventHeaderSize {
		return fmt.Errorf("header size too short %d, must 19", len(data))
	}

	pos := 0

	h.Timestamp = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	h.EventType = EventType(data[pos])
	pos++

	h.ServerID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	h.EventSize = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	h.LogPos = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	h.Flags = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	if h.EventSize < uint32(EventHeaderSize) {
		return fmt.Errorf("invalid event size %d, must >= 19", h.EventSize)
	}

	return nil
}

func (h *EventHeader) Dump(w io.Writer) {
	fmt.Fprintf(w, "=== %s ===\n", EventType(h.EventType))
	fmt.Fprintf(w, "Date: %s\n", time.Unix(int64(h.Timestamp), 0).Format(TimeFormat))
	fmt.Fprintf(w, "Log position: %d\n", h.LogPos)
	fmt.Fprintf(w, "Event size: %d\n", h.EventSize)
}

type FormatDescriptionEvent struct {
	Version uint16
	//len = 50
	ServerVersion          []byte
	CreateTimestamp        uint32
	EventHeaderLength      uint8
	EventTypeHeaderLengths []byte
}

func (e *FormatDescriptionEvent) Decode(data []byte) error {
	pos := 0
	e.Version = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	e.ServerVersion = make([]byte, 50)
	copy(e.ServerVersion, data[pos:])
	pos += 50

	e.CreateTimestamp = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	e.EventHeaderLength = data[pos]
	pos++

	if e.EventHeaderLength != byte(EventHeaderSize) {
		return fmt.Errorf("invalid event header length %d, must 19", e.EventHeaderLength)
	}

	e.EventTypeHeaderLengths = data[pos:]

	return nil
}

func (e *FormatDescriptionEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Version: %d\n", e.Version)
	fmt.Fprintf(w, "Server version: %s\n", e.ServerVersion)
	fmt.Fprintf(w, "Create date: %s\n", time.Unix(int64(e.CreateTimestamp), 0).Format(TimeFormat))
	//fmt.Fprintf(w, "Event header lengths: \n%s", hex.Dump(e.EventTypeHeaderLengths))
	fmt.Fprintln(w)
}

type RotateEvent struct {
	Position    uint64
	NextLogName []byte
}

func (e *RotateEvent) Decode(data []byte) error {
	e.Position = binary.LittleEndian.Uint64(data[0:])
	e.NextLogName = data[8:]

	return nil
}

func (e *RotateEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Position: %d\n", e.Position)
	fmt.Fprintf(w, "Next log name: %s\n", e.NextLogName)
	fmt.Fprintln(w)
}

type XIDEvent struct {
	XID uint64
}

func (e *XIDEvent) Decode(data []byte) error {
	e.XID = binary.LittleEndian.Uint64(data)
	return nil
}

func (e *XIDEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "XID: %d\n", e.XID)
	fmt.Fprintln(w)
}

type QueryEvent struct {
	SlaveProxyID  uint32
	ExecutionTime uint32
	ErrorCode     uint16
	StatusVars    []byte
	Schema        []byte
	Query         []byte
}

func (e *QueryEvent) Decode(data []byte) error {
	pos := 0

	e.SlaveProxyID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	e.ExecutionTime = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	schemaLength := uint8(data[pos])
	pos++

	e.ErrorCode = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	statusVarsLength := binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	e.StatusVars = data[pos : pos+int(statusVarsLength)]
	pos += int(statusVarsLength)

	e.Schema = data[pos : pos+int(schemaLength)]
	pos += int(schemaLength)

	//skip 0x00
	pos++

	e.Query = data[pos:]
	return nil
}

func (e *QueryEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Salve proxy ID: %d\n", e.SlaveProxyID)
	fmt.Fprintf(w, "Execution time: %d\n", e.ExecutionTime)
	fmt.Fprintf(w, "Error code: %d\n", e.ErrorCode)
	//fmt.Fprintf(w, "Status vars: \n%s", hex.Dump(e.StatusVars))
	fmt.Fprintf(w, "Schema: %s\n", e.Schema)
	fmt.Fprintf(w, "Query: %s\n", e.Query)
	fmt.Fprintln(w)
}

type GTIDEvent struct {
	CommitFlag uint8
	SID        []byte
	GNO        int64
}

func (e *GTIDEvent) Decode(data []byte) error {
	e.CommitFlag = uint8(data[0])

	e.SID = data[1:17]

	e.GNO = int64(binary.LittleEndian.Uint64(data[17:]))
	return nil
}

func (e *GTIDEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Commit flag: %d\n", e.CommitFlag)
	u, _ := uuid.FromBytes(e.SID)
	fmt.Fprintf(w, "GTID_NEXT: %s:%d\n", u.String(), e.GNO)
	fmt.Fprintln(w)
}
