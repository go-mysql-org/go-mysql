package replication

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

type BinlogParser struct {
	format *FormatDescriptionEvent

	tables map[uint64]*TableMapEvent

	// for rawMode, we only parse FormatDescriptionEvent and RotateEvent
	rawMode bool
}

func NewBinlogParser() *BinlogParser {
	p := new(BinlogParser)

	p.tables = make(map[uint64]*TableMapEvent)

	return p
}

func (p *BinlogParser) ParseFile(name string, offset int64) (*BinlogStreamer, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	b := make([]byte, 4)
	if _, err = f.Read(b); err != nil {
		return nil, err
	} else if !bytes.Equal(b, BinLogFileHeader) {
		return nil, fmt.Errorf("%s is not a valid binlog file, head 4 bytes must fe'bin' ", name)
	}

	if offset < 4 {
		offset = 4
	}

	if _, err = f.Seek(offset, os.SEEK_SET); err != nil {
		return nil, fmt.Errorf("seek %s to %d error %v", name, offset, err)
	}

	return p.ParseReader(f)
}

func (p *BinlogParser) ParseReader(r io.Reader) (*BinlogStreamer, error) {
	s := newBinlogStreamer()

	go func(s *BinlogStreamer) {
		var buf bytes.Buffer
		var err error

		for {
			buf.Reset()

			if _, err = io.CopyN(&buf, r, EventHeaderSize); err != nil {
				break
			}

			data := buf.Bytes()
			h, err := p.parseHeader(data)
			if err != nil {
				break
			}

			if _, err = io.CopyN(&buf, r, int64(h.EventSize)); err != nil {
				break
			}

			data = buf.Bytes()
			rawData := data

			data = data[EventHeaderSize:]
			eventLen := int(h.EventSize) - EventHeaderSize

			if len(data) != eventLen {
				err = fmt.Errorf("invalid data size %d in event %s, less event length %d", len(data), h.EventType, eventLen)
				break
			}

			var e Event
			e, err = p.parseEvent(h, data)
			if err != nil {
				break
			}

			s.ch <- &BinlogEvent{rawData, h, e}
		}

		s.closeWithError(err)
	}(s)

	return s, nil
}

func (p *BinlogParser) SetRawMode(mode bool) {
	p.rawMode = mode
}

func (p *BinlogParser) parseHeader(data []byte) (*EventHeader, error) {
	h := new(EventHeader)
	err := h.Decode(data)
	if err != nil {
		return nil, err
	}

	return h, nil
}

func (p *BinlogParser) parseEvent(h *EventHeader, data []byte) (Event, error) {
	var e Event

	if h.EventType == FORMAT_DESCRIPTION_EVENT {
		p.format = &FormatDescriptionEvent{}
		e = p.format
	} else {
		if p.format != nil && p.format.ChecksumAlgorithm == BINLOG_CHECKSUM_ALG_CRC32 {
			data = data[0 : len(data)-4]
		}

		if h.EventType == ROTATE_EVENT {
			e = &RotateEvent{}
		} else if !p.rawMode {
			switch h.EventType {
			case QUERY_EVENT:
				e = &QueryEvent{}
			case XID_EVENT:
				e = &XIDEvent{}
			case TABLE_MAP_EVENT:
				te := &TableMapEvent{}
				if p.format.EventTypeHeaderLengths[TABLE_MAP_EVENT-1] == 6 {
					te.tableIDSize = 4
				} else {
					te.tableIDSize = 6
				}
				e = te
			case WRITE_ROWS_EVENTv0,
				UPDATE_ROWS_EVENTv0,
				DELETE_ROWS_EVENTv0,
				WRITE_ROWS_EVENTv1,
				DELETE_ROWS_EVENTv1,
				UPDATE_ROWS_EVENTv1,
				WRITE_ROWS_EVENTv2,
				UPDATE_ROWS_EVENTv2,
				DELETE_ROWS_EVENTv2:
				e = p.newRowsEvent(h)
			case ROWS_QUERY_EVENT:
				e = &RowsQueryEvent{}
			case GTID_EVENT:
				e = &GTIDEvent{}
			case MARIADB_ANNOTATE_ROWS_EVENT:
				e = &MariadbAnnotaeRowsEvent{}
			case MARIADB_BINLOG_CHECKPOINT_EVENT:
				e = &MariadbBinlogCheckPointEvent{}
			case MARIADB_GTID_LIST_EVENT:
				e = &MariadbGTIDListEvent{}
			case MARIADB_GTID_EVENT:
				ee := &MariadbGTIDEvent{}
				ee.GTID.ServerID = h.ServerID
				e = ee
			default:
				e = &GenericEvent{}
			}
		} else {
			e = &GenericEvent{}
		}
	}

	if err := e.Decode(data); err != nil {
		return nil, &EventError{h, err.Error(), data}
	}

	if te, ok := e.(*TableMapEvent); ok {
		p.tables[te.TableID] = te
	}

	//If MySQL restart, it may use the same table id for different tables.
	//We must clear the table map before parsing new events.
	//We have no better way to known whether the event is before or after restart,
	//So we have to clear the table map on every rotate event.
	if _, ok := e.(*RotateEvent); ok {
		p.tables = make(map[uint64]*TableMapEvent)
	}

	return e, nil
}

func (p *BinlogParser) parse(data []byte) (*BinlogEvent, error) {
	rawData := data

	h, err := p.parseHeader(data)

	if err != nil {
		return nil, err
	}

	data = data[EventHeaderSize:]
	eventLen := int(h.EventSize) - EventHeaderSize

	if len(data) != eventLen {
		return nil, fmt.Errorf("invalid data size %d in event %s, less event length %d", len(data), h.EventType, eventLen)
	}

	e, err := p.parseEvent(h, data)
	if err != nil {
		return nil, err
	}

	return &BinlogEvent{rawData, h, e}, nil
}

func (p *BinlogParser) newRowsEvent(h *EventHeader) *RowsEvent {
	e := &RowsEvent{}
	if p.format.EventTypeHeaderLengths[h.EventType-1] == 6 {
		e.tableIDSize = 4
	} else {
		e.tableIDSize = 6
	}

	e.needBitmap2 = false
	e.tables = p.tables

	switch h.EventType {
	case WRITE_ROWS_EVENTv0:
		e.Version = 0
	case UPDATE_ROWS_EVENTv0:
		e.Version = 0
	case DELETE_ROWS_EVENTv0:
		e.Version = 0
	case WRITE_ROWS_EVENTv1:
		e.Version = 1
	case DELETE_ROWS_EVENTv1:
		e.Version = 1
	case UPDATE_ROWS_EVENTv1:
		e.Version = 1
		e.needBitmap2 = true
	case WRITE_ROWS_EVENTv2:
		e.Version = 2
	case UPDATE_ROWS_EVENTv2:
		e.Version = 2
		e.needBitmap2 = true
	case DELETE_ROWS_EVENTv2:
		e.Version = 2
	}

	return e
}
