package replication

import (
	"encoding/binary"
	"fmt"
	"github.com/siddontang/go-mysql/client"
	. "github.com/siddontang/go-mysql/mysql"
	"sync"
	"time"
)

type BinlogSyncer struct {
	c        *client.Conn
	serverID uint32

	host     string
	port     uint16
	user     string
	password string

	masterID uint32

	wg sync.WaitGroup

	quit chan struct{}

	useChecksum bool

	format *FormatDescriptionEvent

	tables map[uint64]*TableMapEvent
}

func NewBinlogSyncer(serverID uint32) *BinlogSyncer {
	b := new(BinlogSyncer)
	b.serverID = serverID

	b.masterID = 0

	b.quit = make(chan struct{})
	b.useChecksum = false

	b.tables = make(map[uint64]*TableMapEvent)

	return b
}

func (b *BinlogSyncer) Close() {
	close(b.quit)

	if b.c != nil {
		b.c.Close()
	}

	b.wg.Wait()
}

func (b *BinlogSyncer) checksumUsed() error {
	if r, err := b.c.Execute("SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'"); err != nil {
		return err
	} else {
		s, _ := r.GetString(0, 1)
		if s == "" || s == "NONE" {
			b.useChecksum = false
		} else {
			b.useChecksum = true
		}
	}
	return nil
}

func (b *BinlogSyncer) RegisterSlave(host string, port uint16, user string, password string) error {
	b.host = host
	b.port = port
	b.user = user
	b.password = password

	var err error
	b.c, err = client.Connect(fmt.Sprintf("%s:%d", host, port), user, password, "")
	if err != nil {
		return err
	}

	//for mysql 5.6+, binlog has a crc32 checksum
	//see https://github.com/alibaba/canal/wiki/BinlogChange(mysql5.6)
	//before mysql 5.6, this will not work, don't matter.:-)
	if err = b.checksumUsed(); err != nil {
		return err
	} else if b.useChecksum {
		if _, err = b.c.Execute(`SET @master_binlog_checksum=@@global.binlog_checksum`); err != nil {
			return err
		}
	}

	if err = b.writeRegisterSlaveCommand(); err != nil {
		return err
	}

	if _, err = b.c.ReadOKPacket(); err != nil {
		return err
	}

	return nil
}

func (b *BinlogSyncer) StartSync(fileName string, pos uint32) (*BinlogStreamer, error) {
	err := b.writeBinglogDumpCommand(fileName, pos)
	if err != nil {
		return nil, err
	}

	s := newBinlogStreamer()

	b.wg.Add(1)
	go b.onStream(s)

	return s, nil
}

func (b *BinlogSyncer) StartSyncGTID(gset *GTIDSet) (*BinlogStreamer, error) {
	err := b.writeBinlogDumpGTIDCommand(BINLOG_DUMP_NON_BLOCK|BINLOG_THROUGH_GTID, "", 0, gset.Encode())
	if err != nil {
		return nil, err
	}

	//to do later
	s := newBinlogStreamer()

	b.wg.Add(1)
	go b.onStream(s)

	return s, nil
}

func (b *BinlogSyncer) writeBinglogDumpCommand(fileName string, binlogPos uint32) error {
	//always start from position 4
	if binlogPos < 4 {
		binlogPos = 4
	}

	b.c.ResetSequence()

	data := make([]byte, 4+1+4+2+4+len(fileName))

	pos := 4
	data[pos] = COM_BINLOG_DUMP
	pos++

	binary.LittleEndian.PutUint32(data[pos:], binlogPos)
	pos += 4

	//only support 0x01 BINGLOG_DUMP_NON_BLOCK
	binary.LittleEndian.PutUint16(data[pos:], BINLOG_DUMP_NON_BLOCK)
	pos += 2

	binary.LittleEndian.PutUint32(data[pos:], b.serverID)
	pos += 4

	data = append(data, fileName...)

	return b.c.WritePacket(data)
}

func (b *BinlogSyncer) writeBinlogDumpGTIDCommand(flags uint16, fileName string, binPos int64, gtidData []byte) error {
	b.c.ResetSequence()

	data := make([]byte, 4+1+2+4+4+len(fileName)+8+4+len(gtidData))
	pos := 4
	data[pos] = COM_BINLOG_DUMP_GTID
	pos++

	binary.LittleEndian.PutUint16(data[pos:], flags)
	pos += 2

	binary.LittleEndian.PutUint32(data[pos:], b.serverID)
	pos += 4

	binary.LittleEndian.PutUint32(data[pos:], uint32(len(fileName)))
	pos += 4

	n := copy(data[pos:], fileName)
	pos += n

	binary.LittleEndian.PutUint64(data[pos:], uint64(binPos))
	pos += 8

	if flags&BINLOG_THROUGH_GTID > 0 {
		binary.LittleEndian.PutUint32(data[pos:], uint32(len(gtidData)))
		pos += 4
		n = copy(data[pos:], gtidData)
		pos += n
	}
	data = data[0:pos]

	return b.c.WritePacket(data)
}

func (b *BinlogSyncer) writeRegisterSlaveCommand() error {
	b.c.ResetSequence()

	data := make([]byte, 4+1+4+1+len(b.host)+1+len(b.user)+1+len(b.password)+2+4+4)
	pos := 4

	data[pos] = COM_REGISTER_SLAVE
	pos++

	binary.LittleEndian.PutUint32(data[pos:], b.serverID)
	pos += 4

	data[pos] = uint8(len(b.host))
	pos++
	n := copy(data[pos:], b.host)
	pos += n

	data[pos] = uint8(len(b.user))
	pos++
	n = copy(data[pos:], b.user)
	pos += n

	data[pos] = uint8(len(b.password))
	pos++
	n = copy(data[pos:], b.password)
	pos += n

	binary.LittleEndian.PutUint16(data[pos:], b.port)
	pos += 2

	//replication rank, not used
	binary.LittleEndian.PutUint32(data[pos:], 0)
	pos += 4

	binary.LittleEndian.PutUint32(data[pos:], b.masterID)

	return b.c.WritePacket(data)
}

func (b *BinlogSyncer) onStream(s *BinlogStreamer) {
	defer func() {
		if e := recover(); e != nil {
			s.ech <- fmt.Errorf("Err: %v\n Stack: %s", e, Pstack())
		}
		b.wg.Done()
	}()

	for {
		select {
		case <-b.quit:
			s.ech <- ErrSyncClosed
			return
		default:
			data, err := b.c.ReadPacket()
			if err != nil {
				s.ech <- err
				return
			}

			switch data[0] {
			case OK_HEADER:
				if err = b.parseEvent(s, data); err != nil {
					s.ech <- err
					return
				}
			case ERR_HEADER:
				err = b.c.HandleErrorPacket(data)
				s.ech <- err
			case EOF_HEADER:
				//no binlog now, sleep and wait a moment again
				time.Sleep(500 * time.Millisecond)
			}
		}
	}
}

func (b *BinlogSyncer) parseEvent(s *BinlogStreamer, data []byte) error {
	//skip 0x00
	data = data[1:]

	evData := data

	h := new(EventHeader)
	err := h.Decode(data)
	if err != nil {
		return err
	}

	data = data[EventHeaderSize:]
	eventLen := int(h.EventSize) - EventHeaderSize

	if len(data) < eventLen {
		return fmt.Errorf("invalid data size %d in event %s, less event length %d", len(data), h.EventType, eventLen)
	}

	if b.useChecksum {
		//last 4 bytes is crc32, check later
		data = data[0 : len(data)-4]
	}
	var e Event
	switch h.EventType {
	case FORMAT_DESCRIPTION_EVENT:
		b.format = &FormatDescriptionEvent{}
		e = b.format
	case ROTATE_EVENT:
		e = &RotateEvent{}
	case QUERY_EVENT:
		e = &QueryEvent{}
	case XID_EVENT:
		e = &XIDEvent{}
	case TABLE_MAP_EVENT:
		te := &TableMapEvent{}
		if b.format.EventTypeHeaderLengths[TABLE_MAP_EVENT-1] == 6 {
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
		e = b.newRowsEvent(h)
	case ROWS_QUERY_EVENT:
		e = &RowsQueryEvent{}
	case GTID_EVENT:
		e = &GTIDEvent{}
	default:
		e = &GenericEvent{}
	}

	if err := e.Decode(data); err != nil {
		return &EventError{h, err.Error(), data}
	}

	if te, ok := e.(*TableMapEvent); ok {
		b.tables[te.TableID] = te
	}

	s.ch <- &BinlogEvent{evData, h, e}

	return nil
}

func (b *BinlogSyncer) newRowsEvent(h *EventHeader) *RowsEvent {
	e := &RowsEvent{}
	if b.format.EventTypeHeaderLengths[h.EventType-1] == 6 {
		e.tableIDSize = 4
	} else {
		e.tableIDSize = 6
	}

	e.needBitmap2 = false
	e.tables = b.tables

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
