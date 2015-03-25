package replication

import (
	"encoding/binary"
	"fmt"
	"github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/client"
	. "github.com/siddontang/go-mysql/mysql"
	"sync"
	"time"
)

type BinlogSyncer struct {
	flavor string

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

	nextPos Position
}

func NewBinlogSyncer(serverID uint32, flavor string) *BinlogSyncer {
	b := new(BinlogSyncer)
	b.flavor = flavor

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
		b.c.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	}

	b.wg.Wait()

	if b.c != nil {
		b.c.Close()
	}
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

func (b *BinlogSyncer) GetMasterUUID() (uuid.UUID, error) {
	if r, err := b.c.Execute("SHOW GLOBAL VARIABLES LIKE 'SERVER_UUID'"); err != nil {
		return uuid.UUID{}, err
	} else {
		s, _ := r.GetString(0, 1)
		if s == "" || s == "NONE" {
			return uuid.UUID{}, nil
		} else {
			return uuid.FromString(s)
		}
	}
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

func (b *BinlogSyncer) EnableSemiSync() error {
	if r, err := b.c.Execute("SHOW VARIABLES LIKE 'rpl_semi_sync_master_enabled';"); err != nil {
		return err
	} else {
		s, _ := r.GetString(0, 1)
		if s != "ON" {
			return fmt.Errorf("master does not support semi synchronous replication")
		}
	}

	_, err := b.c.Execute(`SET @rpl_semi_sync_slave = 1;`)
	return err
}

func (b *BinlogSyncer) StartSync(pos Position) (*BinlogStreamer, error) {
	//always start from position 4
	if pos.Pos < 4 {
		pos.Pos = 4
	}

	err := b.writeBinglogDumpCommand(pos)
	if err != nil {
		return nil, err
	}

	s := newBinlogStreamer()

	b.wg.Add(1)
	go b.onStream(s)

	return s, nil
}

func (b *BinlogSyncer) StartSyncGTID(gset GTIDSet) (*BinlogStreamer, error) {
	var err error
	switch b.flavor {
	case MySQLFlavor:
		err = b.writeBinlogDumpMysqlGTIDCommand(gset)
	case MariaDBFlavor:
		err = b.writeBinlogDumpMariadbGTIDCommand(gset)
	default:
		err = fmt.Errorf("invalid flavor %s", b.flavor)
	}

	if err != nil {
		return nil, err
	}

	//to do later
	s := newBinlogStreamer()

	b.wg.Add(1)
	go b.onStream(s)

	return s, nil
}

func (b *BinlogSyncer) writeBinglogDumpCommand(p Position) error {
	b.c.ResetSequence()

	data := make([]byte, 4+1+4+2+4+len(p.Name))

	pos := 4
	data[pos] = COM_BINLOG_DUMP
	pos++

	binary.LittleEndian.PutUint32(data[pos:], p.Pos)
	pos += 4

	binary.LittleEndian.PutUint16(data[pos:], BINLOG_DUMP_NEVER_STOP)
	pos += 2

	binary.LittleEndian.PutUint32(data[pos:], b.serverID)
	pos += 4

	copy(data[pos:], p.Name)

	return b.c.WritePacket(data)
}

func (b *BinlogSyncer) writeBinlogDumpMysqlGTIDCommand(gset GTIDSet) error {
	p := Position{"", 4}
	gtidData := gset.Encode()

	b.c.ResetSequence()

	data := make([]byte, 4+1+2+4+4+len(p.Name)+8+4+len(gtidData))
	pos := 4
	data[pos] = COM_BINLOG_DUMP_GTID
	pos++

	binary.LittleEndian.PutUint16(data[pos:], 0)
	pos += 2

	binary.LittleEndian.PutUint32(data[pos:], b.serverID)
	pos += 4

	binary.LittleEndian.PutUint32(data[pos:], uint32(len(p.Name)))
	pos += 4

	n := copy(data[pos:], p.Name)
	pos += n

	binary.LittleEndian.PutUint64(data[pos:], uint64(p.Pos))
	pos += 8

	binary.LittleEndian.PutUint32(data[pos:], uint32(len(gtidData)))
	pos += 4
	n = copy(data[pos:], gtidData)
	pos += n

	data = data[0:pos]

	return b.c.WritePacket(data)
}

func (b *BinlogSyncer) writeBinlogDumpMariadbGTIDCommand(gset GTIDSet) error {
	// Copy from vitess

	startPos := gset.String()

	// Tell the server that we understand GTIDs by setting our slave capability
	// to MARIA_SLAVE_CAPABILITY_GTID = 4 (MariaDB >= 10.0.1).
	if _, err := b.c.Execute("SET @mariadb_slave_capability=4"); err != nil {
		return fmt.Errorf("failed to set @mariadb_slave_capability=4: %v", err)
	}

	// Set the slave_connect_state variable before issuing COM_BINLOG_DUMP to
	// provide the start position in GTID form.
	query := fmt.Sprintf("SET @slave_connect_state='%s'", startPos)

	if _, err := b.c.Execute(query); err != nil {
		return fmt.Errorf("failed to set @slave_connect_state='%s': %v", startPos, err)
	}

	// Real slaves set this upon connecting if their gtid_strict_mode option was
	// enabled. We always use gtid_strict_mode because we need it to make our
	// internal GTID comparisons safe.
	if _, err := b.c.Execute("SET @slave_gtid_strict_mode=1"); err != nil {
		return fmt.Errorf("failed to set @slave_gtid_strict_mode=1: %v", err)
	}

	// Since we use @slave_connect_state, the file and position here are ignored.
	return b.writeBinglogDumpCommand(Position{"", 0})
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

func (b *BinlogSyncer) replySemiSyncACK(p Position) error {
	b.c.ResetSequence()

	data := make([]byte, 4+1+4+len(p.Name))
	pos := 4
	// semi sync indicator
	data[pos] = SemiSyncIndicator
	pos++

	binary.LittleEndian.PutUint32(data[pos:], p.Pos)
	pos += 4

	copy(data[pos:], p.Name)

	err := b.c.WritePacket(data)
	if err != nil {
		return err
	}

	_, err = b.c.ReadOKPacket()
	if err != nil {
	}
	return err
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

	needACK := false
	if data[0] == SemiSyncIndicator {
		needACK = (data[1] == 0x01)
		//skip semi sync header
		data = data[2:]
	}

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

	evData := data

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

	if err := e.Decode(data); err != nil {
		return &EventError{h, err.Error(), data}
	}

	if te, ok := e.(*TableMapEvent); ok {
		b.tables[te.TableID] = te
	}

	lastPos := b.nextPos
	b.nextPos.Pos = h.LogPos

	//If MySQL restart, it may use the same table id for different tables.
	//We must clear the table map before parsing new events.
	//We have no better way to known whether the event is before or after restart,
	//So we have to clear the table map on every rotate event.
	if re, ok := e.(*RotateEvent); ok {
		b.tables = make(map[uint64]*TableMapEvent)
		b.nextPos.Name = string(re.NextLogName)
		b.nextPos.Pos = uint32(re.Position)
	}

	s.ch <- &BinlogEvent{evData, h, e}

	if needACK {
		err := b.replySemiSyncACK(lastPos)
		if err != nil {
			return err
		}
	}

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
