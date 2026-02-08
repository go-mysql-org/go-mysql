package main

import (
	"flag"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
)

var filename = flag.String("filename", "/tmp/binlog.000001", "Binlog filename")

func main() {
	flag.Parse()

	slog.Info("Writing binlog events", "filename", *filename)

	fh, err := os.OpenFile(*filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		panic(err)
	}
	fh.Write(replication.BinLogFileHeader)

	e := replication.BinlogEvent{
		Header: &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			EventType: replication.FORMAT_DESCRIPTION_EVENT,
			ServerID:  123,
			Flags:     0,
		},
		Event: &replication.FormatDescriptionEvent{
			Version:           4,
			ServerVersion:     "9.6.0",
			CreateTimestamp:   uint32(time.Now().Unix()),
			EventHeaderLength: replication.EventHeaderSize,

			// Array storing the lenghts of event types
			// Taken from MySQL 9.6.0
			EventTypeHeaderLengths: []byte{
				0x00, 0x0d, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00,
				0x04, 0x00, 0x04, 0x00, 0x00, 0x00, 0x63, 0x00,
				0x04, 0x1a, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x02, 0x00, 0x00, 0x00, 0x0a, 0x0a, 0x0a,
				0x2a, 0x19, 0x00, 0x12, 0x34, 0x00, 0x0a, 0x28, // changed GTID_EVENT length to 0x19
				0x00, 0x00,
			},
		},
	}
	pos, err := fh.Seek(0, io.SeekCurrent)
	if err != nil {
		panic("failed to get binlog position")
	}
	e.Write(fh, uint32(pos))

	e = replication.BinlogEvent{
		Header: &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			EventType: replication.PREVIOUS_GTIDS_EVENT,
			ServerID:  123,
			Flags:     replication.LOG_EVENT_IGNORABLE_F,
		},
		Event: &replication.PreviousGTIDsEvent{
			GTIDSets: "9008f957-01e9-11f1-a96a-764efe8146fe:1-5:7:9-10",
		},
	}
	pos, err = fh.Seek(0, io.SeekCurrent)
	if err != nil {
		panic("failed to get binlog position")
	}
	e.Write(fh, uint32(pos))

	e = replication.BinlogEvent{
		Header: &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			EventType: replication.GTID_EVENT,
			ServerID:  123,
			Flags:     0,
		},
		Event: &replication.GTIDEvent{
			CommitFlag: 0x1,
			SID:        []byte{0x90, 0x08, 0xf9, 0x57, 0x01, 0xe9, 0x11, 0xf1, 0xa9, 0x6a, 0x76, 0x4e, 0xfe, 0x81, 0x46, 0xfe},
			GNO:        11,
		},
	}
	pos, err = fh.Seek(0, io.SeekCurrent)
	if err != nil {
		panic("failed to get binlog position")
	}
	e.Write(fh, uint32(pos))

	e = replication.BinlogEvent{
		Header: &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			EventType: replication.QUERY_EVENT,
			ServerID:  123,
			Flags:     0,
		},
		Event: &replication.QueryEvent{
			Schema:        []byte("test"),
			Query:         []byte("create table t1 (id int primary key)"),
			ExecutionTime: 0,
			ErrorCode:     0,
			SlaveProxyID:  9, // thread_id
		},
	}
	pos, err = fh.Seek(0, io.SeekCurrent)
	if err != nil {
		panic("failed to get binlog position")
	}
	e.Write(fh, uint32(pos))

	e = replication.BinlogEvent{
		Header: &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			EventType: replication.GTID_EVENT,
			ServerID:  123,
			Flags:     0,
		},
		Event: &replication.GTIDEvent{
			CommitFlag: 0x1,
			SID:        []byte{0x90, 0x08, 0xf9, 0x57, 0x01, 0xe9, 0x11, 0xf1, 0xa9, 0x6a, 0x76, 0x4e, 0xfe, 0x81, 0x46, 0xfe},
			GNO:        12,
		},
	}
	pos, err = fh.Seek(0, io.SeekCurrent)
	if err != nil {
		panic("failed to get binlog position")
	}
	e.Write(fh, uint32(pos))

	e = replication.BinlogEvent{
		Header: &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			EventType: replication.QUERY_EVENT,
			ServerID:  123,
			Flags:     0,
		},
		Event: &replication.QueryEvent{
			Schema:        []byte("test"),
			Query:         []byte("BEGIN"),
			ExecutionTime: 0,
			ErrorCode:     0,
			SlaveProxyID:  9, // thread_id
		},
	}
	pos, err = fh.Seek(0, io.SeekCurrent)
	if err != nil {
		panic("failed to get binlog position")
	}
	e.Write(fh, uint32(pos))

	e = replication.BinlogEvent{
		Header: &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			EventType: replication.TABLE_MAP_EVENT,
			ServerID:  123,
			Flags:     0,
		},
		Event: &replication.TableMapEvent{
			Schema:           []byte("test"),
			Table:            []byte("t1"),
			TableID:          90,
			Flags:            0x1,
			ColumnCount:      1,
			ColumnType:       []byte{0x03},
			NullBitmap:       []byte{0x0},
			SignednessBitmap: []byte{0x0},
		},
	}
	pos, err = fh.Seek(0, io.SeekCurrent)
	if err != nil {
		panic("failed to get binlog position")
	}
	e.Write(fh, uint32(pos))

	e = replication.BinlogEvent{
		Header: &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			EventType: replication.WRITE_ROWS_EVENTv2,
			ServerID:  123,
			Flags:     0,
		},
		Event: &replication.RowsEvent{
			Version:       2,
			TableID:       90,
			ColumnCount:   1,
			ColumnBitmap1: []byte{0x1},
			Flags:         replication.STMT_END_F,
		},
	}
	pos, err = fh.Seek(0, io.SeekCurrent)
	if err != nil {
		panic("failed to get binlog position")
	}
	e.Write(fh, uint32(pos))

	e = replication.BinlogEvent{
		Header: &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			EventType: replication.XID_EVENT,
			ServerID:  123,
			Flags:     0,
		},
		Event: &replication.XIDEvent{
			XID: 12,
		},
	}
	pos, err = fh.Seek(0, io.SeekCurrent)
	if err != nil {
		panic("failed to get binlog position")
	}
	e.Write(fh, uint32(pos))
}
