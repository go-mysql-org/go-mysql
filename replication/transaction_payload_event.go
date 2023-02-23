package replication

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/DataDog/zstd"

	. "github.com/go-mysql-org/go-mysql/mysql"
)

// On The Wire: Field Types
const (
	OTW_PAYLOAD_HEADER_END_MARK = iota
	OTW_PAYLOAD_SIZE_FIELD
	OTW_PAYLOAD_COMPRESSION_TYPE_FIELD
	OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD
)

// Compression Types
const (
	ZSTD = 0
	NONE = 255
)

type TransactionPayloadEvent struct {
	parser           *BinlogParser
	Size             uint64
	UncompressedSize uint64
	CompressionType  uint64
	Payload          []byte
	Events           []*BinlogEvent
}

func (e *TransactionPayloadEvent) compressionType() string {
	switch e.CompressionType {
	case ZSTD:
		return "ZSTD"
	case NONE:
		return "NONE"
	default:
		return "Unknown"
	}
}

func (e *TransactionPayloadEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Payload Size: %d\n", e.Size)
	fmt.Fprintf(w, "Payload Uncompressed Size: %d\n", e.UncompressedSize)
	fmt.Fprintf(w, "Payload CompressionType: %s\n", e.compressionType())
	fmt.Fprintf(w, "Payload Body: \n%s", hex.Dump(e.Payload))
	for _, event := range e.Events {
		event.Dump(w)
	}
	fmt.Fprintln(w)
}

func (e *TransactionPayloadEvent) Decode(data []byte) error {
	err := e.decodeFields(data)
	if err != nil {
		return err
	}
	return e.decodePayload()
}

func (e *TransactionPayloadEvent) decodeFields(data []byte) error {
	offset := uint64(0)

	for {
		fieldType := FixedLengthInt(data[offset : offset+1])
		offset++

		if fieldType == OTW_PAYLOAD_HEADER_END_MARK {
			e.Payload = data[offset:]
			break
		} else {
			fieldLength := FixedLengthInt(data[offset : offset+1])
			offset++

			switch fieldType {
			case OTW_PAYLOAD_SIZE_FIELD:
				e.Size = FixedLengthInt(data[offset : offset+fieldLength])
			case OTW_PAYLOAD_COMPRESSION_TYPE_FIELD:
				e.CompressionType = FixedLengthInt(data[offset : offset+fieldLength])
			case OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD:
				e.UncompressedSize = FixedLengthInt(data[offset : offset+fieldLength])
			}

			offset += fieldLength
		}
	}

	return nil
}

func (e *TransactionPayloadEvent) decodePayload() error {
	payloadUncompressed, err := zstd.Decompress(nil, e.Payload)
	if err != nil {
		return err
	}

	// The uncompressed data needs to be split up into individual events for Parse()
	// to work on them. We can't use e.parser directly as we need to disable checksums
	// but we still need the initialization from the FormatDescriptionEvent. We can't
	// modify e.parser as it is used elsewhere.
	parser := NewBinlogParser()
	parser.format = &FormatDescriptionEvent{
		Version:                e.parser.format.Version,
		ServerVersion:          e.parser.format.ServerVersion,
		CreateTimestamp:        e.parser.format.CreateTimestamp,
		EventHeaderLength:      e.parser.format.EventHeaderLength,
		EventTypeHeaderLengths: e.parser.format.EventTypeHeaderLengths,
		ChecksumAlgorithm:      BINLOG_CHECKSUM_ALG_OFF,
	}

	offset := uint32(0)
	for {
		if offset >= uint32(len(payloadUncompressed)) {
			break
		}
		eventLength := binary.LittleEndian.Uint32(payloadUncompressed[offset+9 : offset+13])
		data := payloadUncompressed[offset : offset+eventLength]

		pe, err := parser.Parse(data)
		if err != nil {
			return err
		}
		e.Events = append(e.Events, pe)

		offset += eventLength
	}

	return nil
}
