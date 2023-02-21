package replication

import (
	"encoding/hex"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"

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

func fieldTypeName(ft uint64) string {
	switch ft {
	case OTW_PAYLOAD_HEADER_END_MARK:
		return "HeaderEndMark"
	case OTW_PAYLOAD_SIZE_FIELD:
		return "SizeField"
	case OTW_PAYLOAD_COMPRESSION_TYPE_FIELD:
		return "CompressionType"
	case OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD:
		return "UncompressedSize"
	default:
		return "Unknown"
	}
}

type TransactionPayloadEvent struct {
	Data             []byte
	Size             uint64
	UncompressedSize uint64
	CompressionType  uint64
	Payload          []byte
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
	// fmt.Fprintf(w, "Transaction Payload Event data: \n%s", hex.Dump(e.Data))

	decoder, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	payloadUncompressed, _ := decoder.DecodeAll(e.Payload, nil)
	fmt.Fprintf(w, "Decompressed: \n%s", hex.Dump(payloadUncompressed))
	fmt.Fprintln(w)
}

func (e *TransactionPayloadEvent) Decode(data []byte) error {
	e.Data = data
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
