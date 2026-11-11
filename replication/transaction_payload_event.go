package replication

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// On The Wire: Field Types
// See also binary_log::codecs::binary::Transaction_payload::fields in MySQL
// https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1codecs_1_1binary_1_1Transaction__payload.html#a9fff7ac12ba064f40e9216565c53d07b
//
//nolint:revive // OTW_PAYLOAD_* names mirror the upstream MySQL binlog protocol
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
	format FormatDescriptionEvent
	// parent is the BinlogParser that produced this event. The inner
	// parser used to decode the decompressed payload inherits its
	// user-settable options (UseDecimal, RenderJSONAsMySQLText, ...) so
	// that compressed and uncompressed rows decode identically. The
	// parent also owns the decoder pool used by decodePayload, so it
	// must be set before decoding.
	parent           *BinlogParser
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
	fmt.Fprintln(w, "=== Start of events decoded from compressed payload ===")
	for _, event := range e.Events {
		event.Dump(w)
	}
	fmt.Fprintln(w, "=== End of events decoded from compressed payload ===")
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
	// MySQL writes the field type, the field length and the value with
	// net_store_length (Transaction_payload::encode), so each of them is a
	// length-encoded integer, not a fixed-width one.
	for {
		fieldType, n, err := payloadLengthEncodedInt(data)
		if err != nil {
			return err
		}
		data = data[n:]
		if fieldType == OTW_PAYLOAD_HEADER_END_MARK {
			e.Payload = data
			return nil
		}
		fieldLength, n, err := payloadLengthEncodedInt(data)
		if err != nil {
			return err
		}
		data = data[n:]
		if fieldLength > uint64(len(data)) {
			return errors.New("TransactionPayloadEvent: header field runs past the event")
		}
		field := data[:fieldLength]
		data = data[fieldLength:]

		var dst *uint64
		switch fieldType {
		case OTW_PAYLOAD_SIZE_FIELD:
			dst = &e.Size
		case OTW_PAYLOAD_COMPRESSION_TYPE_FIELD:
			dst = &e.CompressionType
		case OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD:
			dst = &e.UncompressedSize
		default:
			// Unknown fields are skipped by their length, as MySQL does.
			continue
		}
		if *dst, _, err = payloadLengthEncodedInt(field); err != nil {
			return err
		}
	}
}

// payloadLengthEncodedInt decodes one net_store_length integer and returns
// it with the number of bytes it occupies. Unlike mysql.LengthEncodedInt it
// checks that the bytes are there.
func payloadLengthEncodedInt(b []byte) (uint64, int, error) {
	if len(b) == 0 {
		return 0, 0, errors.New("TransactionPayloadEvent: truncated header")
	}
	need := 1
	switch b[0] {
	case 0xfb, 0xff:
		return 0, 0, fmt.Errorf("TransactionPayloadEvent: invalid length-encoded integer 0x%x", b[0])
	case 0xfc:
		need = 3
	case 0xfd:
		need = 4
	case 0xfe:
		need = 9
	}
	if len(b) < need {
		return 0, 0, errors.New("TransactionPayloadEvent: truncated header")
	}
	v, _, n := mysql.LengthEncodedInt(b)
	return v, n, nil
}

// stampInnerEventPositions fixes LogPos of decoded inner events. MySQL copies
// them from transaction cache before position fixup, so they arrive with
// LogPos=0. Stamp checkpoint-equivalent positions: payload start for
// mid-transaction events (resume there replays whole transaction, matching
// uncompressed semantics), payload end for final event (XID/COMMIT, resume
// there skips transaction)
func (e *TransactionPayloadEvent) stampInnerEventPositions(h *EventHeader) {
	if len(e.Events) == 0 || h.LogPos < h.EventSize {
		return
	}
	start := h.LogPos - h.EventSize
	for _, inner := range e.Events {
		inner.Header.LogPos = start
	}
	e.Events[len(e.Events)-1].Header.LogPos = h.LogPos
}

func (e *TransactionPayloadEvent) decodePayload() error {
	if e.CompressionType != ZSTD {
		return fmt.Errorf("TransactionPayloadEvent has compression type %d (%s)",
			e.CompressionType, e.compressionType())
	}

	// The parser caches one zstd decoder across events instead of
	// building a new decoder per event; getPayloadDecoder reports
	// creation errors.
	decoder, err := e.parent.getPayloadDecoder()
	if err != nil {
		return err
	}
	payloadUncompressed, err := decoder.DecodeAll(e.Payload, nil)
	if err != nil {
		// Be conservative: close and drop the decoder instead of
		// returning it to the cache.
		decoder.Close()
		return err
	}
	e.parent.putPayloadDecoder(decoder)

	// The uncompressed data needs to be split up into individual events for Parse()
	// to work on them. We can't use the parent parser directly as we need to disable
	// checksums but we still need the initialization from the FormatDescriptionEvent.
	// We can't modify the parent parser as it is used elsewhere. We do however want
	// to inherit user-settable decode options (UseDecimal, RenderJSONAsMySQLText,
	// IgnoreJSONDecodeError, ...) so that rows inside a compressed payload decode
	// with the same semantics as uncompressed rows.
	var parser *BinlogParser
	if e.parent != nil {
		parser = e.parent.cloneForPayloadDecode()
	} else {
		parser = NewBinlogParser()
	}
	parser.format = &FormatDescriptionEvent{
		Version:                e.format.Version,
		ServerVersion:          e.format.ServerVersion,
		CreateTimestamp:        e.format.CreateTimestamp,
		EventHeaderLength:      e.format.EventHeaderLength,
		EventTypeHeaderLengths: e.format.EventTypeHeaderLengths,
		ChecksumAlgorithm:      BINLOG_CHECKSUM_ALG_OFF,
	}

	offset := uint32(0)
	for {
		payloadUncompressedLength := uint32(len(payloadUncompressed))
		if offset+13 > payloadUncompressedLength {
			break
		}
		eventLength := binary.LittleEndian.Uint32(payloadUncompressed[offset+9 : offset+13])
		if offset+eventLength > payloadUncompressedLength {
			return fmt.Errorf("Event length of %d with offset %d in uncompressed payload exceeds payload length of %d",
				eventLength, offset, payloadUncompressedLength)
		}
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
