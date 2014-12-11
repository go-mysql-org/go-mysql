package replication

import (
	"encoding/binary"
	"fmt"
	. "github.com/siddontang/go-mysql/mysql"
	"io"
)

type TableMapEvent struct {
	tableIDSize int

	TableID uint64

	Flags uint16

	Schema []byte
	Table  []byte

	ColumnCount uint64
	ColumnType  []byte
	ColumnMeta  []uint16

	//len = (ColumnCount + 7) / 8
	NullBitmap []byte
}

func (e *TableMapEvent) Decode(data []byte) error {
	pos := 0
	e.TableID = FixedLengthInt(data[0:e.tableIDSize])
	pos += e.tableIDSize

	e.Flags = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	schemaLength := data[pos]
	pos++

	e.Schema = data[pos : pos+int(schemaLength)]
	pos += int(schemaLength)

	//skip 0x00
	pos++

	tableLength := data[pos]
	pos++

	e.Table = data[pos : pos+int(tableLength)]
	pos += int(tableLength)

	//skip 0x00
	pos++

	var n int
	e.ColumnCount, _, n = LengthEncodedInt(data[pos:])
	pos += n

	e.ColumnType = data[pos : pos+int(e.ColumnCount)]
	pos += int(e.ColumnCount)

	var err error
	var metaData []byte
	if metaData, _, n, err = LengthEnodedString(data[pos:]); err != nil {
		return err
	}

	if err = e.decodeMeta(metaData); err != nil {
		return err
	}

	pos += n

	if len(data[pos:]) != int(e.ColumnCount+7)/8 {
		return io.EOF
	}

	e.NullBitmap = data[pos:]

	return nil
}

func (e *TableMapEvent) decodeMeta(data []byte) error {
	e.ColumnMeta = make([]uint16, e.ColumnCount)
	// to do ......
	// for i, t := range e.ColumnType {
	// 	switch t {

	// 	}
	// }

	return nil
}

func (e *TableMapEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "TableID: %d\n", e.TableID)

}

type RowsEvent struct {
	//if post header len == 6
	//table id is 4bytes, else 6 bytes
	TableID []byte

	Flags uint16

	//if version == 2
	ExtraDataLength uint16
	ExtraData       []byte

	//lenenc_int
	ColumnCount uint64
	//len = (ColumnCount + 7) / 8
	ColumnsPresentBitmap1 []byte

	//if UPDATE_ROWS_EVENTv1 or v2
	//len = (ColumnCount + 7) / 8
	ColumnsPresentBitmap2 []byte

	//rows:
	Rows []Row
}

type Row struct {
	//length (bits set in 'columns-present-bitmap1'+7)/8
	NullBitmap1 []byte

	//value of each field as defined in table-map
	Value1 []byte

	//if UPDATE_ROWS_EVENTv1 or v2
	//length (bits set in 'columns-present-bitmap2'+7)/8
	NullBitmap2 []byte

	//value of each field as defined in table-map
	Value2 []byte
}

type RowsEventExtraData struct {
	Type             uint8
	TypeSpecificData []byte
}

type RowsEventExtraDataInfo struct {
	Length  uint8
	Format  uint8
	PayLoad []byte
}

type RowsEventExtraInfoFormat struct {
	//not used in 5.6.6 yet
}

type RowsQueryEvent struct {
	Length    uint8
	QueryText []byte
}
