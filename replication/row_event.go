package replication

type TableMapEvent struct {
	//if post header len == 6
	//table id is 4bytes, else 6 bytes
	TableID []byte

	Flags uint16

	SchemaNameLength uint8
	SchemaName       []byte
	TableNameLength  uint8
	TableName        []byte

	//lenenc-int
	ColumnCount uint64
	ColumnDef   []byte
	//lenenc-str
	ColumnMetaDef []byte

	//len = (ColumnCount + 8) / 7
	NullBitmap []byte
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
