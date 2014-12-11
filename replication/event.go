package replication

type EventHeader struct {
	Timestamp uint32
	EventType byte
	ServerID  uint32
	EventSize uint32
	LogPos    uint32
	Flags     uint16
}

type StartEventV3 struct {
	Version         uint16
	ServerVersion   [50]byte
	CreateTimestamp uint32
}

type FormatDescriptionEvent struct {
	Version                uint16
	ServerVersion          [50]byte
	CreateTimestamp        uint32
	EventHeaderLength      uint8
	EventTypeHeaderLengths []byte
}

type RotateEvent struct {
	Position    uint64
	NextLogName []byte
}

type StopEvent struct{}

type QueryEvent struct {
	SlaveProxyID     uint32
	ExecutionTime    uint32
	SchemaLength     uint8
	ErrorCode        uint16
	StatusVarsLength uint16
	StatusVars       []byte
	Schema           []byte
	Query            []byte
}

type LoadEvent struct {
	SlaveProxyID uint32
	ExecTime     uint32
	SkipLines    uint32
	TableNameLen uint8
	SchemaLen    uint8
	NumFileds    uint32
	FieldTerm    uint8
	EnclosedBy   uint8
	LineTerm     uint8
	LineStart    uint8
	EscapedBy    uint8
	OptFlags     uint8
	EmptyFlags   uint8

	//len = 1 * NumFields
	FieldNameLengths []byte

	//len = sum(FieldNameLengths) + NumFields
	//array of nul-terminated strings
	FieldNames []byte

	//len = TableNameLen + 1, nul-terminated string
	TableName []byte

	//len = SchemaLen + 1, nul-terminated string
	SchemaName []byte

	//string.NUL
	FileName []byte
}

type NewLoadEvent struct {
	SlaveProxyID  uint32
	ExecTime      uint32
	SkipLines     uint32
	TableNameLen  uint8
	SchemaLen     uint8
	NumFields     uint32
	FieldTermLen  uint8
	FieldTerm     []byte
	EnclosedByLen uint8
	EnclosedBy    []byte
	LineTermLen   uint8
	LineTerm      []byte
	LineStartLen  uint8
	LineStart     []byte
	EscapedByLen  uint8
	EscapedBy     []byte
	OptFlags      uint8

	//len = 1 * NumFields
	FieldNameLengths []byte

	//len = sum(FieldNameLengths) + NumFields
	//array of nul-terminated strings
	FieldNames []byte

	//len = TableNameLen, nul-terminated string
	TableName []byte

	//len = SchemaLen, nul-terminated string
	SchemaName []byte

	//string.EOF
	FileName []byte
}

type CreateFileEvent struct {
	FileID    uint32
	BlockData []byte
}

type AppendBlockEvent struct {
	FileID    uint32
	BlockData []byte
}

type ExecLoadEvent struct {
	FileID uint32
}

type BeginLoadQueryEvent struct {
	FileID    uint32
	BlockData []byte
}

type ExecuteLoadQueryEvent struct {
	SlaveProxyID     uint32
	ExecutionTime    uint32
	SchemaLength     uint8
	ErrorCode        uint16
	StatusVarsLength uint16

	FileID           uint32
	StartPos         uint32
	EndPos           uint32
	DupHandlingFlags uint8
}

type DeleteFileEvent struct {
	FileID uint32
}

type RandEvent struct {
	Seed1 uint64
	Seed2 uint64
}

type XIDEvent struct {
	XID uint64
}

type IntVarEvent struct {
	Type  uint8
	Value uint64
}

type UserVarEvent struct {
	NameLength uint32
	Name       []byte
	IsNull     uint8

	//if not is null
	Type        uint8
	Charset     uint32
	ValueLength uint32
	Value       []byte

	//if more data
	Flags uint8
}

type IncidentEvent struct {
	Type          uint16
	MessageLength uint8
	Message       []byte
}

type HeartbeatEvent struct {
}

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
