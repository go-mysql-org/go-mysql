package replication

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/serialization"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
)

const (
	EventHeaderSize            = 19
	SidLength                  = 16
	LogicalTimestampTypeCode   = 2
	PartLogicalTimestampLength = 8
	BinlogChecksumLength       = 4
	UndefinedServerVer         = 999999 // UNDEFINED_SERVER_VERSION
)

type BinlogEvent struct {
	// raw binlog data which contains all data, including binlog header and event body, and including crc32 checksum if exists
	RawData []byte

	Header *EventHeader
	Event  Event
}

func (e *BinlogEvent) Dump(w io.Writer) {
	e.Header.Dump(w)
	e.Event.Dump(w)
}

type Event interface {
	// Dump Event, format like python-mysql-replication
	Dump(w io.Writer)

	Decode(data []byte) error
}

type EventError struct {
	Header *EventHeader

	// Error message
	Err string

	// Event data
	Data []byte
}

func (e *EventError) Error() string {
	return fmt.Sprintf("Header %#v, Data %q, Err: %v", e.Header, e.Data, e.Err)
}

type EventHeader struct {
	Timestamp uint32
	EventType EventType
	ServerID  uint32
	EventSize uint32
	LogPos    uint32
	Flags     uint16
}

func (h *EventHeader) Decode(data []byte) error {
	if len(data) < EventHeaderSize {
		return errors.Errorf("header size too short %d, must 19", len(data))
	}

	pos := 0

	h.Timestamp = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	h.EventType = EventType(data[pos])
	pos++

	h.ServerID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	h.EventSize = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	h.LogPos = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	h.Flags = binary.LittleEndian.Uint16(data[pos:])
	// pos += 2

	if h.EventSize < uint32(EventHeaderSize) {
		return errors.Errorf("invalid event size %d, must >= 19", h.EventSize)
	}

	return nil
}

// headerFlagsString is returning a pipe separated string with flag names
func headerFlagsString(flags uint16) string {
	var flagstr []string

	if (flags & LOG_EVENT_BINLOG_IN_USE_F) != 0 {
		flagstr = append(flagstr, "IN_USE")
	}
	if (flags & LOG_EVENT_FORCED_ROTATE_F) != 0 {
		flagstr = append(flagstr, "ROTATE")
	}
	if (flags & LOG_EVENT_THREAD_SPECIFIC_F) != 0 {
		flagstr = append(flagstr, "THREAD_SPECIFIC")
	}
	if (flags & LOG_EVENT_SUPPRESS_USE_F) != 0 {
		flagstr = append(flagstr, "SUPPRESS_USE")
	}
	if (flags & LOG_EVENT_UPDATE_TABLE_MAP_VERSION_F) != 0 {
		flagstr = append(flagstr, "UPDATE_TABLE_MAP_VERSION")
	}
	if (flags & LOG_EVENT_ARTIFICIAL_F) != 0 {
		flagstr = append(flagstr, "ARTIFICIAL")
	}
	if (flags & LOG_EVENT_RELAY_LOG_F) != 0 {
		flagstr = append(flagstr, "RELAY_LOG")
	}
	if (flags & LOG_EVENT_IGNORABLE_F) != 0 {
		flagstr = append(flagstr, "IGNORABLE")
	}
	if (flags & LOG_EVENT_NO_FILTER_F) != 0 {
		flagstr = append(flagstr, "NO_FILTER")
	}
	if (flags & LOG_EVENT_MTS_ISOLATE_F) != 0 {
		flagstr = append(flagstr, "MTS_ISOLATE")
	}

	return strings.Join(flagstr, "|")
}

func (h *EventHeader) Dump(w io.Writer) {
	fmt.Fprintf(w, "=== %s ===\n", h.EventType)
	fmt.Fprintf(w, "Date: %s\n", time.Unix(int64(h.Timestamp), 0).Format(mysql.TimeFormat))
	fmt.Fprintf(w, "Log position: %d\n", h.LogPos)
	fmt.Fprintf(w, "Event size: %d\n", h.EventSize)
	fmt.Fprintf(w, "Header Flags: %s\n", headerFlagsString(h.Flags))
}

var (
	checksumVersionSplitMysql   = []int{5, 6, 1}
	checksumVersionProductMysql = (checksumVersionSplitMysql[0]*256+checksumVersionSplitMysql[1])*256 + checksumVersionSplitMysql[2]

	checksumVersionSplitMariaDB   = []int{5, 3, 0}
	checksumVersionProductMariaDB = (checksumVersionSplitMariaDB[0]*256+checksumVersionSplitMariaDB[1])*256 + checksumVersionSplitMariaDB[2]
)

// server version format X.Y.Zabc, a is not . or number
func splitServerVersion(server string) []int {
	seps := strings.Split(server, ".")
	if len(seps) < 3 {
		return []int{0, 0, 0}
	}

	x, _ := strconv.Atoi(seps[0])
	y, _ := strconv.Atoi(seps[1])

	index := 0
	for i, c := range seps[2] {
		if !unicode.IsNumber(c) {
			index = i
			break
		}
	}

	z, _ := strconv.Atoi(seps[2][0:index])

	return []int{x, y, z}
}

func calcVersionProduct(server string) int {
	versionSplit := splitServerVersion(server)

	return (versionSplit[0]*256+versionSplit[1])*256 + versionSplit[2]
}

type FormatDescriptionEvent struct {
	Version                uint16
	ServerVersion          string
	CreateTimestamp        uint32
	EventHeaderLength      uint8
	EventTypeHeaderLengths []byte

	// 0 is off, 1 is for CRC32, 255 is undefined
	ChecksumAlgorithm BinlogChecksum
}

func (e *FormatDescriptionEvent) Decode(data []byte) error {
	pos := 0
	e.Version = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	serverVersionRaw := make([]byte, 50)
	copy(serverVersionRaw, data[pos:])
	pos += 50

	e.CreateTimestamp = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	e.EventHeaderLength = data[pos]
	pos++

	if e.EventHeaderLength != byte(EventHeaderSize) {
		return errors.Errorf("invalid event header length %d, must 19", e.EventHeaderLength)
	}

	serverVersionLength := bytes.Index(serverVersionRaw, []byte{0x0})
	if serverVersionLength < 0 {
		e.ServerVersion = string(serverVersionRaw)
	} else {
		e.ServerVersion = string(serverVersionRaw[:serverVersionLength])
	}
	checksumProduct := checksumVersionProductMysql
	if strings.Contains(strings.ToLower(e.ServerVersion), "mariadb") {
		checksumProduct = checksumVersionProductMariaDB
	}

	if calcVersionProduct(e.ServerVersion) >= checksumProduct {
		// here, the last 5 bytes is 1 byte check sum alg type and 4 byte checksum if exists
		e.ChecksumAlgorithm = BinlogChecksum(data[len(data)-5])
		e.EventTypeHeaderLengths = data[pos : len(data)-5]
	} else {
		e.ChecksumAlgorithm = BINLOG_CHECKSUM_ALG_UNDEF
		e.EventTypeHeaderLengths = data[pos:]
	}

	return nil
}

func (e *FormatDescriptionEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Version: %d\n", e.Version)
	fmt.Fprintf(w, "Server version: %s\n", e.ServerVersion)
	// fmt.Fprintf(w, "Create date: %s\n", time.Unix(int64(e.CreateTimestamp), 0).Format(TimeFormat))
	fmt.Fprintf(w, "Checksum algorithm: %s\n", e.ChecksumAlgorithm)
	// fmt.Fprintf(w, "Event header lengths: \n%s", hex.Dump(e.EventTypeHeaderLengths))
	fmt.Fprintln(w)
}

type RotateEvent struct {
	Position    uint64
	NextLogName []byte
}

func (e *RotateEvent) Decode(data []byte) error {
	e.Position = binary.LittleEndian.Uint64(data[0:])
	e.NextLogName = data[8:]

	return nil
}

func (e *RotateEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Position: %d\n", e.Position)
	fmt.Fprintf(w, "Next log name: %s\n", e.NextLogName)
	fmt.Fprintln(w)
}

type PreviousGTIDsEvent struct {
	GTIDSets string
}

type GtidFormat int

const (
	GtidFormatClassic = iota
	GtidFormatTagged
)

// Decode the number of sids (source identifiers) and if it is using
// tagged GTIDs or classic (non-tagged) GTIDs.
//
// Note that each gtid tag increases the sidno here, so a single UUID
// might turn up multiple times if there are multipl tags.
//
// see also:
// decode_nsids_format in mysql/mysql-server
// https://github.com/mysql/mysql-server/blob/61a3a1d8ef15512396b4c2af46e922a19bf2b174/sql/rpl_gtid_set.cc#L1363-L1378
func decodeSid(data []byte) (format GtidFormat, sidnr uint64) {
	if data[7] == 1 {
		format = GtidFormatTagged
	}

	if format == GtidFormatTagged {
		masked := make([]byte, 8)
		copy(masked, data[1:7])
		sidnr = binary.LittleEndian.Uint64(masked)
		return format, sidnr
	}
	sidnr = binary.LittleEndian.Uint64(data[:8])
	return format, sidnr
}

func (e *PreviousGTIDsEvent) Decode(data []byte) error {
	pos := 0

	format, uuidCount := decodeSid(data)
	pos += 8

	previousGTIDSets := make([]string, uuidCount)

	currentSetnr := 0
	var buf strings.Builder
	for range previousGTIDSets {
		uuid := e.decodeUuid(data[pos : pos+16])
		pos += 16
		var tag string
		if format == GtidFormatTagged {
			tagLength := int(data[pos]) / 2
			pos += 1
			if tagLength > 0 { // 0 == no tag, >0 == tag
				tag = string(data[pos : pos+tagLength])
				pos += tagLength
			}
		}

		if len(tag) > 0 {
			buf.WriteString(":")
			buf.WriteString(tag)
		} else {
			if currentSetnr != 0 {
				buf.WriteString(",")
			}
			buf.WriteString(uuid)
			currentSetnr += 1
		}

		sliceCount := binary.LittleEndian.Uint16(data[pos : pos+8])
		pos += 8
		for range sliceCount {
			buf.WriteString(":")

			start := e.decodeInterval(data[pos : pos+8])
			pos += 8
			stop := e.decodeInterval(data[pos : pos+8])
			pos += 8
			if stop == start+1 {
				fmt.Fprintf(&buf, "%d", start)
			} else {
				fmt.Fprintf(&buf, "%d-%d", start, stop-1)
			}
		}
		if len(tag) == 0 {
			currentSetnr += 1
		}
	}
	e.GTIDSets = buf.String()
	return nil
}

func (e *PreviousGTIDsEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Previous GTID Event: %s\n", e.GTIDSets)
	fmt.Fprintln(w)
}

func (e *PreviousGTIDsEvent) decodeUuid(data []byte) string {
	return fmt.Sprintf("%s-%s-%s-%s-%s", hex.EncodeToString(data[0:4]), hex.EncodeToString(data[4:6]),
		hex.EncodeToString(data[6:8]), hex.EncodeToString(data[8:10]), hex.EncodeToString(data[10:]))
}

func (e *PreviousGTIDsEvent) decodeInterval(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data)
}

type XIDEvent struct {
	XID uint64

	// in fact XIDEvent dosen't have the GTIDSet information, just for beneficial to use
	GSet mysql.GTIDSet
}

func (e *XIDEvent) Decode(data []byte) error {
	e.XID = binary.LittleEndian.Uint64(data)
	return nil
}

func (e *XIDEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "XID: %d\n", e.XID)
	if e.GSet != nil {
		fmt.Fprintf(w, "GTIDSet: %s\n", e.GSet.String())
	}
	fmt.Fprintln(w)
}

type QueryEvent struct {
	SlaveProxyID  uint32
	ExecutionTime uint32
	ErrorCode     uint16
	StatusVars    []QueryEventStatusVar
	Schema        []byte
	Query         []byte

	// for mariadb QUERY_COMPRESSED_EVENT
	compressed bool

	// in fact QueryEvent dosen't have the GTIDSet information, just for beneficial to use
	GSet mysql.GTIDSet
}

func (e *QueryEvent) Decode(data []byte) error {
	pos := 0

	e.SlaveProxyID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	e.ExecutionTime = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	schemaLength := data[pos]
	pos++

	e.ErrorCode = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	statusVarsLength := binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	e.decodeStatusVars(data[pos : pos+int(statusVarsLength)])
	pos += int(statusVarsLength)

	e.Schema = data[pos : pos+int(schemaLength)]
	pos += int(schemaLength)

	// skip 0x00
	pos++

	if e.compressed {
		decompressedQuery, err := mysql.DecompressMariadbData(data[pos:])
		if err != nil {
			return err
		}
		e.Query = decompressedQuery
	} else {
		e.Query = data[pos:]
	}
	return nil
}

// See also
//   - Query_event_status_vars in
//     https://github.com/mysql/mysql-server/blob/trunk/libs/mysql/binlog/event/statement_events.h
//   - https://dev.mysql.com/doc/dev/mysql-server/latest/classmysql_1_1binlog_1_1event_1_1Query__event.html#details
//   - https://mariadb.com/docs/server/reference/clientserver-protocol/replication-protocol/query_event
type QueryEventStatusVar struct {
	VarType  int
	VarValue any
}

func (sv QueryEventStatusVar) String() string {
	retval := ""
	switch sv.VarType {
	case Q_FLAGS2_CODE:
		retval += "FLAGS2"
	case Q_SQL_MODE_CODE:
		retval += "SQL_MODE"
	case Q_CATALOG_CODE:
		retval += "CATALOG"
	case Q_AUTO_INCREMENT:
		retval += "AUTO_INCREMENT"
	case Q_CHARSET_CODE:
		retval += "CHARSET"
	case Q_TIME_ZONE_CODE:
		retval += "TIME_ZONE"
	case Q_CATALOG_NZ_CODE:
		retval += "CATALOG_NZ"
	case Q_LC_TIME_NAMES_CODE:
		retval += "LC_TIME_NAMES"
	case Q_CHARSET_DATABASE_CODE:
		retval += "CHARSET_DATABASE"
	case Q_TABLE_MAP_FOR_UPDATE_CODE:
		retval += "TABLE_MAP_FOR_UPDATE"
	case Q_MASTER_DATA_WRITTEN_CODE:
		retval += "MASTER_DATA_WRITTEN"
	case Q_INVOKE:
		retval += "INVOKE"
	case Q_UPDATED_DB_NAMES:
		retval += "UPDATED_DB_NAMES"
	case Q_MICROSECONDS:
		retval += "MICROSECONDS"
	case Q_COMMIT_TS:
		retval += "COMMIT_TS"
	case Q_COMMIT_TS2:
		retval += "COMMIT_TS2"
	case Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP:
		retval += "EXPLICIT_DEFAULTS_FOR_TIMESTAMP"
	case Q_DDL_LOGGED_WITH_XID:
		retval += "DDL_LOGGED_WITH_XID"
	case Q_DEFAULT_COLLATION_FOR_UTF8MB4:
		retval += "DEFAULT_COLLATION_FOR_UTF8MB4"
	case Q_SQL_REQUIRE_PRIMARY_KEY:
		retval += "SQL_REQUIRE_PRIMARY_KEY"
	case Q_DEFAULT_TABLE_ENCRYPTION:
		retval += "DEFAULT_TABLE_ENCRYPTION"
	case Q_HRNOW:
		retval += "HRNOW"
	case Q_XID:
		retval += "XID"
	}
	retval += fmt.Sprintf(" = %v", sv.VarValue)
	return retval
}

type QFlags uint32

func (f QFlags) String() string {
	var flags []string

	if (uint32(f) & OPTION_AUTO_IS_NULL) != 0 {
		flags = append(flags, "AUTO_IS_NULL")
	}

	if (uint32(f) & OPTION_NOT_AUTOCOMMIT) != 0 {
		flags = append(flags, "NOT_AUTOCOMMIT")
	}

	if (uint32(f) & OPTION_NO_FOREIGN_KEY_CHECKS) != 0 {
		flags = append(flags, "NO_FOREIGN_KEY_CHECKS")
	}

	if (uint32(f) & OPTION_RELAXED_UNIQUE_CHECKS) != 0 {
		flags = append(flags, "RELAXED_UNIQUE_CHECKS")
	}

	return strings.Join(flags, ",")
}

type SQLMode uint64

func (m SQLMode) String() string {
	var modes []string

	if uint64(m)&MODE_REAL_AS_FLOAT != 0 {
		modes = append(modes, "REAL_AS_FLOAT")
	}
	if uint64(m)&MODE_PIPES_AS_CONCAT != 0 {
		modes = append(modes, "PIPES_AS_CONCAT")
	}
	if uint64(m)&MODE_ANSI_QUOTES != 0 {
		modes = append(modes, "ANSI_QUOTES")
	}
	if uint64(m)&MODE_IGNORE_SPACE != 0 {
		modes = append(modes, "IGNORE_SPACE")
	}
	if uint64(m)&MODE_NOT_USED != 0 {
		modes = append(modes, "NOT_USED")
	}
	if uint64(m)&MODE_ONLY_FULL_GROUP_BY != 0 {
		modes = append(modes, "ONLY_FULL_GROUP_BY")
	}
	if uint64(m)&MODE_NO_UNSIGNED_SUBTRACTION != 0 {
		modes = append(modes, "NO_UNSIGNED_SUBTRACTION")
	}
	if uint64(m)&MODE_NO_DIR_IN_CREATE != 0 {
		modes = append(modes, "NO_DIR_IN_CREATE")
	}
	if uint64(m)&MODE_ANSI != 0 {
		modes = append(modes, "ANSI")
	}
	if uint64(m)&MODE_NO_AUTO_VALUE_ON_ZERO != 0 {
		modes = append(modes, "NO_AUTO_VALUE_ON_ZERO")
	}
	if uint64(m)&MODE_NO_BACKSLASH_ESCAPES != 0 {
		modes = append(modes, "NO_BACKSLASH_ESCAPES")
	}
	if uint64(m)&MODE_STRICT_TRANS_TABLES != 0 {
		modes = append(modes, "STRICT_TRANS_TABLES")
	}
	if uint64(m)&MODE_STRICT_ALL_TABLES != 0 {
		modes = append(modes, "STRICT_ALL_TABLES")
	}
	if uint64(m)&MODE_NO_ZERO_IN_DATE != 0 {
		modes = append(modes, "NO_ZERO_IN_DATE")
	}
	if uint64(m)&MODE_NO_ZERO_DATE != 0 {
		modes = append(modes, "NO_ZERO_DATE")
	}
	if uint64(m)&MODE_INVALID_DATES != 0 {
		modes = append(modes, "INVALID_DATES")
	}
	if uint64(m)&MODE_ERROR_FOR_DIVISION_BY_ZERO != 0 {
		modes = append(modes, "ERROR_FOR_DIVISION_BY_ZERO")
	}
	if uint64(m)&MODE_TRADITIONAL != 0 {
		modes = append(modes, "TRADITIONAL")
	}
	if uint64(m)&MODE_HIGH_NOT_PRECEDENCE != 0 {
		modes = append(modes, "HIGH_NOT_PRECEDENCE")
	}
	if uint64(m)&MODE_PAD_CHAR_TO_FULL_LENGTH != 0 {
		modes = append(modes, "PAD_CHAR_TO_FULL_LENGTH")
	}
	if uint64(m)&MODE_TIME_TRUNCATE_FRACTIONAL != 0 {
		modes = append(modes, "TIME_TRUNCATE_FRACTIONAL")
	}
	if uint64(m)&MODE_INTERPRET_UTF8_AS_UTF8MB4 != 0 {
		modes = append(modes, "INTERPRET_UTF8_AS_UTF8MB4")
	}

	return strings.Join(modes, ",")
}

func (e *QueryEvent) decodeStatusVars(data []byte) {
	pos := 0

LOOP1:
	for pos < len(data) {
		switch data[pos] {
		case Q_FLAGS2_CODE:
			pos++
			e.StatusVars = append(e.StatusVars, QueryEventStatusVar{
				VarType:  Q_FLAGS2_CODE,
				VarValue: QFlags(binary.LittleEndian.Uint32(data[pos:])),
			})
			pos += 4
		case Q_SQL_MODE_CODE:
			pos++
			e.StatusVars = append(e.StatusVars, QueryEventStatusVar{
				VarType:  Q_SQL_MODE_CODE,
				VarValue: SQLMode(binary.LittleEndian.Uint64(data[pos:])),
			})
			pos += 8
		case Q_CATALOG_CODE: // MySQL 5.0.0-5.0.3 catalog with trailing zero, replaced by Q_CATALOG_NZ_CODE
			pos++
			catlen := int(data[pos])
			pos++
			e.StatusVars = append(e.StatusVars, QueryEventStatusVar{
				VarType:  Q_CATALOG_CODE,
				VarValue: string(data[pos : pos+catlen]),
			})
			pos += catlen
		case Q_AUTO_INCREMENT:
			pos++
			e.StatusVars = append(e.StatusVars, QueryEventStatusVar{
				VarType:  Q_AUTO_INCREMENT,
				VarValue: data[pos : pos+4],
			})
			pos += 4
		case Q_CHARSET_CODE:
			pos++
			e.StatusVars = append(e.StatusVars, QueryEventStatusVar{
				VarType: Q_CHARSET_CODE,
				VarValue: [3]uint16{
					binary.LittleEndian.Uint16(data[pos:]),
					binary.LittleEndian.Uint16(data[pos+2:]),
					binary.LittleEndian.Uint16(data[pos+4:]),
				},
			})
			pos += 6
		case Q_TIME_ZONE_CODE:
			pos++
			tzlen := int(data[pos])
			pos++
			e.StatusVars = append(e.StatusVars, QueryEventStatusVar{
				VarType:  Q_TIME_ZONE_CODE,
				VarValue: data[pos : pos+tzlen],
			})
			pos += tzlen
		case Q_CATALOG_NZ_CODE:
			pos++
			catlen := int(data[pos])
			pos++
			e.StatusVars = append(e.StatusVars, QueryEventStatusVar{
				VarType:  Q_CATALOG_NZ_CODE,
				VarValue: string(data[pos : pos+catlen]),
			})
			pos += catlen
		case Q_LC_TIME_NAMES_CODE:
			pos++
			e.StatusVars = append(e.StatusVars, QueryEventStatusVar{
				VarType:  Q_LC_TIME_NAMES_CODE,
				VarValue: data[pos : pos+2],
			})
			pos += 2
		case Q_CHARSET_DATABASE_CODE:
			pos++
			e.StatusVars = append(e.StatusVars, QueryEventStatusVar{
				VarType:  Q_CHARSET_DATABASE_CODE,
				VarValue: data[pos : pos+2],
			})
			pos += 2
		case Q_TABLE_MAP_FOR_UPDATE_CODE:
			pos++
			e.StatusVars = append(e.StatusVars, QueryEventStatusVar{
				VarType:  Q_TABLE_MAP_FOR_UPDATE_CODE,
				VarValue: binary.LittleEndian.Uint64(data[pos:]),
			})
			pos += 8
		case Q_MASTER_DATA_WRITTEN_CODE:
			pos++
			e.StatusVars = append(e.StatusVars, QueryEventStatusVar{
				VarType:  Q_TABLE_MAP_FOR_UPDATE_CODE,
				VarValue: data[pos : pos+4],
			})
			pos += 4
		case Q_INVOKE:
			// <len><user><len><host>
			pos++
			userLen := int(data[pos])
			hostLen := int(data[pos+1+userLen])
			e.StatusVars = append(e.StatusVars, QueryEventStatusVar{
				VarType:  Q_INVOKE,
				VarValue: data[pos : pos+1+userLen+1+hostLen],
			})
			pos += 1 + userLen + 1 + hostLen
		case Q_UPDATED_DB_NAMES:
			// 1 byte count of values
			// followed by null terminated values
			pos++
			namecnt := int(data[pos])
			pos++
			var names []string
			for range namecnt {
				namelen := bytes.Index(data[pos:], []byte{0x0})
				names = append(names, string(data[pos:pos+namelen]))
				pos += namelen
			}
			e.StatusVars = append(e.StatusVars, QueryEventStatusVar{
				VarType:  Q_UPDATED_DB_NAMES,
				VarValue: names,
			})
			pos++
		case Q_MICROSECONDS:
			pos++
			e.StatusVars = append(e.StatusVars, QueryEventStatusVar{
				VarType:  Q_MICROSECONDS,
				VarValue: data[pos : pos+3],
			})
			pos += 3
		case Q_COMMIT_TS:
			pos++
			e.StatusVars = append(e.StatusVars, QueryEventStatusVar{
				VarType:  Q_COMMIT_TS,
				VarValue: data[pos : pos+8],
			})
			pos += 8
		case Q_COMMIT_TS2:
			pos++
			e.StatusVars = append(e.StatusVars, QueryEventStatusVar{
				VarType:  Q_COMMIT_TS2,
				VarValue: data[pos : pos+8],
			})
			pos += 8
		case Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP:
			pos++
			e.StatusVars = append(e.StatusVars, QueryEventStatusVar{
				VarType:  Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP,
				VarValue: data[pos : pos+1],
			})
			pos++
		case Q_DDL_LOGGED_WITH_XID:
			pos++
			e.StatusVars = append(e.StatusVars, QueryEventStatusVar{
				VarType:  Q_DDL_LOGGED_WITH_XID,
				VarValue: binary.LittleEndian.Uint64(data[pos:]),
			})
			pos += 8
		case Q_DEFAULT_COLLATION_FOR_UTF8MB4:
			pos++
			e.StatusVars = append(e.StatusVars, QueryEventStatusVar{
				VarType:  Q_DEFAULT_COLLATION_FOR_UTF8MB4,
				VarValue: binary.LittleEndian.Uint16(data[pos:]),
			})
			pos += 2
		case Q_SQL_REQUIRE_PRIMARY_KEY:
			pos++
			e.StatusVars = append(e.StatusVars, QueryEventStatusVar{
				VarType:  Q_SQL_REQUIRE_PRIMARY_KEY,
				VarValue: bool(data[pos] == 0x1), // second byte is the type
			})
			pos += 2
		case Q_DEFAULT_TABLE_ENCRYPTION:
			pos++
			e.StatusVars = append(e.StatusVars, QueryEventStatusVar{
				VarType:  Q_DEFAULT_TABLE_ENCRYPTION,
				VarValue: bool(data[pos] == 0x1), // second byte is the type
			})
			pos += 2
		case Q_HRNOW:
			pos++
			e.StatusVars = append(e.StatusVars, QueryEventStatusVar{
				VarType:  Q_HRNOW,
				VarValue: data[pos : pos+3],
			})
			pos += 3
		case Q_XID:
			pos++
			e.StatusVars = append(e.StatusVars, QueryEventStatusVar{
				VarType:  Q_XID,
				VarValue: binary.LittleEndian.Uint64(data[pos:]),
			})
			pos += 8
		default:
			slog.Warn("failed to decode query event variable", "type", data[pos])
			break LOOP1
		}
	}
}

func (e *QueryEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Slave proxy ID: %d\n", e.SlaveProxyID)
	fmt.Fprintf(w, "Execution time: %d\n", e.ExecutionTime)
	fmt.Fprintf(w, "Error code: %d\n", e.ErrorCode)
	for _, sv := range e.StatusVars {
		fmt.Fprintf(w, "Query Status var: %s\n", sv)
	}
	fmt.Fprintf(w, "Schema: %s\n", e.Schema)
	fmt.Fprintf(w, "Query: %s\n", e.Query)
	if e.GSet != nil {
		fmt.Fprintf(w, "GTIDSet: %s\n", e.GSet.String())
	}
	fmt.Fprintln(w)
}

type GTIDEvent struct {
	CommitFlag     uint8
	SID            []byte
	Tag            string
	GNO            int64
	LastCommitted  int64
	SequenceNumber int64

	// ImmediateCommitTimestamp/OriginalCommitTimestamp are introduced in MySQL-8.0.1, see:
	// https://dev.mysql.com/blog-archive/new-monitoring-replication-features-and-more
	ImmediateCommitTimestamp uint64
	OriginalCommitTimestamp  uint64

	// Total transaction length (including this GTIDEvent), introduced in MySQL-8.0.2, see:
	// https://dev.mysql.com/blog-archive/taking-advantage-of-new-transaction-length-metadata
	TransactionLength uint64

	// ImmediateServerVersion/OriginalServerVersion are introduced in MySQL-8.0.14, see
	// https://dev.mysql.com/doc/refman/8.0/en/replication-compatibility.html
	ImmediateServerVersion uint32
	OriginalServerVersion  uint32
}

func (e *GTIDEvent) Decode(data []byte) error {
	pos := 0
	e.CommitFlag = data[pos]
	pos++
	e.SID = data[pos : pos+SidLength]
	pos += SidLength
	e.GNO = int64(binary.LittleEndian.Uint64(data[pos:]))
	pos += 8

	if len(data) >= 42 {
		if data[pos] == LogicalTimestampTypeCode {
			pos++
			e.LastCommitted = int64(binary.LittleEndian.Uint64(data[pos:]))
			pos += PartLogicalTimestampLength
			e.SequenceNumber = int64(binary.LittleEndian.Uint64(data[pos:]))
			pos += 8

			// IMMEDIATE_COMMIT_TIMESTAMP_LENGTH = 7
			if len(data)-pos < 7 {
				return nil
			}
			e.ImmediateCommitTimestamp = mysql.FixedLengthInt(data[pos : pos+7])
			pos += 7
			if (e.ImmediateCommitTimestamp & (uint64(1) << 55)) != 0 {
				// If the most significant bit set, another 7 byte follows representing OriginalCommitTimestamp
				e.ImmediateCommitTimestamp &= ^(uint64(1) << 55)
				e.OriginalCommitTimestamp = mysql.FixedLengthInt(data[pos : pos+7])
				pos += 7
			} else {
				// Otherwise OriginalCommitTimestamp == ImmediateCommitTimestamp
				e.OriginalCommitTimestamp = e.ImmediateCommitTimestamp
			}

			// TRANSACTION_LENGTH_MIN_LENGTH = 1
			if len(data)-pos < 1 {
				return nil
			}
			var n int
			e.TransactionLength, _, n = mysql.LengthEncodedInt(data[pos:])
			pos += n

			// IMMEDIATE_SERVER_VERSION_LENGTH = 4
			e.ImmediateServerVersion = UndefinedServerVer
			e.OriginalServerVersion = UndefinedServerVer
			if len(data)-pos < 4 {
				return nil
			}
			e.ImmediateServerVersion = binary.LittleEndian.Uint32(data[pos:])
			pos += 4
			if (e.ImmediateServerVersion & (uint32(1) << 31)) != 0 {
				// If the most significant bit set, another 4 byte follows representing OriginalServerVersion
				e.ImmediateServerVersion &= ^(uint32(1) << 31)
				e.OriginalServerVersion = binary.LittleEndian.Uint32(data[pos:])
				// pos += 4
			} else {
				// Otherwise OriginalServerVersion == ImmediateServerVersion
				e.OriginalServerVersion = e.ImmediateServerVersion
			}
		}
	}
	return nil
}

func (e *GTIDEvent) Dump(w io.Writer) {
	fmtTime := func(t time.Time) string {
		if t.IsZero() {
			return "<n/a>"
		}
		return t.Format(time.RFC3339Nano)
	}

	fmt.Fprintf(w, "Commit flag: %d\n", e.CommitFlag)
	u, _ := uuid.FromBytes(e.SID)
	if e.Tag != "" {
		fmt.Fprintf(w, "GTID_NEXT: %s:%s:%d\n", u.String(), e.Tag, e.GNO)
	} else {
		fmt.Fprintf(w, "GTID_NEXT: %s:%d\n", u.String(), e.GNO)
	}
	fmt.Fprintf(w, "LAST_COMMITTED: %d\n", e.LastCommitted)
	fmt.Fprintf(w, "SEQUENCE_NUMBER: %d\n", e.SequenceNumber)
	fmt.Fprintf(w, "Immediate commmit timestamp: %d (%s)\n", e.ImmediateCommitTimestamp, fmtTime(e.ImmediateCommitTime()))
	fmt.Fprintf(w, "Orignal commmit timestamp: %d (%s)\n", e.OriginalCommitTimestamp, fmtTime(e.OriginalCommitTime()))
	fmt.Fprintf(w, "Transaction length: %d\n", e.TransactionLength)
	fmt.Fprintf(w, "Immediate server version: %d\n", e.ImmediateServerVersion)
	fmt.Fprintf(w, "Orignal server version: %d\n", e.OriginalServerVersion)
	fmt.Fprintln(w)
}

func (e *GTIDEvent) GTIDNext() (mysql.GTIDSet, error) {
	u, err := uuid.FromBytes(e.SID)
	if err != nil {
		return nil, err
	}
	return mysql.ParseMysqlGTIDSet(strings.Join([]string{u.String(), strconv.FormatInt(e.GNO, 10)}, ":"))
}

// ImmediateCommitTime returns the commit time of this trx on the immediate server
// or zero time if not available.
func (e *GTIDEvent) ImmediateCommitTime() time.Time {
	return microSecTimestampToTime(e.ImmediateCommitTimestamp)
}

// OriginalCommitTime returns the commit time of this trx on the original server
// or zero time if not available.
func (e *GTIDEvent) OriginalCommitTime() time.Time {
	return microSecTimestampToTime(e.OriginalCommitTimestamp)
}

// GtidTaggedLogEvent is for a GTID event with a tag.
// This is similar to GTIDEvent, but it has a tag and uses a different serialization format.
type GtidTaggedLogEvent struct {
	GTIDEvent
}

func (e *GtidTaggedLogEvent) Decode(data []byte) error {
	msg := serialization.Message{
		Format: serialization.Format{
			Fields: []serialization.Field{
				{
					Name: "gtid_flags",
					Type: &serialization.FieldIntFixed{
						Length: 1,
					},
				},
				{
					Name: "uuid",
					Type: &serialization.FieldIntFixed{
						Length: 16,
					},
				},
				{
					Name: "gno",
					Type: &serialization.FieldIntVar{},
				},
				{
					Name: "tag",
					Type: &serialization.FieldString{},
				},
				{
					Name: "last_committed",
					Type: &serialization.FieldIntVar{},
				},
				{
					Name: "sequence_number",
					Type: &serialization.FieldIntVar{},
				},
				{
					Name: "immediate_commit_timestamp",
					Type: &serialization.FieldUintVar{},
				},
				{
					Name:     "original_commit_timestamp",
					Type:     &serialization.FieldUintVar{},
					Optional: true,
				},
				{
					Name: "transaction_length",
					Type: &serialization.FieldUintVar{},
				},
				{
					Name: "immediate_server_version",
					Type: &serialization.FieldUintVar{},
				},
				{
					Name:     "original_server_version",
					Type:     &serialization.FieldUintVar{},
					Optional: true,
				},
				{
					Name:     "commit_group_ticket",
					Optional: true,
				},
			},
		},
	}

	err := serialization.Unmarshal(data, &msg)
	if err != nil {
		return err
	}

	f, err := msg.GetFieldByName("gtid_flags")
	if err != nil {
		return err
	}
	if v, ok := f.Type.(*serialization.FieldIntFixed); ok {
		e.CommitFlag = v.Value[0]
	} else {
		return errors.New("failed to get gtid_flags field")
	}

	f, err = msg.GetFieldByName("uuid")
	if err != nil {
		return err
	}
	if v, ok := f.Type.(*serialization.FieldIntFixed); ok {
		e.SID = v.Value
	} else {
		return errors.New("failed to get uuid field")
	}

	f, err = msg.GetFieldByName("gno")
	if err != nil {
		return err
	}
	if v, ok := f.Type.(*serialization.FieldIntVar); ok {
		e.GNO = v.Value
	} else {
		return errors.New("failed to get gno field")
	}

	f, err = msg.GetFieldByName("tag")
	if err != nil {
		return err
	}
	if v, ok := f.Type.(*serialization.FieldString); ok {
		e.Tag = v.Value
	} else {
		return errors.New("failed to get tag field")
	}

	f, err = msg.GetFieldByName("last_committed")
	if err != nil {
		return err
	}
	if v, ok := f.Type.(*serialization.FieldIntVar); ok {
		e.LastCommitted = v.Value
	} else {
		return errors.New("failed to get last_committed field")
	}

	f, err = msg.GetFieldByName("sequence_number")
	if err != nil {
		return err
	}
	if v, ok := f.Type.(*serialization.FieldIntVar); ok {
		e.SequenceNumber = v.Value
	} else {
		return errors.New("failed to get sequence_number field")
	}

	f, err = msg.GetFieldByName("immediate_commit_timestamp")
	if err != nil {
		return err
	}
	if v, ok := f.Type.(*serialization.FieldUintVar); ok {
		e.ImmediateCommitTimestamp = v.Value
	} else {
		return errors.New("failed to get immediate_commit_timestamp field")
	}

	f, err = msg.GetFieldByName("original_commit_timestamp")
	if err != nil {
		return err
	}
	if v, ok := f.Type.(*serialization.FieldUintVar); ok {
		if f.Skipped {
			e.OriginalCommitTimestamp = e.ImmediateCommitTimestamp
		} else {
			e.OriginalCommitTimestamp = v.Value
		}
	} else {
		return errors.New("failed to get original_commit_timestamp field")
	}

	f, err = msg.GetFieldByName("immediate_server_version")
	if err != nil {
		return err
	}
	if v, ok := f.Type.(*serialization.FieldUintVar); ok {
		e.ImmediateServerVersion = uint32(v.Value)
	} else {
		return errors.New("failed to get immediate_server_version field")
	}

	f, err = msg.GetFieldByName("original_server_version")
	if err != nil {
		return err
	}
	if v, ok := f.Type.(*serialization.FieldUintVar); ok {
		if f.Skipped {
			e.OriginalServerVersion = e.ImmediateServerVersion
		} else {
			e.OriginalServerVersion = uint32(v.Value)
		}
	} else {
		return errors.New("failed to get original_server_version field")
	}

	f, err = msg.GetFieldByName("transaction_length")
	if err != nil {
		return err
	}
	if v, ok := f.Type.(*serialization.FieldUintVar); ok {
		e.TransactionLength = v.Value
	} else {
		return errors.New("failed to get transaction_length field")
	}

	// TODO: add and test commit_group_ticket

	return nil
}

type BeginLoadQueryEvent struct {
	FileID    uint32
	BlockData []byte
}

func (e *BeginLoadQueryEvent) Decode(data []byte) error {
	pos := 0

	e.FileID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	e.BlockData = data[pos:]

	return nil
}

func (e *BeginLoadQueryEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "File ID: %d\n", e.FileID)
	fmt.Fprintf(w, "Block data: %s\n", e.BlockData)
	fmt.Fprintln(w)
}

type ExecuteLoadQueryEvent struct {
	SlaveProxyID     uint32
	ExecutionTime    uint32
	SchemaLength     uint8
	ErrorCode        uint16
	StatusVars       uint16
	FileID           uint32
	StartPos         uint32
	EndPos           uint32
	DupHandlingFlags uint8
}

func (e *ExecuteLoadQueryEvent) Decode(data []byte) error {
	pos := 0

	e.SlaveProxyID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	e.ExecutionTime = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	e.SchemaLength = data[pos]
	pos++

	e.ErrorCode = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	e.StatusVars = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	e.FileID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	e.StartPos = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	e.EndPos = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	e.DupHandlingFlags = data[pos]

	return nil
}

func (e *ExecuteLoadQueryEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Slave proxy ID: %d\n", e.SlaveProxyID)
	fmt.Fprintf(w, "Execution time: %d\n", e.ExecutionTime)
	fmt.Fprintf(w, "Schame length: %d\n", e.SchemaLength)
	fmt.Fprintf(w, "Error code: %d\n", e.ErrorCode)
	fmt.Fprintf(w, "Status vars length: %d\n", e.StatusVars)
	fmt.Fprintf(w, "File ID: %d\n", e.FileID)
	fmt.Fprintf(w, "Start pos: %d\n", e.StartPos)
	fmt.Fprintf(w, "End pos: %d\n", e.EndPos)
	fmt.Fprintf(w, "Dup handling flags: %d\n", e.DupHandlingFlags)
	fmt.Fprintln(w)
}

// case MARIADB_ANNOTATE_ROWS_EVENT:
// 	return "MariadbAnnotateRowsEvent"

type MariadbAnnotateRowsEvent struct {
	Query []byte
}

func (e *MariadbAnnotateRowsEvent) Decode(data []byte) error {
	e.Query = data
	return nil
}

func (e *MariadbAnnotateRowsEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Query: %s\n", e.Query)
	fmt.Fprintln(w)
}

type MariadbBinlogCheckPointEvent struct {
	Info []byte
}

func (e *MariadbBinlogCheckPointEvent) Decode(data []byte) error {
	e.Info = data
	return nil
}

func (e *MariadbBinlogCheckPointEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Info: %s\n", e.Info)
	fmt.Fprintln(w)
}

type MariadbGTIDEvent struct {
	GTID     mysql.MariadbGTID
	Flags    byte
	CommitID uint64
}

func (e *MariadbGTIDEvent) IsDDL() bool {
	return (e.Flags & BINLOG_MARIADB_FL_DDL) != 0
}

func (e *MariadbGTIDEvent) IsStandalone() bool {
	return (e.Flags & BINLOG_MARIADB_FL_STANDALONE) != 0
}

func (e *MariadbGTIDEvent) IsGroupCommit() bool {
	return (e.Flags & BINLOG_MARIADB_FL_GROUP_COMMIT_ID) != 0
}

func (e *MariadbGTIDEvent) Decode(data []byte) error {
	pos := 0
	e.GTID.SequenceNumber = binary.LittleEndian.Uint64(data)
	pos += 8
	e.GTID.DomainID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4
	e.Flags = data[pos]
	pos += 1

	if (e.Flags & BINLOG_MARIADB_FL_GROUP_COMMIT_ID) > 0 {
		e.CommitID = binary.LittleEndian.Uint64(data[pos:])
	}

	return nil
}

func (e *MariadbGTIDEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "GTID: %v\n", e.GTID)
	fmt.Fprintf(w, "Flags: %v\n", e.Flags)
	fmt.Fprintf(w, "CommitID: %v\n", e.CommitID)
	fmt.Fprintln(w)
}

func (e *MariadbGTIDEvent) GTIDNext() (mysql.GTIDSet, error) {
	return mysql.ParseMariadbGTIDSet(e.GTID.String())
}

type MariadbGTIDListEvent struct {
	GTIDs []mysql.MariadbGTID
}

func (e *MariadbGTIDListEvent) Decode(data []byte) error {
	pos := 0
	v := binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	count := v & uint32((1<<28)-1)

	e.GTIDs = make([]mysql.MariadbGTID, count)

	for i := uint32(0); i < count; i++ {
		e.GTIDs[i].DomainID = binary.LittleEndian.Uint32(data[pos:])
		pos += 4
		e.GTIDs[i].ServerID = binary.LittleEndian.Uint32(data[pos:])
		pos += 4
		e.GTIDs[i].SequenceNumber = binary.LittleEndian.Uint64(data[pos:])
		pos += 8
	}

	return nil
}

func (e *MariadbGTIDListEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Lists: %v\n", e.GTIDs)
	fmt.Fprintln(w)
}

type IntVarEvent struct {
	Type  IntVarEventType
	Value uint64
}

func (i *IntVarEvent) Decode(data []byte) error {
	i.Type = IntVarEventType(data[0])
	i.Value = binary.LittleEndian.Uint64(data[1:])
	return nil
}

func (i *IntVarEvent) Dump(w io.Writer) {
	fmt.Fprintf(w, "Type: %d\n", i.Type)
	fmt.Fprintf(w, "Value: %d\n", i.Value)
}

// HeartbeatEvent is a HEARTBEAT_EVENT or HEARTBEAT_LOG_EVENT_V2
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_replication_binlog_event.html#sect_protocol_replication_event_heartbeat
type HeartbeatEvent struct {
	// Event version, either 1 for HEARTBEAT_EVENT or 2 for HEARTBEAT_LOG_EVENT_V2
	Version int

	// Filename of the binary log
	Filename string

	// Offset is the offset in the binlog file
	Offset uint64
}

// Decode is decoding a heartbeat event payload (excluding event header and checksum)
func (h *HeartbeatEvent) Decode(data []byte) error {
	switch h.Version {
	case 1:
		// Also known as HEARTBEAT_EVENT
		h.Filename = string(data)
	case 2:
		// Also known as HEARTBEAT_LOG_EVENT_V2
		//
		// The server sends this in the binlog stream if the following is set:
		// DumpCommandFlag: replication.USE_HEARTBEAT_EVENT_V2
		pos := 0
		for pos < len(data) {
			switch data[pos] {
			case OTW_HB_LOG_FILENAME_FIELD:
				pos++
				nameLength := int(data[pos])
				pos++
				h.Filename = string(data[pos : pos+nameLength])
				pos += nameLength
			case OTW_HB_LOG_POSITION_FIELD:
				pos++
				offsetLength := int(data[pos])
				pos++
				var n int
				h.Offset, _, n = mysql.LengthEncodedInt(data[pos : pos+offsetLength])
				if n != offsetLength {
					return errors.New("failed to read binary log offset")
				}
				pos += offsetLength
			case OTW_HB_HEADER_END_MARK:
				pos++
			default:
				return errors.New("unknown heartbeatv2 field")
			}
		}
	default:
		return errors.New("unknown heartbeat version")
	}

	return nil
}

func (h *HeartbeatEvent) Dump(w io.Writer) {
	fmt.Fprintf(w,
		"Heartbeat Event Version: %d\nBinlog File Name: %s\nBinlog Offset: %d\n",
		h.Version, h.Filename, h.Offset,
	)
	fmt.Fprintln(w)
}
