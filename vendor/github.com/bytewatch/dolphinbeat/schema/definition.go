package schema

import (
	"github.com/bytewatch/ddl-executor"
)

// MySQL type information.
const (
	TypeDecimal   byte = 0
	TypeTiny      byte = 1
	TypeShort     byte = 2
	TypeLong      byte = 3
	TypeFloat     byte = 4
	TypeDouble    byte = 5
	TypeNull      byte = 6
	TypeTimestamp byte = 7
	TypeLonglong  byte = 8
	TypeInt24     byte = 9
	TypeDate      byte = 10
	/* Original name was TypeTime, renamed to Duration to resolve the conflict with Go type Time.*/
	TypeDuration byte = 11
	TypeDatetime byte = 12
	TypeYear     byte = 13
	TypeNewDate  byte = 14
	TypeVarchar  byte = 15
	TypeBit      byte = 16

	TypeJSON       byte = 0xf5
	TypeNewDecimal byte = 0xf6
	TypeEnum       byte = 0xf7
	TypeSet        byte = 0xf8
	TypeTinyBlob   byte = 0xf9
	TypeMediumBlob byte = 0xfa
	TypeLongBlob   byte = 0xfb
	TypeBlob       byte = 0xfc
	TypeVarString  byte = 0xfd
	TypeString     byte = 0xfe
	TypeGeometry   byte = 0xff
)

type IndexType string

const (
	IndexType_NONE IndexType = ""
	IndexType_PRI            = "PRI"
	IndexType_UNI            = "UNI"
	IndexType_MUL            = "MUL"
)

type TableDef struct {
	Database string       `json:"database"`
	Name     string       `json:"name"`
	Columns  []*ColumnDef `json:"columns"`
	Charset  string       `json:"charset"`
}

type ColumnDef struct {
	Name      string    `json:"name"`
	Type      string    `json:"type"`
	InnerType byte      `json:"inner_type"`
	Key       IndexType `json:"key"`
	Charset   string    `json:"charset"`
	Unsigned  bool      `json:"unsigned"`
	Nullable  bool      `json:"nullable"`
}

func makeColumnDef(c *executor.ColumnDef) *ColumnDef {
	return &ColumnDef{
		Name:      c.Name,
		Type:      c.Type,
		InnerType: c.InnerType,
		Key:       IndexType(c.Key),
		Charset:   c.Charset,
		Unsigned:  c.Unsigned,
		Nullable:  c.Nullable,
	}
}

func makeTableDef(t *executor.TableDef) *TableDef {
	tableDef := &TableDef{
		Database: t.Database,
		Name:     t.Name,
		Charset:  t.Charset,
		Columns:  make([]*ColumnDef, 0, len(t.Columns)),
	}
	for _, c := range t.Columns {
		tableDef.Columns = append(tableDef.Columns, makeColumnDef(c))
	}
	return tableDef
}
