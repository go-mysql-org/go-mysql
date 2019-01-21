package schema

import (
	"github.com/siddontang/go-mysql/mysql"
)

type SchemaStorage interface {
	// SaveSnapshot will be called when schema tracker decides to save a snapshot
	SaveSnapshot(data []byte, pos mysql.Position) error

	// SaveSnapshot will be called when schema tracker decides to save a ddl statement
	SaveStatement(db string, statement string, pos mysql.Position) error

	// LoadLastSnapshot will be called when schema tracker need to restore snapshot, as base data
	LoadLastSnapshot() ([]byte, mysql.Position, error)

	// Reset will be called to get a empty storage
	Reset() error

	// StatementIterator return an iterator which can iterate all ddl statements after last snapshot
	StatementIterator() Iterator
}

type Iterator interface {
	First() (string, string, mysql.Position, error)
	Next() (string, string, mysql.Position, error)
	End() bool
}
