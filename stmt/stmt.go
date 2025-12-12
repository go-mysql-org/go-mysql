package stmt

import "github.com/go-mysql-org/go-mysql/mysql"

type PreparedStmt struct {
	ID      uint32
	Params  int
	Columns int

	RawParamFields  [][]byte
	RawColumnFields [][]byte

	paramFields  []*mysql.Field
	columnFields []*mysql.Field
}

func (s *PreparedStmt) GetParamFields() []*mysql.Field {
	if s.RawParamFields == nil {
		return nil
	}
	if s.paramFields == nil {
		s.paramFields = make([]*mysql.Field, len(s.RawParamFields))
		for i, raw := range s.RawParamFields {
			s.paramFields[i] = &mysql.Field{}
			_ = s.paramFields[i].Parse(raw)
		}
	}
	return s.paramFields
}

func (s *PreparedStmt) GetColumnFields() []*mysql.Field {
	if s.RawColumnFields == nil {
		return nil
	}
	if s.columnFields == nil {
		s.columnFields = make([]*mysql.Field, len(s.RawColumnFields))
		for i, raw := range s.RawColumnFields {
			s.columnFields[i] = &mysql.Field{}
			_ = s.columnFields[i].Parse(raw)
		}
	}
	return s.columnFields
}
