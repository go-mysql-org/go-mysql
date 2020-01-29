package mysql

import "sync"

type Result struct {
	Status uint16

	InsertId     uint64
	AffectedRows uint64

	*Resultset
}

type Executer interface {
	Execute(query string, args ...interface{}) (*Result, error)
}

var (
	resultPool = sync.Pool{
		New: func() interface{} {
			return &Result{
				Resultset: &Resultset{},
			}
		},
	}
)

func NewResult(resultsetCount int) *Result {
	r := resultPool.Get().(*Result)
	r.reset(resultsetCount)
	return r
}

func (r *Result) reset(resultsetCount int) {
	r.Status = 0
	r.InsertId = 0
	r.AffectedRows = 0
	r.Resultset.reset(resultsetCount)
}

func (r *Result) Close() {
	resultPool.Put(r)
}
