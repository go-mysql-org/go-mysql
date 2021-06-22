package mysql

type Result struct {
	Status uint16

	InsertId     uint64
	AffectedRows uint64

	*Resultset

	Next *Result
}

type Executer interface {
	Execute(query string, args ...interface{}) (*Result, error)
}

func (r *Result) Close() {
	if r.Resultset != nil {
		r.Resultset.returnToPool()
		r.Resultset = nil
	}
}

func (r *Result) lastChained() (int, *Result) {
	count := 1
	var lastRes *Result
	for lastRes = r; lastRes.Next != nil; lastRes = lastRes.Next {
		count++
	}

	return count, lastRes
}

func (r *Result) ChainResult(cr *Result) {
	_, lastRes := r.lastChained()
	lastRes.Next = cr
}

func (r *Result) Length() int {
	n, _ := r.lastChained()
	return n
}
