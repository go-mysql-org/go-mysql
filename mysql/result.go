package mysql

import "sort"

// Result should be created by NewResultWithoutRows or NewResult. The zero value
// of Result is invalid.
type Result struct {
	Status   uint16
	Warnings uint16

	InsertId     uint64 //nolint:revive // exported field renamed would be a breaking API change
	AffectedRows uint64

	StatusMessage   string
	SessionTracking *SessionTrackingInfo

	*Resultset

	StreamResult *StreamResult
}

type SessionTrackingInfo struct {
	GTID             string
	TransactionState string
	Variables        map[string]string
	Schema           string
	State            string
	Characteristics  string
}

// AppendOKSessionTrackSuffix appends the OK-packet session tracking suffix:
// [statusMessageLen][statusMessage][sessionTrackBlockLen][sessionTrackBlock].
func AppendOKSessionTrackSuffix(data []byte, r *Result) []byte {
	if r == nil {
		return data
	}

	statusMessage := r.StatusMessage
	data = append(data, byte(len(statusMessage)))
	if len(statusMessage) > 0 {
		data = append(data, statusMessage...)
	}

	block := encodeSessionTracking(r.SessionTracking)
	data = append(data, byte(len(block)))
	if len(block) > 0 {
		data = append(data, block...)
	}

	return data
}

func encodeSessionTracking(s *SessionTrackingInfo) []byte {
	if s == nil {
		return nil
	}

	var data []byte

	if len(s.Variables) > 0 {
		names := make([]string, 0, len(s.Variables))
		for name := range s.Variables {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			value := s.Variables[name]
			payload := make([]byte, 0, 2+len(name)+len(value))
			payload = append(payload, byte(len(name)))
			payload = append(payload, name...)
			payload = append(payload, byte(len(value)))
			payload = append(payload, value...)
			data = appendSessionTrackEntry(data, SESSION_TRACK_SYSTEM_VARIABLES, payload)
		}
	}

	if s.Schema != "" {
		payload := make([]byte, 0, 1+len(s.Schema))
		payload = append(payload, byte(len(s.Schema)))
		payload = append(payload, s.Schema...)
		data = appendSessionTrackEntry(data, SESSION_TRACK_SCHEMA, payload)
	}

	if s.State != "" {
		data = appendSessionTrackEntry(data, SESSION_TRACK_STATE_CHANGE, []byte(s.State[:1]))
	}

	if s.GTID != "" {
		payload := make([]byte, 0, 2+len(s.GTID))
		payload = append(payload, 0x00)
		payload = append(payload, byte(len(s.GTID)))
		payload = append(payload, s.GTID...)
		data = appendSessionTrackEntry(data, SESSION_TRACK_GTIDS, payload)
	}

	if s.Characteristics != "" {
		payload := make([]byte, 0, 1+len(s.Characteristics))
		payload = append(payload, byte(len(s.Characteristics)))
		payload = append(payload, s.Characteristics...)
		data = appendSessionTrackEntry(data, SESSION_TRACK_TRANSACTION_CHARACTERISTICS, payload)
	}

	if s.TransactionState != "" {
		payload := make([]byte, 0, 1+len(s.TransactionState))
		payload = append(payload, byte(len(s.TransactionState)))
		payload = append(payload, s.TransactionState...)
		data = appendSessionTrackEntry(data, SESSION_TRACK_TRANSACTION_STATE, payload)
	}

	return data
}

func appendSessionTrackEntry(data []byte, trackType byte, payload []byte) []byte {
	data = append(data, trackType)
	data = append(data, byte(len(payload)))
	data = append(data, payload...)
	return data
}

func NewResult(resultset *Resultset) *Result {
	return &Result{
		Resultset: resultset,
	}
}

func NewResultReserveResultset(fieldCount int) *Result {
	return &Result{
		Resultset: NewResultset(fieldCount),
	}
}

type Executer interface {
	Execute(query string, args ...any) (*Result, error)
}

func (r *Result) Close() {
	if r.Resultset != nil {
		r.returnToPool()
		r.Resultset = nil
	}
	if r.StreamResult != nil {
		r.StreamResult.Close()
		r.StreamResult = nil
	}
}

func (r *Result) HasResultset() bool {
	if r == nil {
		return false
	}
	if r.Resultset != nil && len(r.Fields) > 0 {
		return true
	}
	return false
}

func (r *Result) IsStreaming() bool {
	return r != nil && r.StreamResult != nil
}
