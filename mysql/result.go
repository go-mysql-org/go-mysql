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
	data = append(data, PutLengthEncodedInt(uint64(len(statusMessage)))...)
	if len(statusMessage) > 0 {
		data = append(data, statusMessage...)
	}

	block := encodeSessionTracking(r.SessionTracking)
	data = append(data, PutLengthEncodedInt(uint64(len(block)))...)
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
			var payload []byte
			payload = appendLenEncString(payload, name)
			payload = appendLenEncString(payload, value)
			data = appendSessionTrackEntry(data, SESSION_TRACK_SYSTEM_VARIABLES, payload)
		}
	}

	if s.Schema != "" {
		var payload []byte
		payload = appendLenEncString(payload, s.Schema)
		data = appendSessionTrackEntry(data, SESSION_TRACK_SCHEMA, payload)
	}

	if s.State != "" {
		var payload []byte
		payload = appendLenEncString(payload, s.State[:1])
		data = appendSessionTrackEntry(data, SESSION_TRACK_STATE_CHANGE, payload)
	}

	if s.GTID != "" {
		var payload []byte
		payload = append(payload, 0x00)
		payload = appendLenEncString(payload, s.GTID)
		data = appendSessionTrackEntry(data, SESSION_TRACK_GTIDS, payload)
	}

	if s.Characteristics != "" {
		var payload []byte
		payload = appendLenEncString(payload, s.Characteristics)
		data = appendSessionTrackEntry(data, SESSION_TRACK_TRANSACTION_CHARACTERISTICS, payload)
	}

	if s.TransactionState != "" {
		var payload []byte
		payload = appendLenEncString(payload, s.TransactionState)
		data = appendSessionTrackEntry(data, SESSION_TRACK_TRANSACTION_STATE, payload)
	}

	return data
}

func appendLenEncString(data []byte, s string) []byte {
	data = append(data, PutLengthEncodedInt(uint64(len(s)))...)
	data = append(data, s...)
	return data
}

func appendSessionTrackEntry(data []byte, trackType byte, payload []byte) []byte {
	data = append(data, trackType)
	data = append(data, PutLengthEncodedInt(uint64(len(payload)))...)
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
