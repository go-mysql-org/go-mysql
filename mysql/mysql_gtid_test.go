package mysql

import (
	"reflect"
	"testing"

	"github.com/google/uuid"
)

func TestParseUUIDSet(t *testing.T) {
	tests := []struct {
		input    string
		expected map[string]*UUIDSet
		wantErr  bool
	}{
		{
			input: "0b8beec9-911e-11e9-9f7b-8a057645f3f6:1-1175877800",
			expected: map[string]*UUIDSet{
				"0b8beec9-911e-11e9-9f7b-8a057645f3f6": {
					SID:       uuid.Must(uuid.Parse("0b8beec9-911e-11e9-9f7b-8a057645f3f6")),
					Intervals: []Interval{{Start: 1, Stop: 1175877801}}, // Stop is Start+1 for single intervals
				},
			},
			wantErr: false,
		},
		{
			input: "0b8beec9-911e-11e9-9f7b-8a057645f3f6:1-1175877800,246e88bd-0288-11e8-9cee-230cd2fc765b:1-592884032",
			expected: map[string]*UUIDSet{
				"0b8beec9-911e-11e9-9f7b-8a057645f3f6": {
					SID:       uuid.Must(uuid.Parse("0b8beec9-911e-11e9-9f7b-8a057645f3f6")),
					Intervals: []Interval{{Start: 1, Stop: 1175877801}},
				},
				"246e88bd-0288-11e8-9cee-230cd2fc765b": {
					SID:       uuid.Must(uuid.Parse("246e88bd-0288-11e8-9cee-230cd2fc765b")),
					Intervals: []Interval{{Start: 1, Stop: 592884033}},
				},
			},
			wantErr: false,
		},
		{
			input:   "invalid",
			wantErr: true,
		},
		{
			input:    "",
			expected: nil,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseUUIDSet(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseUUIDSet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("ParseUUIDSet() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestParseMysqlGTIDSet(t *testing.T) {
	input := "0b8beec9-911e-11e9-9f7b-8a057645f3f6:1-1175877800,246e88bd-0288-11e8-9cee-230cd2fc765b:1-592884032"
	expected := &MysqlGTIDSet{
		Sets: map[string]*UUIDSet{
			"0b8beec9-911e-11e9-9f7b-8a057645f3f6": {
				SID:       uuid.Must(uuid.Parse("0b8beec9-911e-11e9-9f7b-8a057645f3f6")),
				Intervals: []Interval{{Start: 1, Stop: 1175877801}},
			},
			"246e88bd-0288-11e8-9cee-230cd2fc765b": {
				SID:       uuid.Must(uuid.Parse("246e88bd-0288-11e8-9cee-230cd2fc765b")),
				Intervals: []Interval{{Start: 1, Stop: 592884033}},
			},
		},
	}

	got, err := ParseMysqlGTIDSet(input)
	if err != nil {
		t.Fatalf("ParseMysqlGTIDSet() error = %v", err)
	}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("ParseMysqlGTIDSet() = %v, want %v", got, expected)
	}

	if got.String() != input {
		t.Errorf("String() = %v, want %v", got.String(), input)
	}
}
