package canal

import (
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
)

// TestHeartbeatIntervalConversion tests interval conversion from seconds to duration
func TestHeartbeatIntervalConversion(t *testing.T) {
	tests := []struct {
		name             string
		intervalSeconds  int
		expectedDuration time.Duration
	}{
		{
			name:             "Zero interval (disabled)",
			intervalSeconds:  0,
			expectedDuration: 0,
		},
		{
			name:             "60 seconds",
			intervalSeconds:  60,
			expectedDuration: 60 * time.Second,
		},
		{
			name:             "300 seconds (5 minutes)",
			intervalSeconds:  300,
			expectedDuration: 5 * time.Minute,
		},
		{
			name:             "3600 seconds (1 hour)",
			intervalSeconds:  3600,
			expectedDuration: 1 * time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			duration := time.Duration(tt.intervalSeconds) * time.Second
			assert.Equal(t, tt.expectedDuration, duration)
		})
	}
}

// TestShouldSendHeartbeat tests the heartbeat timing logic
func TestShouldSendHeartbeat(t *testing.T) {
	tests := []struct {
		name              string
		heartbeatInterval time.Duration
		lastEventTime     time.Time
		currentTime       time.Time
		expected          bool
	}{
		{
			name:              "Disabled (zero interval)",
			heartbeatInterval: 0,
			lastEventTime:     time.Now().Add(-100 * time.Second),
			currentTime:       time.Now(),
			expected:          false,
		},
		{
			name:              "Not enough time passed",
			heartbeatInterval: 60 * time.Second,
			lastEventTime:     time.Now().Add(-30 * time.Second),
			currentTime:       time.Now(),
			expected:          false,
		},
		{
			name:              "Exactly at interval",
			heartbeatInterval: 60 * time.Second,
			lastEventTime:     time.Now().Add(-60 * time.Second),
			currentTime:       time.Now(),
			expected:          true,
		},
		{
			name:              "Past interval",
			heartbeatInterval: 60 * time.Second,
			lastEventTime:     time.Now().Add(-120 * time.Second),
			currentTime:       time.Now(),
			expected:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Canal{
				heartbeatInterval: tt.heartbeatInterval,
				lastEventSentTime: tt.lastEventTime,
			}

			result := c.shouldSendHeartbeat()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestSendAsHeartbeat tests heartbeat event creation
func TestSendAsHeartbeat(t *testing.T) {
	tests := []struct {
		name          string
		eventType     interface{}
		expectWarning bool
	}{
		{
			name:          "Valid RowsEvent",
			eventType:     &replication.RowsEvent{},
			expectWarning: false,
		},
		{
			name:          "Invalid event type (QueryEvent)",
			eventType:     &replication.QueryEvent{},
			expectWarning: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock event handler
			mockHandler := &mockHeartbeatEventHandler{}

			// Create canal with mock handler
			c := &Canal{
				eventHandler: mockHandler,
				master: &masterInfo{
					gset: nil,
				},
			}

			// Create binlog event with proper RowsEvent
			var event replication.Event
			if rowsEvent, ok := tt.eventType.(*replication.RowsEvent); ok {
				event = rowsEvent
			} else {
				event = tt.eventType.(*replication.QueryEvent)
			}

			ev := &replication.BinlogEvent{
				Header: &replication.EventHeader{
					Timestamp: uint32(time.Now().Unix()),
					LogPos:    1234,
				},
				Event: event,
			}

			// Call sendAsHeartbeat
			c.sendAsHeartbeat(ev)

			if !tt.expectWarning {
				// Verify heartbeat was sent
				assert.Equal(t, 1, mockHandler.callCount, "OnRow should be called once")
				assert.NotNil(t, mockHandler.lastEvent, "Event should be captured")
				assert.Equal(t, "heartbeat", mockHandler.lastEvent.Action, "Action should be 'heartbeat'")
				assert.Nil(t, mockHandler.lastEvent.Table, "Table should be nil")
				assert.Nil(t, mockHandler.lastEvent.Rows, "Rows should be nil")
				assert.Equal(t, ev.Header, mockHandler.lastEvent.Header, "Header should match")
			} else {
				// Verify heartbeat was NOT sent (type assertion failed)
				assert.Equal(t, 0, mockHandler.callCount, "OnRow should not be called for invalid event type")
			}
		})
	}
}

// TestHeartbeatEventGTIDSet tests GTID is properly set on heartbeat events
func TestHeartbeatEventGTIDSet(t *testing.T) {
	mockHandler := &mockHeartbeatEventHandler{}

	// Create a mock GTID set
	mockGTIDSet, _ := mysql.ParseGTIDSet("mysql", "6d8aec2b-6ad2-11f0-8f30-e6b37ac480a5:1-10")

	c := &Canal{
		eventHandler: mockHandler,
		master: &masterInfo{
			gset: mockGTIDSet,
		},
	}

	ev := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			LogPos:    1234,
		},
		Event: &replication.RowsEvent{},
	}

	c.sendAsHeartbeat(ev)

	assert.Equal(t, 1, mockHandler.callCount)
	assert.NotNil(t, mockHandler.lastEvent)
	assert.Equal(t, mockGTIDSet, mockHandler.lastEvent.Header.Gtid, "GTID should be set from SyncedGTIDSet")
}

// TestHeartbeatTimerReset tests that timer is properly reset
func TestHeartbeatTimerReset(t *testing.T) {
	c := &Canal{
		heartbeatInterval: 60 * time.Second,
		lastEventSentTime: time.Now().Add(-100 * time.Second),
	}

	// Should send heartbeat (100s > 60s)
	assert.True(t, c.shouldSendHeartbeat())

	// Simulate timer reset
	c.lastEventSentTime = time.Now()

	// Should NOT send heartbeat now (0s < 60s)
	assert.False(t, c.shouldSendHeartbeat())
}

// TestFormatPositionInfo tests the formatPositionInfo helper function
func TestFormatPositionInfo(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		logPos   uint32
		gtid     mysql.GTIDSet
		expected string
	}{
		{
			name:     "With GTID",
			fileName: "mysql-bin.000123",
			logPos:   4567,
			gtid:     mustParseGTID("6d8aec2b-6ad2-11f0-8f30-e6b37ac480a5:1-10"),
			expected: "position (mysql-bin.000123, 4567), GTID: 6d8aec2b-6ad2-11f0-8f30-e6b37ac480a5:1-10",
		},
		{
			name:     "Without GTID (nil)",
			fileName: "mysql-bin.000456",
			logPos:   8910,
			gtid:     nil,
			expected: "position (mysql-bin.000456, 8910)",
		},
		{
			name:     "With empty GTID",
			fileName: "mysql-bin.000789",
			logPos:   1234,
			gtid:     &mysql.MysqlGTIDSet{},
			expected: "position (mysql-bin.000789, 1234)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatPositionInfo(tt.fileName, tt.logPos, tt.gtid)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func mustParseGTID(gtidStr string) mysql.GTIDSet {
	if gtidStr == "" {
		return nil
	}
	gtid, _ := mysql.ParseGTIDSet("mysql", gtidStr)
	return gtid
}

// Mock event handler for testing
type mockHeartbeatEventHandler struct {
	DummyEventHandler
	callCount int
	lastEvent *RowsEvent
}

func (h *mockHeartbeatEventHandler) OnRow(e *RowsEvent) error {
	h.callCount++
	h.lastEvent = e
	return nil
}

func (h *mockHeartbeatEventHandler) String() string {
	return "mockHeartbeatEventHandler"
}
