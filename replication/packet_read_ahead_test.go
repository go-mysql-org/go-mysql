package replication

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type queuedPacket struct {
	data []byte
	err  error
}

type testPacketSource struct {
	packets   chan queuedPacket
	closed    chan struct{}
	once      sync.Once
	reads     atomic.Int32
	panicRead bool
}

func (s *testPacketSource) ReadPacket() ([]byte, error) {
	if s.panicRead {
		panic("test packet panic")
	}
	select {
	case p := <-s.packets:
		s.reads.Add(1)
		return p.data, p.err
	case <-s.closed:
		return nil, io.EOF
	}
}

func (s *testPacketSource) InterruptRead() error { s.once.Do(func() { close(s.closed) }); return nil }

func testSource() *testPacketSource {
	return &testPacketSource{packets: make(chan queuedPacket, 20), closed: make(chan struct{})}
}

func TestPacketReadAheadOrderAndError(t *testing.T) {
	s := testSource()
	wantErr := errors.New("disconnected")
	for i := range 10 {
		s.packets <- queuedPacket{data: []byte{byte(i)}}
	}
	s.packets <- queuedPacket{err: wantErr}
	r := newPacketReadAhead(context.Background(), s, 4)
	defer r.close()
	for i := range 10 {
		b, err := r.readPacket()
		require.NoError(t, err)
		require.Equal(t, []byte{byte(i)}, b)
	}
	_, err := r.readPacket()
	require.ErrorIs(t, err, wantErr)
}

func TestPacketReadAheadBoundAndOversizedPacket(t *testing.T) {
	for _, size := range []int{4, 16} {
		t.Run(string(rune('a'+size)), func(t *testing.T) {
			s := testSource()
			for range 6 {
				s.packets <- queuedPacket{data: make([]byte, size)}
			}
			r := newPacketReadAhead(context.Background(), s, 8)
			defer r.close()
			want := int32(8/size + 1)
			if size > 8 {
				want = 2
			}
			require.Eventually(t, func() bool { return s.reads.Load() == want }, time.Second, time.Millisecond)
			time.Sleep(10 * time.Millisecond)
			require.Equal(t, want, s.reads.Load()) // budget plus one pending packet
			for range 6 {
				p, err := r.readPacket()
				require.NoError(t, err)
				require.Len(t, p, size)
			}
		})
	}
}

func TestPacketReadAheadClose(t *testing.T) {
	for _, blockedBudget := range []bool{false, true} {
		t.Run(map[bool]string{false: "socket", true: "budget"}[blockedBudget], func(t *testing.T) {
			s := testSource()
			if blockedBudget {
				for range 5 {
					s.packets <- queuedPacket{data: make([]byte, 8)}
				}
			}
			r := newPacketReadAhead(context.Background(), s, 8)
			if blockedBudget {
				require.Eventually(t, func() bool { return s.reads.Load() == 2 }, time.Second, time.Millisecond)
			}
			done := make(chan struct{})
			go func() { r.close(); close(done) }()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("close blocked")
			}
			r.close() // repeated close is safe
		})
	}
}

func TestPacketReadAheadConnectionGeneration(t *testing.T) {
	old := testSource()
	old.packets <- queuedPacket{data: []byte("stale")}
	first := newPacketReadAhead(context.Background(), old, 1024)
	require.Eventually(t, func() bool { return old.reads.Load() == 1 }, time.Second, time.Millisecond)
	first.close()
	next := testSource()
	next.packets <- queuedPacket{data: []byte("new")}
	second := newPacketReadAhead(context.Background(), next, 1024)
	defer second.close()
	p, err := second.readPacket()
	require.NoError(t, err)
	require.Equal(t, "new", string(p))
}

func TestPacketReadAheadPanic(t *testing.T) {
	s := testSource()
	s.panicRead = true
	r := newPacketReadAhead(context.Background(), s, 1024)
	defer r.close()
	_, err := r.readPacket()
	require.ErrorContains(t, err, "test packet panic")
}

type bufferedTestPacketSource struct{ *testPacketSource }

func (s *bufferedTestPacketSource) BufferedPacketSize() int {
	if len(s.packets) > 0 {
		return 1
	}
	return -1
}

func TestPacketReadAheadBufferedBatchAndIdle(t *testing.T) {
	s := &bufferedTestPacketSource{testSource()}
	for i := range 10 {
		s.packets <- queuedPacket{data: []byte{byte(i)}}
	}
	r := newPacketReadAhead(context.Background(), s, 1024)
	defer r.close()
	// An incomplete batch is delivered without waiting for more socket data.
	require.Eventually(t, func() bool { return len(r.packets) == 1 }, time.Second, time.Millisecond)
	for i := range 10 {
		p, err := r.readPacket()
		require.NoError(t, err)
		require.Equal(t, []byte{byte(i)}, p)
	}
}

func TestPacketReadAheadBufferedBatchErrorOrder(t *testing.T) {
	s := &bufferedTestPacketSource{testSource()}
	want := errors.New("batch disconnected")
	for i := range 10 {
		s.packets <- queuedPacket{data: []byte{byte(i)}}
	}
	s.packets <- queuedPacket{err: want}
	r := newPacketReadAhead(context.Background(), s, 1024)
	defer r.close()
	for i := range 10 {
		p, err := r.readPacket()
		require.NoError(t, err)
		require.Equal(t, []byte{byte(i)}, p)
	}
	_, err := r.readPacket()
	require.ErrorIs(t, err, want)
}
