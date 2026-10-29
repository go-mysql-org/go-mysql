package replication

import (
	"context"
	"fmt"
	"io"
	"runtime/debug"
	"sync"

	"github.com/go-mysql-org/go-mysql/client"
)

// InterruptRead must only close the underlying transport, not mutate packet
// sequence state concurrently with the producer. Full connection cleanup takes
// place after the producer has been joined.
type replicationPacketSource struct{ *client.Conn }

func (s replicationPacketSource) InterruptRead() error { return s.Conn.Conn.Conn.Close() }

type packetSource interface {
	ReadPacket() ([]byte, error)
	InterruptRead() error
}

type packetBatch struct {
	packets [][]byte
	err     error
	charge  int
}

type packetReadPanic struct{ detail string }

func (e *packetReadPanic) Error() string { return e.detail }

// packetReadAhead belongs to one connection generation. Only the producer uses
// the packet reader; only the consumer uses parser, GTID and delivery state.
// Packets own their storage, so handing them off does not alias bufio memory.
// The producer may hold one pending batch outside the charged budget. A packet
// larger than the budget is admitted alone to guarantee forward progress.
// The budget does not cover events retained by downstream consumers.
type packetReadAhead struct {
	source      packetSource
	ctx         context.Context
	cancel      context.CancelFunc
	packets     chan packetBatch
	done        chan struct{}
	mu          sync.Mutex
	released    *sync.Cond
	used, limit int
	previous    int // consumer-owned charge, released after parsing/delivery
	current     [][]byte
	index       int
	batchErr    error
}

func newPacketReadAhead(ctx context.Context, source packetSource, limit int) *packetReadAhead {
	ctx, cancel := context.WithCancel(ctx)

	r := &packetReadAhead{
		source: source, ctx: ctx, cancel: cancel,
		packets: make(chan packetBatch, 128), done: make(chan struct{}), limit: limit,
	}
	r.released = sync.NewCond(&r.mu)
	go r.run()
	return r
}

func (r *packetReadAhead) run() {
	defer close(r.done)
	defer close(r.packets)
	defer func() {
		if v := recover(); v != nil {
			select {
			case r.packets <- packetBatch{err: &packetReadPanic{detail: fmt.Sprintf("packet reader panic: %v\n%s", v, debug.Stack())}}:
			case <-r.ctx.Done():
			}
		}
	}()
	buffered, canBatch := r.source.(interface{ BufferedPacketSize() int })
	for r.ctx.Err() == nil {
		data, err := r.source.ReadPacket()
		batch := packetBatch{err: err}
		if err == nil {
			batch.packets = append(batch.packets, data)
		}
		size := cap(data)
		// Drain only complete packets already in bufio. Never wait for another
		// packet to fill a batch, including on an otherwise idle connection.
		for err == nil && canBatch && len(batch.packets) < 64 && size < min(r.limit, 64<<10) {
			next := buffered.BufferedPacketSize()
			if next < 0 || next+size > min(r.limit, 64<<10) {
				break
			}
			data, err = r.source.ReadPacket()
			if err == nil {
				batch.packets = append(batch.packets, data)
				size += cap(data)
			}
			batch.err = err
		}
		// Capacity, rather than length, is the memory actually retained by a packet.
		// An oversized packet takes the entire budget, allowing forward progress.
		charge := min(size, r.limit)
		batch.charge = charge
		r.mu.Lock()
		for r.used+charge > r.limit && r.ctx.Err() == nil {
			r.released.Wait()
		}
		if r.ctx.Err() != nil {
			r.mu.Unlock()
			return
		}
		r.used += charge
		r.mu.Unlock()
		select {
		case r.packets <- batch:
		case <-r.ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

func (r *packetReadAhead) readPacket() ([]byte, error) {
	for {
		if err := r.ctx.Err(); err != nil {
			return nil, err
		}
		if r.index < len(r.current) {
			data := r.current[r.index]
			r.current[r.index] = nil
			r.index++
			return data, nil
		}
		if r.previous != 0 {
			r.mu.Lock()
			r.used -= r.previous
			r.released.Signal()
			r.mu.Unlock()
			r.previous = 0
		}
		r.current = nil
		if r.batchErr != nil {
			err := r.batchErr
			r.batchErr = nil
			return nil, err
		}
		select {
		case <-r.ctx.Done():
			return nil, r.ctx.Err()
		case p, ok := <-r.packets:
			if !ok {
				return nil, io.EOF
			}
			r.previous, r.current, r.index, r.batchErr = p.charge, p.packets, 0, p.err
		}
	}
}

// close interrupts both a blocked socket read and a producer waiting for space.
// It joins the producer before the caller can reset protocol/parser state.
func (r *packetReadAhead) close() {
	r.cancel()
	r.mu.Lock()
	r.released.Broadcast()
	r.mu.Unlock()
	_ = r.source.InterruptRead()
	<-r.done
}
