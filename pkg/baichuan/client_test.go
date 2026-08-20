package baichuan

import (
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

func testDispatchClient() *Client {
	return &Client{
		pending: make(map[pendingKey]chan *Message),
		subs:    make(map[uint32]map[*subscription]struct{}),
		closed:  make(chan struct{}),
	}
}

func TestDispatchCountsDropsPerSubscription(t *testing.T) {
	t.Parallel()

	c := testDispatchClient()
	sub, unsubscribe := c.subscribe(msgIDVideo)
	defer unsubscribe()

	msg := &Message{Header: Header{MsgID: msgIDVideo}}
	for range cap(sub.ch) {
		c.dispatch(msg)
	}
	if got := sub.dropped.Load(); got != 0 {
		t.Fatalf("dropped = %d while the channel still had room", got)
	}

	c.dispatch(msg)
	c.dispatch(msg)
	if got := sub.dropped.Load(); got != 2 {
		t.Fatalf("dropped = %d, want 2", got)
	}

	// a second subscriber with room keeps receiving, drops are per subscription
	other, unsubscribeOther := c.subscribe(msgIDVideo)
	defer unsubscribeOther()
	c.dispatch(msg)
	if got := other.dropped.Load(); got != 0 {
		t.Fatalf("fresh subscription dropped = %d, want 0", got)
	}
	if len(other.ch) != 1 {
		t.Fatalf("fresh subscription holds %d messages, want 1", len(other.ch))
	}
}

type nopTransport struct {
	closed chan struct{}
	once   sync.Once
}

func (t *nopTransport) Read(_ []byte) (int, error) {
	<-t.closed
	return 0, io.EOF
}

func (t *nopTransport) Write(p []byte) (int, error) { return len(p), nil }

func (t *nopTransport) Close() error {
	t.once.Do(func() { close(t.closed) })
	return nil
}

func testIdleClient() *Client {
	c := testDispatchClient()
	c.transport = &nopTransport{closed: make(chan struct{})}
	c.lastRead.Store(time.Now().UnixNano())
	c.lastSend.Store(time.Now().UnixNano())
	return c
}

func TestCloseWhenIdleDropsAQuietConnection(t *testing.T) {
	t.Parallel()

	c := testIdleClient()
	c.CloseWhenIdle(60 * time.Millisecond)

	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("an unused connection was not closed")
	}
	if !errors.Is(c.Err(), ErrIdle) {
		t.Fatalf("Err() = %v, want ErrIdle", c.Err())
	}
}

func TestCloseWhenIdleKeepsAConnectionCarryingTraffic(t *testing.T) {
	t.Parallel()

	c := testIdleClient()
	c.CloseWhenIdle(60 * time.Millisecond)

	// a running preview only reads, so incoming media alone has to count
	for range 8 {
		time.Sleep(20 * time.Millisecond)
		c.lastRead.Store(time.Now().UnixNano())
	}

	select {
	case <-c.Done():
		t.Fatal("a connection carrying traffic was closed")
	default:
	}
}
