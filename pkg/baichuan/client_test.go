package baichuan

import (
	"testing"
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
