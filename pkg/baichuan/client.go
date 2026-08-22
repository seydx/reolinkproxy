// Package baichuan provides the protocol implementation for communicating with Baichuan cameras.
package baichuan

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Client is a Baichuan connection that supports TCP and local UID/UDP transport.
type Client struct {
	cfg       Config
	transport ioReadWriteCloser
	isUDP     bool

	sendMu sync.Mutex
	seqMu  sync.Mutex
	msgNum uint16

	stateMu       sync.RWMutex
	mode          EncryptionMode
	negotiated    bool
	loginInfo     LoginDeviceInfo
	aesKey        [16]byte
	hasAESKey     bool
	binaryMu      sync.Mutex
	binaryMsgNums map[uint16]time.Time

	loginMu  sync.Mutex
	loggedIn bool

	// snapMu serializes Snap calls: the JPEG chunks arrive on fresh message
	// numbers, so concurrent snaps on one client would interleave.
	snapMu sync.Mutex

	pendingMu sync.Mutex
	pending   map[pendingKey]chan *Message

	subMu sync.RWMutex
	subs  map[uint32]map[*subscription]struct{}

	closed    chan struct{}
	closeOnce sync.Once
	closeErr  closeState
	wg        sync.WaitGroup

	// lastRead / lastSend are when a message last came off / went onto the
	// wire, in unix nanos
	lastRead atomic.Int64
	lastSend atomic.Int64

	keepAliveOnce sync.Once
	idleCloseOnce sync.Once

	// foreignAlarmOnce keeps the "events are for another channel" warning to
	// one line per connection
	foreignAlarmOnce sync.Once

	// alarmPushed latches once the camera has pushed an alarm event on this
	// connection. Firmwares that accept the subscription without ever pushing
	// need it re-sent, so the state has to be per connection.
	alarmPushed atomic.Bool
}

func (c *Client) warnf(format string, args ...any) {
	if c.cfg.Warnf != nil {
		c.cfg.Warnf(format, args...)
		return
	}
	c.debugf(format, args...)
}

func (c *Client) debugf(format string, args ...any) {
	if c.cfg.Debugf != nil {
		c.cfg.Debugf(format, args...)
	}
}

type ioReadWriteCloser interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
	Close() error
}

// Dial opens a Baichuan connection over TCP or local UID/UDP.
func Dial(ctx context.Context, cfg Config) (*Client, error) {
	cfg = cfg.normalized()

	var (
		transport ioReadWriteCloser
		isUDP     bool
		err       error
	)

	preferUID := cfg.PreferUID && cfg.UID != ""

	switch {
	case cfg.Host != "" && !preferUID:
		transport, err = dialTCP(ctx, cfg)
		if err != nil {
			if cfg.UID == "" {
				return nil, err
			}
			// A sleeping battery camera refuses the connection outright: it
			// keeps no TCP port open, only the P2P transport answers.
			var uidErr error
			transport, uidErr = dialUIDLocal(ctx, cfg.UID, cfg.Host, cfg.Timeout)
			if uidErr != nil {
				return nil, fmt.Errorf("%w (uid transport: %v)", err, uidErr)
			}
			isUDP = true
		}

	case cfg.UID != "":
		transport, err = dialUIDLocal(ctx, cfg.UID, cfg.Host, cfg.Timeout)
		if err != nil {
			if cfg.Host == "" {
				return nil, err
			}
			var tcpErr error
			transport, tcpErr = dialTCP(ctx, cfg)
			if tcpErr != nil {
				return nil, fmt.Errorf("%w (tcp transport: %v)", err, tcpErr)
			}
			break
		}
		isUDP = true

	default:
		return nil, fmt.Errorf("either host or uid must be set")
	}

	client := &Client{
		cfg:           cfg,
		transport:     transport,
		isUDP:         isUDP,
		mode:          EncryptionNone,
		binaryMsgNums: make(map[uint16]time.Time),
		pending:       make(map[pendingKey]chan *Message),
		subs:          make(map[uint32]map[*subscription]struct{}),
		closed:        make(chan struct{}),
	}

	client.wg.Add(1)
	go client.readLoop()
	return client, nil
}

func (c *Client) readLoop() {
	defer c.wg.Done()

	for {
		msg, err := c.readMessage()
		if err != nil {
			c.shutdown(err)
			return
		}
		c.lastRead.Store(time.Now().UnixNano())
		c.dispatch(msg)
	}
}

func (c *Client) dispatch(msg *Message) {
	c.pendingMu.Lock()
	respCh := c.pending[pendingKey{msgID: msg.Header.MsgID, msgNum: msg.Header.MsgNum}]
	c.pendingMu.Unlock()
	if respCh != nil {
		select {
		case respCh <- msg:
		default:
		}
	}

	c.subMu.RLock()
	subs := make([]*subscription, 0, len(c.subs[msg.Header.MsgID]))
	for sub := range c.subs[msg.Header.MsgID] {
		subs = append(subs, sub)
	}
	c.subMu.RUnlock()
	for _, sub := range subs {
		select {
		case sub.ch <- msg:
		default:
			// the counter lets stateful consumers (bcmedia) notice the gap
			// instead of stitching unrelated bytes together
			sub.dropped.Add(1)
		}
	}
}

func (c *Client) shutdown(err error) {
	c.closeOnce.Do(func() {
		c.closeErr.set(err)
		close(c.closed)
		_ = c.transport.Close()
	})
}

// Close terminates the Baichuan connection.
func (c *Client) Close() error {
	c.shutdown(context.Canceled)
	c.wg.Wait()
	return nil
}

// Err returns the terminal client error, if any.
func (c *Client) Err() error {
	return c.closeErr.get()
}

// Done reports when the underlying connection has terminated.
func (c *Client) Done() <-chan struct{} {
	return c.closed
}

// Login negotiates the nonce, derives AES if needed, and authenticates.
func (c *Client) Login(ctx context.Context) error {
	c.loginMu.Lock()
	defer c.loginMu.Unlock()

	if c.loggedIn {
		return nil
	}

	nonceResp, err := c.sendRequest(ctx, request{
		MsgID:     msgIDLogin,
		ChannelID: channelIDHost,
		Class:     classLegacy,
		ForceBC:   true,
	})
	if err != nil {
		return fmt.Errorf("request nonce: %w", err)
	}

	nonce, err := parseNonce(nonceResp.XML)
	if err != nil {
		snippet := nonceResp.XML
		if len(snippet) > 160 {
			snippet = snippet[:160]
		}
		return fmt.Errorf(
			"parse nonce: %w (response_code=%#x class=%#x xml_prefix=%q)",
			err,
			nonceResp.Header.ResponseCode,
			nonceResp.Header.Class,
			snippet,
		)
	}

	c.stateMu.Lock()
	c.aesKey = DeriveAESKey(nonce, c.cfg.Password)
	c.hasAESKey = true
	// The login body itself is BC-encrypted even when AES was negotiated —
	// except when the firmware negotiated "none" (0xDD00), which rejects a
	// BC-scrambled login.
	loginPlain := c.negotiated && c.mode == EncryptionNone
	c.stateMu.Unlock()

	loginXML, err := buildLoginXML(MD5Modern(c.cfg.Username+nonce), MD5Modern(c.cfg.Password+nonce))
	if err != nil {
		return err
	}

	loginResp, err := c.sendRequest(ctx, request{
		MsgID:     msgIDLogin,
		ChannelID: channelIDHost,
		Class:     classModernWithOffset,
		Body:      loginXML,
		ForceBC:   !loginPlain,
	})
	if err != nil {
		return fmt.Errorf("login failed: %w", err)
	}

	c.stateMu.Lock()
	c.loginInfo = parseLoginDeviceInfo(loginResp.XML)
	c.stateMu.Unlock()

	c.stateMu.Lock()
	// Firmwares without the 0xDD negotiation reply get the official client's
	// assumption (AES); negotiated modes are authoritative.
	if !c.negotiated && c.hasAESKey {
		c.mode = EncryptionAES
	}
	c.stateMu.Unlock()

	c.loggedIn = true

	if !c.cfg.LowPower {
		c.StartKeepAlive()
	}

	return nil
}

// StartKeepAlive begins pinging the camera so a dead link is noticed. Login
// does this on its own; a LowPower peer starts silent and only gets the pings
// when the caller has no other way to keep the connection honest.
func (c *Client) StartKeepAlive() {
	c.keepAliveOnce.Do(func() {
		c.wg.Add(1)
		go c.keepAliveLoop()
	})
}

// CloseWhenIdle closes the connection once nothing has been sent or received
// for the given span, so a battery camera can go back to sleep. Traffic in
// either direction counts, which keeps a running preview alive.
func (c *Client) CloseWhenIdle(after time.Duration) {
	if after <= 0 {
		return
	}
	c.idleCloseOnce.Do(func() {
		c.wg.Add(1)
		go c.idleCloseLoop(after)
	})
}

func (c *Client) idleCloseLoop(after time.Duration) {
	defer c.wg.Done()

	ticker := time.NewTicker(after / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			last := max(c.lastRead.Load(), c.lastSend.Load())
			if time.Since(time.Unix(0, last)) < after {
				continue
			}
			c.debugf("closing the connection to let the camera sleep")
			c.shutdown(ErrIdle)
			return
		case <-c.closed:
			return
		}
	}
}

func dialTCP(ctx context.Context, cfg Config) (net.Conn, error) {
	address := cfg.Host
	if _, _, splitErr := net.SplitHostPort(address); splitErr != nil {
		address = net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	}

	dialer := net.Dialer{Timeout: cfg.Timeout}
	return dialer.DialContext(ctx, "tcp", address)
}

// UsedUID reports whether the connection runs over the P2P transport instead
// of TCP.
func (c *Client) UsedUID() bool {
	return c.isUDP
}

func (c *Client) keepAliveLoop() {
	defer c.wg.Done()

	const pingTimeout = 4 * time.Second

	interval := 30 * time.Second
	if c.isUDP {
		interval = 500 * time.Millisecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// A half-open TCP connection (camera lost power, NAT dropped the mapping)
	// stays readable forever, so the ping is answered or the link is dead.
	// One missed reply can be a busy camera, three in a row is not.
	const maxPingFailures = 3
	pingFailures := 0

	for {
		select {
		case <-ticker.C:
			if c.isUDP {
				_ = c.sendNoReply(request{
					MsgID:     msgIDUDPKeepAlive,
					ChannelID: channelIDHost,
					Class:     classModernWithOffset,
				})
				continue
			}

			sentAt := time.Now()
			pingCtx, cancel := context.WithTimeout(context.Background(), pingTimeout)
			_, err := c.sendRequest(pingCtx, request{
				MsgID:     msgIDPing,
				ChannelID: channelIDHost,
				Class:     classModernWithOffset,
			})
			cancel()

			if err == nil {
				pingFailures = 0
				continue
			}

			// A camera on a congested link answers late because the reply is
			// queued behind its video backlog, not because it is gone. Bytes
			// arriving after the ping went out prove the link is alive.
			if last := c.lastRead.Load(); last > sentAt.UnixNano() {
				pingFailures = 0
				continue
			}

			pingFailures++
			if pingFailures >= maxPingFailures {
				c.shutdown(fmt.Errorf("ping failed %d consecutive times: %w", maxPingFailures, err))
				return
			}
		case <-c.closed:
			return
		}
	}
}

// subscription is one fanout listener. dropped counts messages the readLoop
// could not deliver because ch was full.
type subscription struct {
	ch      chan *Message
	dropped atomic.Uint64
}

// Subscribe attaches a best-effort fanout listener for a given msg_id.
func (c *Client) Subscribe(msgID uint32) (<-chan *Message, func()) {
	sub, unsubscribe := c.subscribe(msgID)
	return sub.ch, unsubscribe
}

func (c *Client) subscribe(msgID uint32) (*subscription, func()) {
	// Generous buffer: the preview subscription carries a full 4K media
	// stream, and a dropped message desyncs the bcmedia parser.
	sub := &subscription{ch: make(chan *Message, 256)}

	c.subMu.Lock()
	if c.subs[msgID] == nil {
		c.subs[msgID] = make(map[*subscription]struct{})
	}
	c.subs[msgID][sub] = struct{}{}
	c.subMu.Unlock()

	var once sync.Once
	return sub, func() {
		once.Do(func() {
			c.subMu.Lock()
			if subs := c.subs[msgID]; subs != nil {
				delete(subs, sub)
				if len(subs) == 0 {
					delete(c.subs, msgID)
				}
			}
			c.subMu.Unlock()
		})
	}
}

// StartPreview starts live media streaming and returns a parsed bcmedia reader.
func (c *Client) StartPreview(ctx context.Context, channel uint8, stream Stream) (*MediaReader, error) {
	if err := c.Login(ctx); err != nil {
		return nil, err
	}

	streamType, handle := streamParams(stream)
	body, err := buildPreviewXML(channel, handle, stream)
	if err != nil {
		return nil, err
	}

	sub, unsubscribe := c.subscribe(msgIDVideo)
	if _, err := c.sendRequest(ctx, request{
		MsgID:      msgIDVideo,
		ChannelID:  headerChannelID(channel),
		StreamType: streamType,
		Class:      classModernWithOffset,
		Body:       body,
	}); err != nil {
		unsubscribe()
		return nil, err
	}

	packets := make(chan MediaPacket, 128)
	stop := make(chan struct{})

	reader := &MediaReader{
		Packets: packets,
		client:  c,
		channel: channel,
		stream:  stream,
		stop:    stop,
	}
	var stopOnce sync.Once
	reader.stopOnce = func() {
		stopOnce.Do(func() {
			close(stop)
		})
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer unsubscribe()
		defer close(packets)

		var parser MediaParser
		for {
			select {
			case <-c.closed:
				return
			case <-stop:
				return
			case msg := <-sub.ch:
				if msg == nil || !msg.Binary || len(msg.Payload) == 0 {
					continue
				}

				if msg.Header.StreamType != streamType {
					continue
				}

				// a dropped fanout message leaves a hole in the byte stream; the
				// parser would complete the current frame with unrelated bytes
				// and hand corrupt video downstream, so give up before feeding it
				if dropped := sub.dropped.Load(); dropped > 0 {
					reader.setErr(fmt.Errorf("bcmedia stream lost %d message(s), consumer too slow", dropped))
					return
				}

				parsed, err := parser.Append(msg.Payload)
				if err != nil {
					// A lost or corrupted media message desyncs the bcmedia
					// byte stream. The connection itself is fine (and shared
					// with the event listener and other streams), so end just
					// this reader — the consumer restarts the preview with a
					// fresh parser.
					prefixLen := min(len(msg.Payload), 32)
					reader.setErr(fmt.Errorf("bcmedia parse: %w (payload_prefix=%x)", err, msg.Payload[:prefixLen]))
					return
				}

				for _, packet := range parsed {
					select {
					case packets <- packet:
					case <-c.closed:
						return
					case <-stop:
						return
					}
				}
			}
		}
	}()

	return reader, nil
}

// StopPreview tells the camera to stop sending preview packets for a stream.
func (c *Client) StopPreview(ctx context.Context, channel uint8, stream Stream) error {
	if err := c.Login(ctx); err != nil {
		return err
	}

	streamType, handle := streamParams(stream)
	body, err := buildStopPreviewXML(channel, handle)
	if err != nil {
		return err
	}

	resp, err := c.sendRequest(ctx, request{
		MsgID:      msgIDVideoStop,
		ChannelID:  headerChannelID(channel),
		StreamType: streamType,
		Class:      classModernWithOffset,
		Body:       body,
	})
	if err != nil {
		if _, ok := err.(*StatusError); ok {
			return err
		}
		return nil
	}

	return resp.success()
}

func (c *Client) sendRequest(ctx context.Context, req request) (*Message, error) {
	msg, err := c.roundTripRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := msg.success(); err != nil {
		return nil, err
	}
	return msg, nil
}

func (c *Client) roundTripRequest(ctx context.Context, req request) (*Message, error) {
	req.MsgNum = c.reserveMessageNumber()
	return c.roundTripRequestWithReservedMsgNum(ctx, req)
}

func (c *Client) roundTripRequestWithReservedMsgNum(ctx context.Context, req request) (*Message, error) {
	key := pendingKey{msgID: req.MsgID, msgNum: req.MsgNum}
	responseCh := make(chan *Message, 1)

	c.pendingMu.Lock()
	c.pending[key] = responseCh
	c.pendingMu.Unlock()
	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, key)
		c.pendingMu.Unlock()
	}()

	if err := c.writeRequest(req); err != nil {
		return nil, err
	}

	select {
	case msg := <-responseCh:
		return msg, nil
	case <-c.closed:
		if err := c.closeErr.get(); err != nil {
			return nil, err
		}
		return nil, context.Canceled
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Client) sendNoReply(req request) error {
	req.MsgNum = c.reserveMessageNumber()
	return c.writeRequest(req)
}

func (c *Client) writeRequest(req request) error {
	payload := c.encodeRequest(req)

	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	_, err := c.transport.Write(payload)
	c.lastSend.Store(time.Now().UnixNano())
	return err
}

func (c *Client) reserveMessageNumber() uint16 {
	c.seqMu.Lock()
	defer c.seqMu.Unlock()
	msgNum := c.msgNum
	c.msgNum++
	return msgNum
}

func (c *Client) snapshotCipher() (EncryptionMode, [16]byte, bool) {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.mode, c.aesKey, c.hasAESKey
}

// LoginDeviceInfo returns the DeviceInfo block from the login reply. Zero
// value before the first successful Login.
func (c *Client) LoginDeviceInfo() LoginDeviceInfo {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.loginInfo
}

func (c *Client) setNegotiatedEncryption(code uint16) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()

	switch byte(code) { //#nosec G115
	case 0x00:
		c.mode = EncryptionNone
	case 0x01:
		c.mode = EncryptionBC
	case 0x02, 0x03, 0x12:
		// 0x12 is "full AES" — XML payloads are AES-encrypted like 0x02.
		c.mode = EncryptionAES
	default:
		return
	}
	c.negotiated = true
}

func streamParams(stream Stream) (uint8, uint32) {
	switch stream {
	case StreamSub:
		return 1, 256
	case StreamExtern:
		return 2, 1024
	default:
		return 0, 0
	}
}
