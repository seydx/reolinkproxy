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
	subs  map[uint32]map[chan *Message]struct{}

	closed    chan struct{}
	closeOnce sync.Once
	closeErr  closeState
	wg        sync.WaitGroup

	keepAliveOnce sync.Once

	// alarmPushed latches once the camera has pushed an alarm event on this
	// connection. Firmwares that accept the subscription without ever pushing
	// need it re-sent, so the state has to be per connection.
	alarmPushed atomic.Bool
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

	switch {
	case cfg.Host != "":
		address := cfg.Host
		if _, _, splitErr := net.SplitHostPort(address); splitErr != nil {
			address = net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
		}

		var conn net.Conn
		dialer := net.Dialer{Timeout: cfg.Timeout}
		conn, err = dialer.DialContext(ctx, "tcp", address)
		if err != nil {
			return nil, err
		}
		transport = conn

	case cfg.UID != "":
		transport, err = dialUIDLocal(ctx, cfg.UID, cfg.Timeout)
		if err != nil {
			return nil, err
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
		subs:          make(map[uint32]map[chan *Message]struct{}),
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
		var subs []chan *Message
		for ch := range c.subs[msg.Header.MsgID] {
			subs = append(subs, ch)
		}
		c.subMu.RUnlock()
		for _, ch := range subs {
			select {
			case ch <- msg:
			default:
			}
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

	c.keepAliveOnce.Do(func() {
		c.wg.Add(1)
		go c.keepAliveLoop()
	})

	return nil
}

func (c *Client) keepAliveLoop() {
	defer c.wg.Done()

	const pingTimeout = 4 * time.Second

	interval := 5 * time.Second
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

// Subscribe attaches a best-effort fanout listener for a given msg_id.
func (c *Client) Subscribe(msgID uint32) (<-chan *Message, func()) {
	// Generous buffer: the preview subscription carries a full 4K media
	// stream, and a dropped message desyncs the bcmedia parser.
	ch := make(chan *Message, 256)

	c.subMu.Lock()
	if c.subs[msgID] == nil {
		c.subs[msgID] = make(map[chan *Message]struct{})
	}
	c.subs[msgID][ch] = struct{}{}
	c.subMu.Unlock()

	var once sync.Once
	return ch, func() {
		once.Do(func() {
			c.subMu.Lock()
			if subs := c.subs[msgID]; subs != nil {
				delete(subs, ch)
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

	sub, unsubscribe := c.Subscribe(msgIDVideo)
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
			case msg := <-sub:
				if msg == nil || !msg.Binary || len(msg.Payload) == 0 {
					continue
				}

				if msg.Header.StreamType != streamType {
					continue
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
