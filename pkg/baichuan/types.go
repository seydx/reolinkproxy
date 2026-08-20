package baichuan

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Default constants and internal message IDs for the Baichuan protocol.
const (
	DefaultPort    = 9000
	DefaultTimeout = 10 * time.Second

	magicHeader = 0x0ABCDEF0

	classLegacy           = 0x6514
	classModern           = 0x6614
	classModernWithOffset = 0x6414
	classModernAlt        = 0x0000

	msgIDPTZControl               = 18
	msgIDPTZControlPreset         = 19
	msgIDPTZPresetGet             = 190
	msgIDLogin                    = 1
	msgIDLogout                   = 2
	msgIDVideo                    = 3
	msgIDVideoStop                = 4
	msgIDTalkAbility              = 10
	msgIDTalkReset                = 11
	msgIDMotionRequest            = 31
	msgIDMotion                   = 33
	msgIDGetPorts                 = 37
	msgIDPing                     = 93
	msgIDAbilityInfo              = 151
	msgIDTalkConfig               = 201
	msgIDTalk                     = 202
	msgIDUDPKeepAlive             = 234
	msgIDBatteryInfoList          = 252
	msgIDBatteryInfo              = 253
	msgIDReboot                   = 23
	msgIDIspSet                   = 25
	msgIDIspGet                   = 26
	msgIDSnap                     = 109
	msgIDGetDevInfo               = 80
	msgIDStreamInfoList           = 146
	msgIDGetSupport               = 199
	msgIDWebhookGet               = 806
	msgIDWebhookSet               = 807
	msgIDWifiSignal               = 115
	msgIDChannelInfoList          = 145
	msgIDWifiSSID                 = 116
	msgIDAutoFocusGet             = 224
	msgIDAutoFocusSet             = 225
	msgIDPlayAudio                = 263
	msgIDSirenGet                 = 264
	msgIDSirenSet                 = 265
	msgIDFloodlightManual         = 288
	msgIDWhiteLedGet              = 289
	msgIDFloodlightStatus         = 291
	msgIDAiAlarmGet               = 342
	msgIDAiAlarmSet               = 343
	msgIDQuickReplyPlay           = 349
	msgIDDingDongOpt1             = 484
	msgIDDingDongOpt2             = 485
	msgIDDingDongGet              = 486
	msgIDDingDongSet              = 487
	msgIDPrivacyModeGet           = 574
	msgIDPrivacyModeSet           = 575
	msgIDPreRecordGet             = 594
	msgIDPreRecordSet             = 595
	msgIDSceneGet                 = 601
	msgIDSceneSet                 = 602
	msgIDSceneInfo                = 604
	msgIDDingDongSilentGet        = 609
	msgIDDingDongSilentSet        = 610
	msgIDPtzGuardGet              = 332
	msgIDPtzGuardSet              = 331
	msgIDPtz3DLocation            = 445
	defaultUIDMTU          uint32 = 1350
)

// Header channel-ID semantics (reolink_aio): 250 = host, 1-100 = channel+1,
// 0/251 = push. Standalone cameras tolerate host-level commands on channel 0,
// but NVRs/HomeHubs reject them — the login then fails as "bad credentials".
const (
	channelIDHost uint8 = 250
	channelIDPush uint8 = 251
)

func headerChannelID(channel uint8) uint8 {
	return channel + 1
}

// EncryptionMode is the negotiated XML encryption mode used by Baichuan.
type EncryptionMode uint8

// Available encryption modes.
const (
	EncryptionNone EncryptionMode = iota
	EncryptionBC
	EncryptionAES
)

// Stream selects the requested camera stream.
type Stream string

// Supported stream types.
const (
	StreamMain   Stream = "mainStream"
	StreamSub    Stream = "subStream"
	StreamExtern Stream = "externStream"
)

// ErrIdle is the terminal error of a connection that CloseWhenIdle dropped
// because nothing used it, not because anything went wrong.
var ErrIdle = errors.New("connection closed while idle")

// Config contains connection settings for a Baichuan client.
type Config struct {
	Host     string
	Port     int
	UID      string
	Username string
	Password string
	Timeout  time.Duration
	// Debugf, when set, receives protocol-level diagnostics. Callers route it
	// to the camera's logger so the output stays attributable.
	Debugf func(format string, args ...any)
	// Warnf, when set, receives the few protocol findings a user has to see
	// even without debug logging, e.g. events arriving for a channel nobody
	// listens on. Callers route it to their camera logger.
	Warnf func(format string, args ...any)
	// LowPower marks a peer that must not be kept awake: a battery camera
	// only sleeps once nothing holds its radio, so Login starts no keepalive.
	// The caller decides what happens instead, see StartKeepAlive and
	// CloseWhenIdle.
	LowPower bool
}

func (c Config) normalized() Config {
	if c.Port == 0 {
		c.Port = DefaultPort
	}
	if c.Timeout == 0 {
		c.Timeout = DefaultTimeout
	}
	return c
}

// Header is the decoded Baichuan header.
type Header struct {
	MsgID         uint32
	BodyLen       uint32
	ChannelID     uint8
	StreamType    uint8
	MsgNum        uint16
	ResponseCode  uint16
	Class         uint16
	PayloadOffset uint32
}

// HasPayloadOffset reports whether the header carries the 24-byte modern layout.
func (h Header) HasPayloadOffset() bool {
	return hasPayloadOffset(h.Class)
}

// IsModern reports whether the message is modern, not legacy.
func (h Header) IsModern() bool {
	return h.Class != classLegacy
}

func hasPayloadOffset(class uint16) bool {
	return class == classModernWithOffset || class == classModernAlt
}

// Extension is the small XML block that may precede the primary payload.
type Extension struct {
	BinaryData *int `xml:"binaryData"`
	ChannelID  *int `xml:"channelId"`
	EncryptLen *int `xml:"encryptLen"`
}

// Message is a decoded Baichuan message.
type Message struct {
	Header        Header
	Extension     []byte
	Payload       []byte
	XML           string
	Binary        bool
	ExtensionMeta *Extension
}

func (m *Message) success() error {
	if !m.Header.HasPayloadOffset() {
		return nil
	}

	switch m.Header.ResponseCode {
	case 200, 201, 300:
		return nil
	default:
		return &StatusError{
			MsgID: m.Header.MsgID,
			Code:  m.Header.ResponseCode,
		}
	}
}

// StatusError is returned when the camera replies with a non-success status.
type StatusError struct {
	MsgID uint32
	Code  uint16
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("baichuan cmd %d failed with status %d", e.MsgID, e.Code)
}

// MediaPacketKind identifies the parsed bcmedia payload type.
type MediaPacketKind int

// Available media packet kinds.
const (
	MediaPacketInfoV1 MediaPacketKind = iota + 1
	MediaPacketInfoV2
	MediaPacketIFrame
	MediaPacketPFrame
	MediaPacketAAC
	MediaPacketADPCM
)

func (k MediaPacketKind) String() string {
	switch k {
	case MediaPacketInfoV1:
		return "info-v1"
	case MediaPacketInfoV2:
		return "info-v2"
	case MediaPacketIFrame:
		return "iframe"
	case MediaPacketPFrame:
		return "pframe"
	case MediaPacketAAC:
		return "aac"
	case MediaPacketADPCM:
		return "adpcm"
	default:
		return "unknown"
	}
}

// MediaPacket is one parsed bcmedia unit.
type MediaPacket struct {
	Kind               MediaPacketKind
	Codec              string
	Data               []byte
	TimestampMicrosecs uint32
	HasTimestamp       bool
	UnixTime           *time.Time
	Width              uint32
	Height             uint32
	FPS                uint8
}

// MediaReader exposes the parsed bcmedia stream coming from a preview session.
type MediaReader struct {
	Packets  <-chan MediaPacket
	client   *Client
	channel  uint8
	stream   Stream
	stop     chan struct{}
	stopOnce func()

	errMu sync.Mutex
	err   error
}

// Err returns why the reader ended early (e.g. a media parse desync), or nil
// for a clean stop.
func (r *MediaReader) Err() error {
	r.errMu.Lock()
	defer r.errMu.Unlock()
	return r.err
}

func (r *MediaReader) setErr(err error) {
	r.errMu.Lock()
	defer r.errMu.Unlock()
	if r.err == nil {
		r.err = err
	}
}

// Close stops the media reader.
func (r *MediaReader) Close() {
	if r.stopOnce != nil {
		r.stopOnce()
	}
	if r.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = r.client.StopPreview(ctx, r.channel, r.stream)
	}
}
