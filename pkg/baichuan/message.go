package baichuan

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"
)

type request struct {
	MsgID      uint32
	ChannelID  uint8
	StreamType uint8
	Class      uint16
	MsgNum     uint16
	Extension  []byte
	Body       []byte
	Binary     bool
	ForceBC    bool
}

type pendingKey struct {
	msgID  uint32
	msgNum uint16
}

type closeState struct {
	err error
	mu  sync.Mutex
}

func (s *closeState) set(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
	}
}

func (s *closeState) get() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (c *Client) readMessage() (*Message, error) {
	headerBuf := make([]byte, 20)
	if _, err := io.ReadFull(c.transport, headerBuf); err != nil {
		return nil, err
	}

	if binary.LittleEndian.Uint32(headerBuf[0:4]) != magicHeader {
		return nil, fmt.Errorf("unexpected baichuan magic %#x", binary.LittleEndian.Uint32(headerBuf[0:4]))
	}

	header := Header{
		MsgID:        binary.LittleEndian.Uint32(headerBuf[4:8]),
		BodyLen:      binary.LittleEndian.Uint32(headerBuf[8:12]),
		ChannelID:    headerBuf[12],
		StreamType:   headerBuf[13],
		MsgNum:       binary.LittleEndian.Uint16(headerBuf[14:16]),
		ResponseCode: binary.LittleEndian.Uint16(headerBuf[16:18]),
		Class:        binary.LittleEndian.Uint16(headerBuf[18:20]),
	}

	if header.HasPayloadOffset() {
		offsetBuf := make([]byte, 4)
		if _, err := io.ReadFull(c.transport, offsetBuf); err != nil {
			return nil, err
		}
		header.PayloadOffset = binary.LittleEndian.Uint32(offsetBuf)
	}

	if header.BodyLen > 50*1024*1024 {
		return nil, fmt.Errorf("implausibly large baichuan body length: %d", header.BodyLen)
	}

	body := make([]byte, header.BodyLen)
	if _, err := io.ReadFull(c.transport, body); err != nil {
		return nil, err
	}

	if header.MsgID == msgIDLogin && header.IsModern() && (header.ResponseCode>>8) == 0xDD {
		c.setNegotiatedEncryption(header.ResponseCode)
	}

	mode, aesKey, hasAESKey := c.snapshotCipher()

	extLen := uint32(0)
	if header.HasPayloadOffset() && header.PayloadOffset > 0 {
		extLen = header.PayloadOffset
	}
	if extLen > header.BodyLen {
		return nil, fmt.Errorf("invalid payload offset %d for body size %d", extLen, header.BodyLen)
	}

	extEncrypted := body[:extLen]
	payloadEncrypted := body[extLen:]

	var extension []byte
	prePayloadXML := ""
	if len(extEncrypted) > 0 {
		extension = decryptXMLAuto(header.ChannelID, extEncrypted, mode, aesKey, hasAESKey)
		prePayloadXML = trimXML(extension)
	}

	extensionMeta, _ := parseExtension(extension)

	binaryPayload := false
	if extensionMeta != nil && extensionMeta.BinaryData != nil && *extensionMeta.BinaryData == 1 {
		c.markBinaryMsgNum(header.MsgNum)
		binaryPayload = true
	} else {
		binaryPayload = c.isBinaryMsgNum(header.MsgNum)
	}

	var payload []byte
	var xmlText string
	if len(payloadEncrypted) > 0 {
		if binaryPayload {
			payload = append([]byte(nil), payloadEncrypted...)
			encryptLen := 0
			if extensionMeta != nil && extensionMeta.EncryptLen != nil {
				encryptLen = *extensionMeta.EncryptLen
			} else if v, ok := parseEncryptLen(prePayloadXML); ok {
				encryptLen = v
			}
			if encryptLen > 0 && hasAESKey && encryptLen <= len(payloadEncrypted) {
				decryptedPrefix := aesCFB(payloadEncrypted[:encryptLen], aesKey, false)
				payload = append(append([]byte(nil), decryptedPrefix...), payloadEncrypted[encryptLen:]...)
			}
			xmlText = prePayloadXML
		} else {
			payload = decryptXMLAuto(header.ChannelID, payloadEncrypted, mode, aesKey, hasAESKey)
			xmlText = trimXML(payload)
		}
	}

	return &Message{
		Header:        header,
		Extension:     extension,
		Payload:       payload,
		XML:           xmlText,
		Binary:        binaryPayload,
		ExtensionMeta: extensionMeta,
	}, nil
}

// binaryMsgNumTTL bounds how long a message number stays flagged as binary
// after its last use. Message numbers are uint16 and wrap on long-running
// connections (snapshot chunks burn several per call), and a stale flag would
// route an unrelated reply into the media path.
const binaryMsgNumTTL = 5 * time.Minute

func (c *Client) markBinaryMsgNum(msgNum uint16) {
	c.binaryMu.Lock()
	defer c.binaryMu.Unlock()
	c.binaryMsgNums[msgNum] = time.Now()

	for num, lastSeen := range c.binaryMsgNums {
		if time.Since(lastSeen) > binaryMsgNumTTL {
			delete(c.binaryMsgNums, num)
		}
	}
}

func (c *Client) isBinaryMsgNum(msgNum uint16) bool {
	c.binaryMu.Lock()
	defer c.binaryMu.Unlock()

	lastSeen, ok := c.binaryMsgNums[msgNum]
	if !ok {
		return false
	}
	if time.Since(lastSeen) > binaryMsgNumTTL {
		delete(c.binaryMsgNums, msgNum)
		return false
	}
	// Refresh: an active media stream keeps its message number alive.
	c.binaryMsgNums[msgNum] = time.Now()
	return true
}

func (c *Client) encodeRequest(req request) []byte {
	mode, aesKey, hasAESKey := c.snapshotCipher()
	if req.ForceBC {
		mode = EncryptionBC
	}

	extension := append([]byte(nil), req.Extension...)
	body := append([]byte(nil), req.Body...)

	if req.Class != classLegacy {
		if len(extension) > 0 {
			extension = encryptXML(req.ChannelID, extension, mode, aesKey, hasAESKey)
		}
		if !req.Binary && len(body) > 0 {
			body = encryptXML(req.ChannelID, body, mode, aesKey, hasAESKey)
		}
	}

	headerLen := 20
	if hasPayloadOffset(req.Class) {
		headerLen = 24
	}

	packet := make([]byte, headerLen+len(extension)+len(body))
	binary.LittleEndian.PutUint32(packet[0:4], magicHeader)
	binary.LittleEndian.PutUint32(packet[4:8], req.MsgID)
	binary.LittleEndian.PutUint32(packet[8:12], uint32(len(extension)+len(body))) //#nosec G115
	packet[12] = req.ChannelID
	packet[13] = req.StreamType
	binary.LittleEndian.PutUint16(packet[14:16], req.MsgNum)

	responseCode := uint16(0)
	if req.Class == classLegacy && req.MsgID == msgIDLogin && len(body) == 0 {
		// Header-only legacy login is the nonce request and uses the special marker
		// seen in the official client / reolink_aio implementation.
		responseCode = 0xDC12
	}
	binary.LittleEndian.PutUint16(packet[16:18], responseCode)
	binary.LittleEndian.PutUint16(packet[18:20], req.Class)
	if hasPayloadOffset(req.Class) {
		offset := uint32(0)
		if len(extension) > 0 {
			offset = uint32(len(extension)) //#nosec G115
		}
		binary.LittleEndian.PutUint32(packet[20:24], offset)
	}

	copy(packet[headerLen:], extension)
	copy(packet[headerLen+len(extension):], body)
	return packet
}
