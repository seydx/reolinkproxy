package baichuan

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
)

type xmlSnapBody struct {
	XMLName xml.Name `xml:"body"`
	Snap    xmlSnap  `xml:"Snap"`
}

type xmlSnap struct {
	Version      string `xml:"version,attr,omitempty"`
	ChannelID    uint8  `xml:"channelId"`
	LogicChannel uint8  `xml:"logicChannel"`
	Time         int    `xml:"time"`
	FullFrame    int    `xml:"fullFrame"`
	StreamType   string `xml:"streamType"`
}

type xmlSnapReply struct {
	Snap *struct {
		ChannelID   int    `xml:"channelId"`
		FileName    string `xml:"fileName"`
		PictureSize int    `xml:"pictureSize"`
	} `xml:"Snap"`
}

// Snap requests an on-demand JPEG snapshot (msg 109). The camera first
// replies with the picture size, then streams the JPEG as binary messages —
// on fresh message numbers, not the request's, so snaps are serialized per
// client and chunks are collected by message ID: response code 200 means more
// data follows, 201 marks the final chunk.
func (c *Client) Snap(ctx context.Context, channel uint8) ([]byte, error) {
	if err := c.Login(ctx); err != nil {
		return nil, err
	}

	c.snapMu.Lock()
	defer c.snapMu.Unlock()

	body, err := marshalXMLDocument(xmlSnapBody{
		Snap: xmlSnap{
			Version:      "1.1",
			ChannelID:    channel,
			LogicChannel: channel,
			StreamType:   "main",
		},
	})
	if err != nil {
		return nil, err
	}

	// Subscribe before sending so no chunk is missed.
	sub, unsubscribe := c.subscribe(msgIDSnap)
	defer unsubscribe()

	resp, err := c.sendRequest(ctx, request{
		MsgID:     msgIDSnap,
		ChannelID: headerChannelID(channel),
		Class:     classModernWithOffset,
		Extension: channelExtension(channel),
		Body:      body,
	})
	if err != nil {
		return nil, err
	}

	expected := parseSnapPictureSize(resp.XML)

	var buf bytes.Buffer
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.closed:
			if err := c.closeErr.get(); err != nil {
				return nil, err
			}
			return nil, context.Canceled
		case msg := <-sub.ch:
			if msg == nil || !msg.Binary || len(msg.Payload) == 0 {
				continue
			}
			// a dropped chunk cannot be recovered, fail instead of assembling
			// a truncated JPEG
			if sub.dropped.Load() > 0 {
				return nil, fmt.Errorf("snapshot chunk lost, consumer too slow")
			}
			buf.Write(msg.Payload)

			final := msg.Header.ResponseCode == 201
			if img, done := snapComplete(&buf, expected, final); done {
				return img, nil
			}
			if final {
				return nil, fmt.Errorf("snapshot incomplete: got %d of %d bytes", buf.Len(), expected)
			}
		}
	}
}

func channelExtension(channel uint8) []byte {
	return fmt.Appendf(nil, `<?xml version="1.0" encoding="utf-8"?><Extension version="1.1"><channelId>%d</channelId></Extension>`, channel)
}

// jpegEOI is the JPEG end-of-image marker, the completion fallback when the
// camera does not announce a picture size.
var jpegEOI = []byte{0xff, 0xd9}

func snapComplete(buf *bytes.Buffer, expected int, final bool) ([]byte, bool) {
	data := buf.Bytes()
	if expected > 0 {
		if len(data) >= expected {
			return data[:expected], true
		}
		return nil, false
	}
	if final || bytes.HasSuffix(data, jpegEOI) {
		return data, true
	}
	return nil, false
}

func parseSnapPictureSize(xmlText string) int {
	if xmlText == "" {
		return 0
	}
	var reply xmlSnapReply
	if err := xml.Unmarshal([]byte(xmlText), &reply); err != nil || reply.Snap == nil {
		return 0
	}
	if reply.Snap.PictureSize < 0 {
		return 0
	}
	return reply.Snap.PictureSize
}
