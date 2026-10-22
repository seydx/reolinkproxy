package baichuan

import (
	"context"
	"encoding/xml"
	"fmt"
	"strings"
)

// ChannelCapabilities is the decoded hardware capability set of one channel,
// derived from the Support report (msg 199). Bit interpretations follow
// reolink_aio.
type ChannelCapabilities struct {
	Channel int
	// PTZ is true when the channel has any motorized control; Pan/Tilt/Zoom
	// describe the supported axes.
	PTZ  bool
	Pan  bool
	Tilt bool
	Zoom bool
	// Battery is true for battery-powered cameras.
	Battery bool
	// Doorbell is true when the channel is a video doorbell.
	Doorbell bool
	// Siren is true when the camera has a controllable siren.
	Siren bool
	// Floodlight is true when the camera has a controllable white-light LED.
	Floodlight bool
	// Talk is true when the channel has a speaker for two-way audio.
	Talk bool
	// Motion is true when the channel reports motion detection.
	Motion bool
	// AITypes lists the supported AI detections ("people", "vehicle", "face",
	// "dog_cat", "package"). Empty for non-AI cameras.
	AITypes []string
}

// Support is the decoded device capability report (msg 199).
type Support struct {
	ChannelNum int
	// AudioTalk is true when the device supports two-way audio.
	AudioTalk bool
	// ExternStream is false when the firmware flags noExternStream.
	ExternStream bool
	Channels     []ChannelCapabilities
}

// CapabilitiesFor returns the capability set of a channel.
func (s *Support) CapabilitiesFor(channel uint8) (ChannelCapabilities, bool) {
	for _, ch := range s.Channels {
		if ch.Channel == int(channel) {
			return ch, true
		}
	}
	return ChannelCapabilities{}, false
}

type xmlSupportBody struct {
	Support *xmlSupport `xml:"Support"`
}

type xmlSupport struct {
	ChannelNum     int              `xml:"channelNum"`
	PTZMode        string           `xml:"ptzMode"`
	AudioTalk      int              `xml:"audioTalk"`
	NoExternStream int              `xml:"noExternStream"`
	Items          []xmlSupportItem `xml:"item"`
}

type xmlSupportItem struct {
	ChnID           int    `xml:"chnID"`
	ChannelType     string `xml:"channelType"`
	PTZType         int    `xml:"ptzType"`
	Battery         int    `xml:"battery"`
	DoorbellVersion int    `xml:"doorbellVersion"`
	AudioVersion    int    `xml:"audioVersion"`
	LedCtrl         int    `xml:"ledCtrl"`
	IPCAudioTalk    *int   `xml:"ipcAudioTalk"`
	Motion          int    `xml:"motion"`
	AIType          int    `xml:"aitype"`
}

// GetSupport queries and decodes the device capability report (msg 199).
func (c *Client) GetSupport(ctx context.Context) (*Support, error) {
	resp, err := c.execHostCommand(ctx, msgIDGetSupport, nil)
	if err != nil {
		return nil, err
	}

	var body xmlSupportBody
	if err := xml.Unmarshal([]byte(resp.XML), &body); err != nil {
		return nil, fmt.Errorf("parse support XML: %w", err)
	}
	if body.Support == nil {
		return nil, fmt.Errorf("no Support in response")
	}

	raw := body.Support
	support := &Support{
		ChannelNum:   raw.ChannelNum,
		AudioTalk:    raw.AudioTalk != 0,
		ExternStream: raw.NoExternStream == 0,
	}

	// an NVR's audioTalk describes the recorder, its cameras answer with ipcAudioTalk per item
	deviceTalk := support.AudioTalk && !c.LoginDeviceInfo().IsNVR()
	for _, item := range raw.Items {
		support.Channels = append(support.Channels, decodeChannelCapabilities(item, deviceTalk))
	}
	return support, nil
}

func decodeChannelCapabilities(item xmlSupportItem, deviceTalk bool) ChannelCapabilities {
	caps := ChannelCapabilities{
		Channel:  item.ChnID,
		PTZ:      item.PTZType != 0,
		Battery:  item.Battery > 0,
		Doorbell: item.DoorbellVersion > 0,
		Motion:   item.Motion > 0,
		// audioVersion bit 2 → siren playback.
		Siren: (item.AudioVersion>>2)&1 == 1,
		// ledCtrl bits 1+2 → controllable floodlight.
		Floodlight: (item.LedCtrl>>1)&1 == 1 && (item.LedCtrl>>2)&1 == 1,
		// speakerless models still answer TalkAbility and accept one TalkConfig, then refuse talk until reboot
		Talk: deviceTalk,
	}
	if item.IPCAudioTalk != nil {
		caps.Talk = *item.IPCAudioTalk != 0
	}
	// a doorbell behind an NVR or Hub can report doorbellVersion 0 and only name its channel type
	if strings.EqualFold(item.ChannelType, "doorbell") {
		caps.Doorbell = true
	}

	switch item.PTZType {
	case 2, 3, 5, 6, 7:
		caps.Pan = true
	}
	switch item.PTZType {
	case 2, 3, 5, 6:
		caps.Tilt = true
	}
	switch item.PTZType {
	case 1, 2, 5:
		caps.Zoom = true
	}

	for _, ai := range []struct {
		bit  int
		name string
	}{
		{1, "people"},
		{2, "vehicle"},
		{3, "face"},
		{4, "dog_cat"},
		{17, "package"},
	} {
		if (item.AIType>>ai.bit)&1 == 1 {
			caps.AITypes = append(caps.AITypes, ai.name)
		}
	}

	return caps
}
