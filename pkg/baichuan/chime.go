package baichuan

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Chime is a Reolink chime paired with a doorbell.
type Chime struct {
	ID     int
	Name   string
	Online bool
}

// ChimeParams are the chime settings that msg 485 reads and writes together.
type ChimeParams struct {
	Name string
	// Volume is the chime's level from 0 to 4.
	Volume int
	LED    bool
}

type xmlDingDongOptBody[T any] struct {
	XMLName xml.Name `xml:"body"`
	Opt     T        `xml:"dingdongDeviceOpt"`
}

type xmlDingDongGetParam struct {
	Version string `xml:"version,attr"`
	ID      int    `xml:"id"`
	Opt     string `xml:"opt"`
}

type xmlDingDongSetParam struct {
	Version  string `xml:"version,attr"`
	Opt      string `xml:"opt"`
	ID       int    `xml:"id"`
	VolLevel int    `xml:"volLevel"`
	LedState int    `xml:"ledState"`
	Name     string `xml:"name"`
}

type xmlDingDongRing struct {
	Version string `xml:"version,attr"`
	ID      int    `xml:"id"`
	Opt     string `xml:"opt"`
	MusicID int    `xml:"musicId"`
}

type xmlChimeInfo struct {
	ID         int    `xml:"id"`
	Name       string `xml:"name"`
	NetState   *int   `xml:"netState"`
	NetStateLC *int   `xml:"netstate"`
	DeviceType string `xml:"deviceType"`
}

type xmlChimeCfg struct {
	ID    int    `xml:"id"`
	Inner string `xml:",innerxml"`
}

type xmlChimeAlarm struct {
	Type    string `xml:"type"`
	Valid   int    `xml:"valid"`
	MusicID int    `xml:"musicId"`
}

// Chimes lists the chimes paired with the channel's doorbell (msg 484).
func (c *Client) Chimes(ctx context.Context, channel uint8) ([]Chime, error) {
	resp, err := c.execCommand(ctx, msgIDDingDongList, channel, nil)
	if err != nil {
		return nil, err
	}
	return parseChimes(resp.XML)
}

// ChimeTones returns the tone each chime plays for an event type such as
// "visitor", -1 for a chime that stays silent on it (msg 486).
func (c *Client) ChimeTones(ctx context.Context, channel uint8, event string) (map[int]int, error) {
	resp, err := c.execCommand(ctx, msgIDDingDongCfg, channel, nil)
	if err != nil {
		return nil, err
	}
	return parseChimeTones(resp.XML, event)
}

// ChimeParams reads a chime's name, volume and LED state (msg 485).
func (c *Client) ChimeParams(ctx context.Context, channel uint8, chimeID int) (ChimeParams, error) {
	resp, err := c.execCommand(ctx, msgIDDingDongOpt, channel, xmlDingDongOptBody[xmlDingDongGetParam]{
		Opt: xmlDingDongGetParam{Version: "1.1", ID: chimeID, Opt: "getParam"},
	})
	if err != nil {
		return ChimeParams{}, err
	}
	return parseChimeParams(resp.XML)
}

// SetChimeParams writes a chime's name, volume and LED state (msg 485).
func (c *Client) SetChimeParams(ctx context.Context, channel uint8, chimeID int, params ChimeParams) error {
	_, err := c.execCommand(ctx, msgIDDingDongOpt, channel, xmlDingDongOptBody[xmlDingDongSetParam]{
		Opt: xmlDingDongSetParam{
			Version:  "1.1",
			Opt:      "setParam",
			ID:       chimeID,
			VolLevel: params.Volume,
			LedState: boolInt(params.LED),
			Name:     params.Name,
		},
	})
	return err
}

// RingChime plays one of the chime's tones once (msg 485).
func (c *Client) RingChime(ctx context.Context, channel uint8, chimeID int, tone int) error {
	_, err := c.execCommand(ctx, msgIDDingDongOpt, channel, xmlDingDongOptBody[xmlDingDongRing]{
		Opt: xmlDingDongRing{Version: "1.1", ID: chimeID, Opt: "ringWithMusic", MusicID: tone},
	})
	return err
}

func parseChimes(xmlText string) ([]Chime, error) {
	infos, err := decodeElements[xmlChimeInfo](xmlText, "dingdongDeviceInfo")
	if err != nil {
		return nil, fmt.Errorf("parse chime list: %w", err)
	}
	chimes := make([]Chime, 0, len(infos))
	for _, info := range infos {
		// the list names a battery doorbell itself too
		if info.DeviceType == "BatteryDB" {
			continue
		}
		state := info.NetState
		if state == nil {
			state = info.NetStateLC
		}
		chimes = append(chimes, Chime{ID: info.ID, Name: strings.TrimSpace(info.Name), Online: state != nil && *state == 2})
	}
	return chimes, nil
}

func parseChimeTones(xmlText string, event string) (map[int]int, error) {
	cfgs, err := decodeElements[xmlChimeCfg](xmlText, "deviceCfg")
	if err != nil {
		return nil, fmt.Errorf("parse chime config: %w", err)
	}
	tones := make(map[int]int, len(cfgs))
	for _, cfg := range cfgs {
		alarms, err := decodeElements[xmlChimeAlarm](cfg.Inner, "alarminCfg")
		if err != nil {
			return nil, fmt.Errorf("parse chime config: %w", err)
		}
		for _, alarm := range alarms {
			if !strings.EqualFold(strings.TrimSpace(alarm.Type), event) {
				continue
			}
			tone := alarm.MusicID
			if alarm.Valid != 1 {
				tone = -1
			}
			tones[cfg.ID] = tone
		}
	}
	return tones, nil
}

func parseChimeParams(xmlText string) (ChimeParams, error) {
	volume, err := strconv.Atoi(strings.TrimSpace(xmlElementText(xmlText, "volLevel")))
	if err != nil {
		return ChimeParams{}, fmt.Errorf("chime params without a volume: %w", err)
	}
	return ChimeParams{
		Name:   xmlElementText(xmlText, "name"),
		Volume: volume,
		LED:    strings.TrimSpace(xmlElementText(xmlText, "ledState")) == "1",
	}, nil
}

func decodeElements[T any](xmlText string, name string) ([]T, error) {
	dec := xml.NewDecoder(strings.NewReader(xmlText))
	var out []T
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		start, ok := tok.(xml.StartElement)
		if !ok || start.Name.Local != name {
			continue
		}
		var v T
		if err := dec.DecodeElement(&v, &start); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
