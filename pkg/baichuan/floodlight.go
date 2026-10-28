package baichuan

import (
	"context"
	"encoding/xml"
)

type xmlFloodlightStatusBody struct {
	Entries []struct {
		Channel int `xml:"channel"`
		Status  int `xml:"status"`
	} `xml:"FloodlightStatusList>FloodlightStatus"`
}

// ParseFloodlightStates decodes a floodlight status push (cmd 291) into the
// per-channel spotlight state.
func ParseFloodlightStates(xmlText string) map[int]bool {
	if xmlText == "" {
		return nil
	}
	var body xmlFloodlightStatusBody
	if err := xml.Unmarshal([]byte(xmlText), &body); err != nil {
		return nil
	}

	states := make(map[int]bool, len(body.Entries))
	for _, entry := range body.Entries {
		states[entry.Channel] = entry.Status != 0
	}
	return states
}

// ListenForFloodlight subscribes to floodlight status pushes and reports the
// channel's spotlight state. The camera pushes its current state shortly after
// login and on every change, including changes it made itself.
func (c *Client) ListenForFloodlight(ctx context.Context, channel uint8, callback func(on bool)) func() {
	return c.listenForStates(ctx, msgIDFloodlightStatus, ParseFloodlightStates, channel, callback)
}
