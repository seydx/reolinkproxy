package baichuan

import (
	"context"
	"encoding/xml"
	"sync"
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
	sub, unsubscribe := c.Subscribe(msgIDFloodlightStatus)
	stop := make(chan struct{})

	go func() {
		defer unsubscribe()

		for {
			select {
			case <-ctx.Done():
				return
			case <-c.closed:
				return
			case <-stop:
				return
			case msg := <-sub:
				if msg == nil {
					continue
				}
				if on, ok := ParseFloodlightStates(msg.XML)[int(channel)]; ok {
					callback(on)
				}
			}
		}
	}()

	var stopOnce sync.Once
	return func() {
		stopOnce.Do(func() {
			close(stop)
		})
	}
}
