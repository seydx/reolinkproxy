package baichuan

import (
	"context"
	"encoding/xml"
	"sync"
)

// BatteryList is the XML payload battery cameras push periodically (msg 252),
// wrapping one BatteryInfo per channel.
type BatteryList struct {
	Items []BatteryInfo `xml:"BatteryInfo"`
}

type batteryListMessage struct {
	BatteryList *BatteryList `xml:"BatteryList"`
}

// ListenForBattery subscribes to spontaneous battery status pushes (sent by
// battery cameras without a request) and invokes the callback with each
// update for the channel. Mains-powered cameras never push, so the callback
// simply stays silent for them.
func (c *Client) ListenForBattery(ctx context.Context, channel uint8, callback func(BatteryInfo)) func() {
	infoSub, unsubscribeInfo := c.Subscribe(msgIDBatteryInfo)
	listSub, unsubscribeList := c.Subscribe(msgIDBatteryInfoList)
	stop := make(chan struct{})

	handle := func(msg *Message) {
		if msg == nil {
			return
		}
		for _, info := range parseBatteryPayload(msg.XML) {
			if info.ChannelID == int(channel) {
				callback(info)
			}
		}
	}

	go func() {
		defer unsubscribeInfo()
		defer unsubscribeList()

		for {
			select {
			case <-ctx.Done():
				return
			case <-c.closed:
				return
			case <-stop:
				return
			case msg := <-infoSub:
				handle(msg)
			case msg := <-listSub:
				handle(msg)
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

// parseBatteryPayload decodes both push shapes: a bare BatteryInfo (msg 253)
// and a BatteryList wrapper (msg 252).
func parseBatteryPayload(xmlText string) []BatteryInfo {
	if xmlText == "" {
		return nil
	}

	var single BatteryMessage
	if err := xml.Unmarshal([]byte(xmlText), &single); err == nil && single.BatteryInfo != nil {
		return []BatteryInfo{*single.BatteryInfo}
	}

	var list batteryListMessage
	if err := xml.Unmarshal([]byte(xmlText), &list); err == nil && list.BatteryList != nil {
		return list.BatteryList.Items
	}

	return nil
}
