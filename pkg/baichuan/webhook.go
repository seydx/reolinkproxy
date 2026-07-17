package baichuan

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"sync"
)

// Webhook support (battery cameras): instead of a persistent TCP event
// subscription — which would keep a sleeping camera awake — the camera is
// given an HTTP URL (msg 807, "HaCfg") and pushes events itself. Payloads are
// JSON: either {uid, cmd, xml} carrying the same XML bodies as TCP pushes, or
// {uid, data:{event, reason}} for sleep/wake transitions.

type xmlHaCfgBody struct {
	XMLName xml.Name `xml:"body"`
	HaCfg   xmlHaCfg `xml:"HaCfg"`
}

type xmlHaCfg struct {
	Version    string `xml:"version,attr,omitempty"`
	Enable     int    `xml:"enable"`
	URL        string `xml:"url"`
	VerifyCert int    `xml:"verify_cert"`
}

type xmlHaCfgReply struct {
	HaCfg *struct {
		Enable int    `xml:"enable"`
		URL    string `xml:"url"`
	} `xml:"HaCfg"`
}

// SetWebhook enables or disables the camera's event push webhook. Returns a
// StatusError on firmwares without webhook support.
func (c *Client) SetWebhook(ctx context.Context, enable bool, url string) error {
	enableInt := 0
	if enable {
		enableInt = 1
	}
	_, err := c.execHostCommand(ctx, msgIDWebhookSet, xmlHaCfgBody{
		HaCfg: xmlHaCfg{Version: "1.1", Enable: enableInt, URL: url},
	})
	return err
}

// GetWebhook reads the current webhook configuration. A successful reply also
// serves as the capability check — unsupported firmwares answer with a status
// error.
func (c *Client) GetWebhook(ctx context.Context) (enabled bool, url string, err error) {
	resp, err := c.execHostCommand(ctx, msgIDWebhookGet, nil)
	if err != nil {
		return false, "", err
	}

	var reply xmlHaCfgReply
	if err := xml.Unmarshal([]byte(resp.XML), &reply); err != nil {
		return false, "", fmt.Errorf("parse webhook XML: %w", err)
	}
	if reply.HaCfg == nil {
		return false, "", fmt.Errorf("no HaCfg in response")
	}
	return reply.HaCfg.Enable == 1, reply.HaCfg.URL, nil
}

// WebhookPush is one decoded webhook POST body.
type WebhookPush struct {
	// UID is the camera UID the push originates from.
	UID string `json:"uid"`
	// Cmd and XML mirror a TCP push: the XML body of the given command ID
	// (33 alarms, 145 sleep, 252/253 battery, ...). Zero/empty for the
	// event form.
	Cmd int    `json:"cmd"`
	XML string `json:"xml"`
	// Data is the simple event form: event "sleep"/"wake"/"test", with wake
	// reason "doorbell", "pir", "network" or "other".
	Data map[string]string `json:"data"`
}

// ParseWebhookPush decodes a webhook POST body.
func ParseWebhookPush(body []byte) (WebhookPush, error) {
	var push WebhookPush
	if err := json.Unmarshal(body, &push); err != nil {
		return WebhookPush{}, fmt.Errorf("parse webhook push: %w", err)
	}
	return push, nil
}

// ParseAlarmStateXML decodes an alarm push XML body (cmd 33) for a channel,
// e.g. from a webhook push. matched is false when the payload carries no
// event for the channel.
func ParseAlarmStateXML(xmlText string, channel uint8) (state AlarmState, matched bool, err error) {
	return parseAlarmState(xmlText, channel)
}

// ParseBatteryXML decodes a battery push XML body (cmd 252/253), e.g. from a
// webhook push.
func ParseBatteryXML(xmlText string) []BatteryInfo {
	return parseBatteryPayload(xmlText)
}

// ListenForSleep subscribes to channel-info pushes (msg 145) and invokes the
// callback with the channel's sleep state on each update.
func (c *Client) ListenForSleep(ctx context.Context, channel uint8, callback func(sleeping bool)) func() {
	sub, unsubscribe := c.Subscribe(msgIDChannelInfoList)
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
				if sleeping, ok := ParseSleepStates(msg.XML)[int(channel)]; ok {
					callback(sleeping)
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

type xmlChannelInfoListBody struct {
	ChannelInfos []struct {
		ChannelID  int    `xml:"channelId"`
		LoginState string `xml:"loginState"`
	} `xml:"ChannelInfoList>ChannelInfo"`
}

// ParseSleepStates decodes a channel-info push XML body (cmd 145) into the
// per-channel sleep state (loginState "standby" = sleeping).
func ParseSleepStates(xmlText string) map[int]bool {
	if xmlText == "" {
		return nil
	}
	var body xmlChannelInfoListBody
	if err := xml.Unmarshal([]byte(xmlText), &body); err != nil {
		return nil
	}

	states := make(map[int]bool, len(body.ChannelInfos))
	for _, info := range body.ChannelInfos {
		states[info.ChannelID] = info.LoginState == "standby"
	}
	return states
}
