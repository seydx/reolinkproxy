package baichuan

import (
	"context"
	"encoding/xml"
	"slices"
	"strings"
	"sync"
)

// AlarmEventList contains a list of alarm events from the camera.
type AlarmEventList struct {
	AlarmEvents []AlarmEvent `xml:"AlarmEvent"`
}

// AlarmEvent represents a single motion or AI alarm event.
type AlarmEvent struct {
	ChannelID uint8         `xml:"channelId"`
	Status    string        `xml:"status"`
	AIType    string        `xml:"AItype"`
	SmartAI   []SmartAIType `xml:"smartAiTypeList>smartAiType"`
}

// SmartAIType is one zone-based smart detection (crossline, intrusion,
// linger) in an alarm event. Index is a bitmask of the zones currently
// detecting, SubList carries the AI type detected per zone.
type SmartAIType struct {
	Type    string       `xml:"type"`
	Index   int          `xml:"index"`
	SubList []SmartAISub `xml:"subList"`
}

// SmartAISub is one per-zone detection of a smart AI event.
type SmartAISub struct {
	Index int    `xml:"index"`
	Type  string `xml:"type"`
}

// AlarmMessage is the XML payload containing an AlarmEventList.
type AlarmMessage struct {
	AlarmEventList *AlarmEventList `xml:"AlarmEventList"`
}

// AlarmState is one decoded alarm update for a channel. Field semantics
// follow reolink_aio: motion is the "MD" status (or the unclassified "other"
// AI type on PIR-style cameras), a doorbell press arrives as the "visitor"
// status.
type AlarmState struct {
	// MotionDetected is true while the camera reports motion.
	MotionDetected bool
	// Visitor is true while a doorbell press is active.
	Visitor bool
	// AITypes lists the active AI classifications, e.g. "people", "vehicle"
	// or "dog_cat". Empty when none are active.
	AITypes []string
}

// Active reports whether any motion or AI detection is currently firing.
func (s AlarmState) Active() bool {
	return s.MotionDetected || len(s.AITypes) > 0
}

// RefreshAlarmSubscription (re-)requests the camera's event push. The
// subscription is host-wide, so it is addressed to the push channel rather than
// the camera's own channel; NVRs reject it otherwise and never push anything.
func (c *Client) RefreshAlarmSubscription(ctx context.Context) error {
	_, err := c.sendRequest(ctx, request{
		MsgID:     msgIDMotionRequest,
		ChannelID: channelIDPush,
		Class:     classModernWithOffset,
	})
	return err
}

// AlarmPushSeen reports whether the camera has pushed an alarm event on this
// connection yet. Some firmwares answer the subscription with a success and
// then stay silent, which is only visible as the absence of pushes.
func (c *Client) AlarmPushSeen() bool {
	return c.alarmPushed.Load()
}

// ListenForAlarms subscribes to motion/AI alarm events and invokes the
// callback with each decoded update for the channel.
func (c *Client) ListenForAlarms(ctx context.Context, channel uint8, callback func(AlarmState)) (func(), error) {
	if err := c.Login(ctx); err != nil {
		return nil, err
	}

	if err := c.requireAbilityRW(ctx, channel, "motion"); err != nil {
		return nil, err
	}

	if err := c.RefreshAlarmSubscription(ctx); err != nil {
		return nil, err
	}

	// dual-lens cameras may alarm on the telephoto stream channel, both
	// lenses belong to the channel-0 camera; a separately adopted tele
	// channel only owns its own alarms
	dualLens := channel == 0 && c.LoginDeviceInfo().IsDualLens()

	motionSub, unsubscribeMotion := c.Subscribe(msgIDMotion)
	stop := make(chan struct{})

	go func() {
		defer unsubscribeMotion()

		for {
			select {
			case <-ctx.Done():
				return
			case <-c.closed:
				return
			case <-stop:
				return
			case msg := <-motionSub:
				if msg == nil {
					continue
				}
				list := pushListName(msg.XML)
				// day/night changes share the alarm message id, so only a real
				// alarm list proves the camera pushes events. Any channel counts:
				// the subscription is host-wide.
				if list == "AlarmEventList" {
					c.alarmPushed.Store(true)
				}

				state, matched, err := parseAlarmState(msg.XML, channel, dualLens)
				switch {
				case err != nil:
					c.debugf("alarm push could not be parsed: %v (xml=%q)", err, msg.XML)
				case !matched:
					c.debugf("ignored %s push for channel %d", list, channel)
				default:
					c.debugf("alarm push channel %d: motion=%t visitor=%t ai=%v", channel, state.MotionDetected, state.Visitor, state.AITypes)
					callback(state)
				}
			}
		}
	}()

	var stopOnce sync.Once
	return func() {
		stopOnce.Do(func() {
			close(stop)
		})
	}, nil
}

func parseAlarmState(xmlText string, channel uint8, anyChannel bool) (AlarmState, bool, error) {
	if xmlText == "" {
		return AlarmState{}, false, nil
	}

	var payload AlarmMessage
	if err := xml.Unmarshal([]byte(xmlText), &payload); err != nil {
		return AlarmState{}, false, err
	}

	if payload.AlarmEventList == nil {
		return AlarmState{}, false, nil
	}

	var state AlarmState
	matched := false
	for _, ev := range payload.AlarmEventList.AlarmEvents {
		if !anyChannel && ev.ChannelID != channel {
			continue
		}
		matched = true
		if strings.Contains(ev.Status, "MD") {
			state.MotionDetected = true
		}
		if strings.Contains(ev.Status, "visitor") {
			state.Visitor = true
		}
		// PIR-style cameras report unclassified detections as AI type
		// "other" instead of an MD status.
		if strings.Contains(ev.AIType, "other") {
			state.MotionDetected = true
		}
		for _, aiType := range parseAITypes(ev.AIType) {
			if !slices.Contains(state.AITypes, aiType) {
				state.AITypes = append(state.AITypes, aiType)
			}
		}
		// zone-based smart detections (crossline, intrusion, linger) may
		// fire without an MD status or plain AItype
		for _, smart := range ev.SmartAI {
			if smart.Index > 0 {
				state.MotionDetected = true
			}
			for _, sub := range smart.SubList {
				types := parseAITypes(sub.Type)
				if len(types) == 0 {
					// a zone entry without a type is still a detection in that
					// zone; battery doorbells report linger that way
					state.MotionDetected = true
					continue
				}
				for _, aiType := range types {
					if !slices.Contains(state.AITypes, aiType) {
						state.AITypes = append(state.AITypes, aiType)
					}
				}
			}
		}
	}

	return state, matched, nil
}

// pushListName returns the list element below <body> so an ignored push can be
// named without dumping its payload. Cameras share the alarm message id with
// DayNightEventList and friends.
func pushListName(xmlText string) string {
	decoder := xml.NewDecoder(strings.NewReader(xmlText))
	depth := 0
	for {
		token, err := decoder.Token()
		if err != nil {
			return "unknown"
		}
		if start, ok := token.(xml.StartElement); ok {
			depth++
			if depth == 2 {
				return start.Name.Local
			}
		}
	}
}

// parseAITypes splits the AItype field ("people", "people&vehicle", …) into
// individual type names, dropping "none" and "other".
func parseAITypes(raw string) []string {
	if raw == "" {
		return nil
	}

	var out []string
	for _, part := range strings.FieldsFunc(raw, func(r rune) bool {
		return r == '&' || r == ',' || r == ' '
	}) {
		part = strings.ToLower(strings.TrimSpace(part))
		if part == "" || part == "none" || part == "other" {
			continue
		}
		out = append(out, part)
	}
	return out
}
