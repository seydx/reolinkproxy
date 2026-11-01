package baichuan

import (
	"context"
	"encoding/xml"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"
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

// AlarmMessage is the XML payload of an alarm push. Firmwares differ in the
// list element they wrap events in, so every list below <body> is scanned and
// only AlarmEvent children count (reolink_aio does the same).
type AlarmMessage struct {
	Lists []AlarmEventList `xml:",any"`
}

// the wrapper names of a yolo push are unknown, reolink_aio walks them blind too
type yoloMessage struct {
	Lists []yoloEventList `xml:",any"`
}

type yoloEventList struct {
	Events []yoloEvent `xml:",any"`
}

type yoloEvent struct {
	Channel *uint8     `xml:"channel"`
	Types   []yoloType `xml:"YoloWorldType"`
}

type yoloType struct {
	Type string `xml:"type"`
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

const (
	busyAttempts = 3
	// a camera still busy after connecting, or a sleeping one woken by the refused request, answers within this
	busyBackoff = 1500 * time.Millisecond
)

// RefreshAlarmSubscription (re-)requests the camera's event push. The
// subscription is host-wide, so it is addressed to the push channel rather than
// the camera's own channel; NVRs reject it otherwise and never push anything.
func (c *Client) RefreshAlarmSubscription(ctx context.Context) error {
	_, err := c.retryWhileBusy(ctx, func() (*Message, error) {
		return c.sendRequest(ctx, request{
			MsgID:     msgIDMotionRequest,
			ChannelID: channelIDPush,
			Class:     classModernWithOffset,
		})
	})
	return err
}

func (c *Client) retryWhileBusy(ctx context.Context, send func() (*Message, error)) (*Message, error) {
	for attempt := 1; ; attempt++ {
		msg, err := send()
		if err == nil || !isBusyStatus(err) || attempt == busyAttempts {
			return msg, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.closed:
			return nil, err
		case <-time.After(busyBackoff):
		}
	}
}

func isBusyStatus(err error) bool {
	var statusErr *StatusError
	return errors.As(err, &statusErr) && statusErr.Code == 400
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

	// keep the receiver whatever the camera answers, the renewal retries:
	// some models answer 400 forever and push anyway, others refuse with 402
	// or 405, and failing here resets the shared connection with its streams
	if err := c.RefreshAlarmSubscription(ctx); err != nil {
		if _, ok := errors.AsType[*StatusError](err); !ok {
			return nil, err
		}
		c.debugf("camera did not accept the event subscription yet (%v), retrying in the background", err)
	}

	// dual-lens cameras may alarm on the telephoto stream channel, both
	// lenses belong to the channel-0 camera; a separately adopted tele
	// channel only owns its own alarms
	dualLens := channel == 0 && c.LoginDeviceInfo().IsDualLens()

	motionSub, unsubscribeMotion := c.Subscribe(msgIDMotion)
	yoloSub, unsubscribeYolo := c.Subscribe(msgIDYoloAlarm)
	yoloDetailSub, unsubscribeYoloDetail := c.Subscribe(msgIDYoloAlarmDetail)
	stop := make(chan struct{})

	go func() {
		defer unsubscribeMotion()
		defer unsubscribeYolo()
		defer unsubscribeYoloDetail()

		// cameras with YOLO World report people, vehicles and bicycles apart from the alarm push
		var alarm AlarmState
		var yolo, yoloDetail []string
		report := func() {
			state := withAITypes(alarm, yolo, yoloDetail)
			c.debugf("alarm push channel %d: motion=%t visitor=%t ai=%v", channel, state.MotionDetected, state.Visitor, state.AITypes)
			callback(state)
		}
		updateYolo := func(msg *Message, current *[]string, basic bool) {
			res, err := parseYolo(msg.XML, channel, dualLens)
			if err != nil {
				c.debugf("yolo push could not be parsed: %v (xml=%q)", err, msg.XML)
				return
			}
			if res.hasEvent {
				c.alarmPushed.Store(true)
			}
			// a basic push lists every channel still detecting, a detailed one only the channels it names
			if !res.matched && !basic {
				return
			}
			if slices.Equal(*current, res.state.AITypes) {
				return
			}
			*current = res.state.AITypes
			report()
		}

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
				res, err := parseAlarm(msg.XML, channel, dualLens)
				// day/night changes share the alarm message id, so only a push
				// carrying real events proves the camera reports alarms. Any
				// channel counts: the subscription is host-wide.
				if res.hasEvent {
					c.alarmPushed.Store(true)
				}

				switch {
				case err != nil:
					c.debugf("alarm push could not be parsed: %v (xml=%q)", err, msg.XML)
				case res.matched:
					alarm = res.state
					report()
				case res.hasEvent && !c.LoginDeviceInfo().IsNVR():
					// the camera reports alarms, just not for the channel this
					// camera was adopted as; silently dropping them looks like
					// a camera that never detects anything. A recorder pushes
					// every channel's alarms on each connection.
					c.foreignAlarmOnce.Do(func() {
						c.warnf("camera pushes alarm events for channel(s) %v, this camera listens on channel %d, so its detections stay idle", res.channels, channel)
					})
				default:
					c.debugf("ignored %s push for channel %d", pushListName(msg.XML), channel)
				}
			case msg := <-yoloSub:
				if msg != nil {
					updateYolo(msg, &yolo, true)
				}
			case msg := <-yoloDetailSub:
				if msg != nil {
					updateYolo(msg, &yoloDetail, false)
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

// alarmParse is what one alarm push decoded to. hasEvent separates a real
// alarm push from the day/night and friends that share the message id;
// channels names who the events were for, so a push meant for nobody we know
// can be reported instead of silently dropped.
type alarmParse struct {
	state    AlarmState
	matched  bool
	hasEvent bool
	channels []uint8
}

func parseAlarmState(xmlText string, channel uint8, anyChannel bool) (AlarmState, bool, error) {
	res, err := parseAlarm(xmlText, channel, anyChannel)
	return res.state, res.matched, err
}

func parseAlarm(xmlText string, channel uint8, anyChannel bool) (alarmParse, error) {
	if xmlText == "" {
		return alarmParse{}, nil
	}

	var payload AlarmMessage
	if err := xml.Unmarshal([]byte(xmlText), &payload); err != nil {
		return alarmParse{}, err
	}

	var res alarmParse
	state := &res.state
	for _, list := range payload.Lists {
		for _, ev := range list.AlarmEvents {
			res.hasEvent = true
			res.channels = appendNew(res.channels, ev.ChannelID)
			if !anyChannel && ev.ChannelID != channel {
				continue
			}
			res.matched = true
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
			state.AITypes = appendNew(state.AITypes, parseAITypes(ev.AIType)...)
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
					state.AITypes = appendNew(state.AITypes, types...)
				}
			}
		}
	}

	return res, nil
}

func parseYolo(xmlText string, channel uint8, anyChannel bool) (alarmParse, error) {
	if xmlText == "" {
		return alarmParse{}, nil
	}

	var payload yoloMessage
	if err := xml.Unmarshal([]byte(xmlText), &payload); err != nil {
		return alarmParse{}, err
	}

	var res alarmParse
	for _, list := range payload.Lists {
		for _, ev := range list.Events {
			if ev.Channel == nil {
				continue
			}
			res.hasEvent = true
			res.channels = appendNew(res.channels, *ev.Channel)
			if !anyChannel && *ev.Channel != channel {
				continue
			}
			res.matched = true
			for _, t := range ev.Types {
				if aiType := yoloAIType(t.Type); aiType != "" {
					res.state.AITypes = appendNew(res.state.AITypes, aiType)
				}
			}
		}
	}
	return res, nil
}

// yoloAIType names a YOLO World class the way the alarm push names its AI types.
func yoloAIType(raw string) string {
	switch name := strings.ToLower(strings.TrimSpace(raw)); name {
	case "person":
		return "people"
	case "motor vehicle":
		return "vehicle"
	case "animal":
		return "dog_cat"
	default:
		return name
	}
}

func withAITypes(state AlarmState, more ...[]string) AlarmState {
	state.AITypes = slices.Clone(state.AITypes)
	for _, types := range more {
		state.AITypes = appendNew(state.AITypes, types...)
	}
	return state
}

func appendNew[T comparable](list []T, items ...T) []T {
	for _, item := range items {
		if !slices.Contains(list, item) {
			list = append(list, item)
		}
	}
	return list
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
