package bridge

import (
	"slices"
	"strings"
	"sync"

	"github.com/shareed2k/reolinkproxy/pkg/baichuan"
)

// MotionEvent reports a motion/AI detection state change.
type MotionEvent struct {
	// Active is true while motion or any AI detection is firing.
	Active bool
	// AITypes lists the active AI classifications ("people", "vehicle",
	// "dog_cat", …). Doorbell presses are delivered via OnDoorbell instead.
	AITypes []string
}

// BatteryState reports battery metrics pushed by battery cameras.
type BatteryState struct {
	Percent int
	// Charging is true while actively charging; Full is true when charged and
	// still on the charger.
	Charging     bool
	Full         bool
	ChargeStatus string
	LowPower     bool
	Temperature  int
	Voltage      int
}

// cameraEvents holds the registered handlers and the dispatch state used for
// edge detection and deduplication.
type cameraEvents struct {
	mu           sync.Mutex
	onMotion     func(MotionEvent)
	onDoorbell   func()
	onBattery    func(BatteryState)
	onConnection func(connected bool)
	onSleep      func(sleeping bool)

	lastVisitor    bool
	lastMotion     *MotionEvent
	lastSleep      *bool
	lastConnection *bool
}

// OnMotion registers the handler invoked on motion/AI state changes.
func (c *Camera) OnMotion(fn func(MotionEvent)) {
	c.events.mu.Lock()
	defer c.events.mu.Unlock()
	c.events.onMotion = fn
}

// OnDoorbell registers the handler invoked when a visitor presses the doorbell.
func (c *Camera) OnDoorbell(fn func()) {
	c.events.mu.Lock()
	defer c.events.mu.Unlock()
	c.events.onDoorbell = fn
}

// OnBattery registers the handler invoked with battery status pushes.
func (c *Camera) OnBattery(fn func(BatteryState)) {
	c.events.mu.Lock()
	defer c.events.mu.Unlock()
	c.events.onBattery = fn
}

// OnConnection registers the handler invoked when the Baichuan connection is
// established (true) or lost (false). AddCamera already starts connecting, so
// a state reached before this call is replayed instead of lost.
func (c *Camera) OnConnection(fn func(connected bool)) {
	c.events.mu.Lock()
	c.events.onConnection = fn
	last := c.events.lastConnection
	c.events.mu.Unlock()

	if fn != nil && last != nil {
		fn(*last)
	}
}

// OnSleep registers the handler invoked when a battery camera enters (true)
// or leaves (false) standby.
func (c *Camera) OnSleep(fn func(sleeping bool)) {
	c.events.mu.Lock()
	defer c.events.mu.Unlock()
	c.events.onSleep = fn
}

// handleSleep dispatches deduplicated sleep-state changes.
func (c *Camera) handleSleep(sleeping bool) {
	c.events.mu.Lock()
	changed := c.events.lastSleep == nil || *c.events.lastSleep != sleeping
	c.events.lastSleep = &sleeping
	onSleep := c.events.onSleep
	c.events.mu.Unlock()

	if changed && onSleep != nil {
		onSleep(sleeping)
	}
}

// handleAlarm turns a raw alarm update into motion/doorbell events: a
// visitor press fires OnDoorbell on its rising edge, motion/AI feeds the
// motion state (deduplicated) and the pause logic.
func (c *Camera) handleAlarm(state baichuan.AlarmState) {
	event := MotionEvent{
		Active:  state.Active(),
		AITypes: state.AITypes,
	}

	c.events.mu.Lock()
	fireDoorbell := state.Visitor && !c.events.lastVisitor
	c.events.lastVisitor = state.Visitor
	fireMotion := c.events.lastMotion == nil ||
		c.events.lastMotion.Active != event.Active ||
		!slices.Equal(c.events.lastMotion.AITypes, event.AITypes)
	c.events.lastMotion = &event
	onDoorbell := c.events.onDoorbell
	onMotion := c.events.onMotion
	c.events.mu.Unlock()

	if c.motion != nil {
		c.motion.setActive(event.Active)
	}
	if fireDoorbell && onDoorbell != nil {
		onDoorbell()
	}
	if fireMotion && onMotion != nil {
		onMotion(event)
	}
}

func (c *Camera) handleBattery(info baichuan.BatteryInfo) {
	c.events.mu.Lock()
	onBattery := c.events.onBattery
	c.events.mu.Unlock()

	if onBattery != nil {
		onBattery(batteryStateFromInfo(info))
	}
}

func (c *Camera) handleConnection(connected bool) {
	c.events.mu.Lock()
	c.events.lastConnection = &connected
	onConnection := c.events.onConnection
	c.events.mu.Unlock()

	if onConnection != nil {
		onConnection(connected)
	}
}

func batteryStateFromInfo(info baichuan.BatteryInfo) BatteryState {
	status := strings.ToLower(info.ChargeStatus)
	return BatteryState{
		Percent:  info.BatteryPercent,
		Charging: status == "charging",
		// "chargecomplete" = full on charger; "none" = discharging.
		Full:         status == "chargecomplete",
		ChargeStatus: info.ChargeStatus,
		LowPower:     info.LowPower != 0,
		Temperature:  info.Temperature,
		Voltage:      info.Voltage,
	}
}
