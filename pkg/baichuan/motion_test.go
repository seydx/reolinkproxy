package baichuan

import (
	"slices"
	"testing"
)

func TestParseAlarmStateMatchingChannel(t *testing.T) {
	t.Parallel()

	xmlText := `<?xml version="1.0" encoding="utf-8"?><body><AlarmEventList><AlarmEvent><channelId>0</channelId><status>none</status><AItype>none</AItype></AlarmEvent></AlarmEventList></body>`

	state, matched, err := parseAlarmState(xmlText, 0)
	if err != nil {
		t.Fatalf("parseAlarmState() error = %v", err)
	}
	if !matched {
		t.Fatalf("parseAlarmState() matched = false, want true")
	}
	if state.Active() {
		t.Fatalf("parseAlarmState() active = true, want false")
	}
}

func TestParseAlarmStateIgnoresOtherChannels(t *testing.T) {
	t.Parallel()

	xmlText := `<?xml version="1.0" encoding="utf-8"?><body><AlarmEventList><AlarmEvent><channelId>1</channelId><status>MD</status><AItype>people</AItype></AlarmEvent></AlarmEventList></body>`

	state, matched, err := parseAlarmState(xmlText, 0)
	if err != nil {
		t.Fatalf("parseAlarmState() error = %v", err)
	}
	if matched {
		t.Fatalf("parseAlarmState() matched = true, want false")
	}
	if state.Active() {
		t.Fatalf("parseAlarmState() active = true, want false")
	}
}

func TestParseAlarmStateDecodesAITypes(t *testing.T) {
	t.Parallel()

	xmlText := `<?xml version="1.0" encoding="utf-8"?><body><AlarmEventList><AlarmEvent><channelId>0</channelId><status>MD</status><AItype>people&amp;vehicle</AItype></AlarmEvent></AlarmEventList></body>`

	state, matched, err := parseAlarmState(xmlText, 0)
	if err != nil {
		t.Fatalf("parseAlarmState() error = %v", err)
	}
	if !matched {
		t.Fatalf("parseAlarmState() matched = false, want true")
	}
	if !state.MotionDetected {
		t.Fatalf("parseAlarmState() motionDetected = false, want true")
	}
	if !slices.Equal(state.AITypes, []string{"people", "vehicle"}) {
		t.Fatalf("parseAlarmState() aiTypes = %v, want [people vehicle]", state.AITypes)
	}
}

func TestParseAlarmStateVisitorWithoutMotion(t *testing.T) {
	t.Parallel()

	xmlText := `<?xml version="1.0" encoding="utf-8"?><body><AlarmEventList><AlarmEvent><channelId>0</channelId><status>visitor</status><AItype>none</AItype></AlarmEvent></AlarmEventList></body>`

	state, matched, err := parseAlarmState(xmlText, 0)
	if err != nil {
		t.Fatalf("parseAlarmState() error = %v", err)
	}
	if !matched {
		t.Fatalf("parseAlarmState() matched = false, want true")
	}
	if state.MotionDetected {
		t.Fatalf("parseAlarmState() motionDetected = true, want false")
	}
	if !state.Visitor {
		t.Fatalf("parseAlarmState() visitor = false, want true")
	}
	if len(state.AITypes) != 0 {
		t.Fatalf("parseAlarmState() aiTypes = %v, want empty", state.AITypes)
	}
}

func TestParseAlarmStateOtherAITypeCountsAsMotion(t *testing.T) {
	t.Parallel()

	xmlText := `<?xml version="1.0" encoding="utf-8"?><body><AlarmEventList><AlarmEvent><channelId>0</channelId><status>none</status><AItype>other</AItype></AlarmEvent></AlarmEventList></body>`

	state, matched, err := parseAlarmState(xmlText, 0)
	if err != nil {
		t.Fatalf("parseAlarmState() error = %v", err)
	}
	if !matched {
		t.Fatalf("parseAlarmState() matched = false, want true")
	}
	if !state.MotionDetected {
		t.Fatalf("parseAlarmState() motionDetected = false, want true (PIR other)")
	}
	if len(state.AITypes) != 0 {
		t.Fatalf("parseAlarmState() aiTypes = %v, want empty", state.AITypes)
	}
}

func TestParseAbilityInfoFiltersByChannel(t *testing.T) {
	t.Parallel()

	xmlText := `<?xml version="1.0" encoding="utf-8"?><body><AbilityInfo><userName>admin</userName><alarm><subModule><channelId>1</channelId><abilityValue>motion_rw</abilityValue></subModule><subModule><channelId>0</channelId><abilityValue>motion_ro,rfAlarm_ro</abilityValue></subModule></alarm><system><subModule><abilityValue>version_ro</abilityValue></subModule></system></AbilityInfo></body>`

	abilities, err := parseAbilityInfo(xmlText, 0)
	if err != nil {
		t.Fatalf("parseAbilityInfo() error = %v", err)
	}
	if got, want := abilities["motion"], abilityReadOnly; got != want {
		t.Fatalf("abilities[motion] = %v, want %v", got, want)
	}
	if got, want := abilities["rfalarm"], abilityReadOnly; got != want {
		t.Fatalf("abilities[rfalarm] = %v, want %v", got, want)
	}
	if got, want := abilities["version"], abilityReadOnly; got != want {
		t.Fatalf("abilities[version] = %v, want %v", got, want)
	}
}
