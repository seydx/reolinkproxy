package baichuan

import (
	"slices"
	"testing"
)

func TestParseAlarmStateMatchingChannel(t *testing.T) {
	t.Parallel()

	xmlText := `<?xml version="1.0" encoding="utf-8"?><body><AlarmEventList><AlarmEvent><channelId>0</channelId><status>none</status><AItype>none</AItype></AlarmEvent></AlarmEventList></body>`

	state, matched, err := parseAlarmState(xmlText, 0, false)
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

	state, matched, err := parseAlarmState(xmlText, 0, false)
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

	state, matched, err := parseAlarmState(xmlText, 0, false)
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

	state, matched, err := parseAlarmState(xmlText, 0, false)
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

	state, matched, err := parseAlarmState(xmlText, 0, false)
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

func TestParseAlarmStateSmartAIZoneEvent(t *testing.T) {
	t.Parallel()

	xmlText := `<?xml version="1.0" encoding="utf-8"?><body><AlarmEventList><AlarmEvent><channelId>0</channelId><status>none</status><AItype>none</AItype><smartAiTypeList><smartAiType><type>crossline</type><index>3</index><subList><index>0</index><type>people</type></subList><subList><index>1</index><type>vehicle</type></subList></smartAiType></smartAiTypeList></AlarmEvent></AlarmEventList></body>`

	state, matched, err := parseAlarmState(xmlText, 0, false)
	if err != nil {
		t.Fatalf("parseAlarmState() error = %v", err)
	}
	if !matched {
		t.Fatalf("parseAlarmState() matched = false, want true")
	}
	if !state.MotionDetected {
		t.Fatalf("parseAlarmState() motionDetected = false, want true for active smart zone")
	}
	if !slices.Equal(state.AITypes, []string{"people", "vehicle"}) {
		t.Fatalf("parseAlarmState() aiTypes = %v, want [people vehicle]", state.AITypes)
	}
}

func TestParseAlarmStateSmartAIClearedZones(t *testing.T) {
	t.Parallel()

	xmlText := `<?xml version="1.0" encoding="utf-8"?><body><AlarmEventList><AlarmEvent><channelId>0</channelId><status>none</status><AItype>none</AItype><smartAiTypeList><smartAiType><type>crossline</type><index>0</index></smartAiType></smartAiTypeList></AlarmEvent></AlarmEventList></body>`

	state, matched, err := parseAlarmState(xmlText, 0, false)
	if err != nil {
		t.Fatalf("parseAlarmState() error = %v", err)
	}
	if !matched {
		t.Fatalf("parseAlarmState() matched = false, want true")
	}
	if state.Active() {
		t.Fatalf("parseAlarmState() active = true, want false for cleared zones")
	}
}

func TestParseAlarmStateDualLensMergesStreamChannels(t *testing.T) {
	t.Parallel()

	xmlText := `<?xml version="1.0" encoding="utf-8"?><body><AlarmEventList><AlarmEvent><channelId>0</channelId><status>none</status><AItype>people</AItype></AlarmEvent><AlarmEvent><channelId>1</channelId><status>MD</status><AItype>people&amp;vehicle</AItype></AlarmEvent></AlarmEventList></body>`

	state, matched, err := parseAlarmState(xmlText, 0, true)
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

func TestLoginDeviceInfoDualLens(t *testing.T) {
	t.Parallel()

	dual := LoginDeviceInfo{Type: "camera", ChannelNum: 2, AnalogChnNum: 1}
	if !dual.IsDualLens() {
		t.Fatalf("IsDualLens() = false, want true for 2 stream / 1 analog")
	}
	single := LoginDeviceInfo{Type: "camera", ChannelNum: 1, AnalogChnNum: 1}
	if single.IsDualLens() {
		t.Fatalf("IsDualLens() = true, want false for 1/1")
	}
	noAnalog := LoginDeviceInfo{Type: "camera", ChannelNum: 2}
	if noAnalog.IsDualLens() {
		t.Fatalf("IsDualLens() = true, want false without analogChnNum")
	}
	nvr := LoginDeviceInfo{Type: "nvr", ChannelNum: 36, AnalogChnNum: 16}
	if nvr.IsDualLens() {
		t.Fatalf("IsDualLens() = true, want false for NVR")
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

// Battery doorbells report a lingering person as a zone entry without an AI
// type, and with the outer index left at zero. Skipping it dropped the event.
func TestParseAlarmStateZoneWithoutAITypeCountsAsMotion(t *testing.T) {
	t.Parallel()

	xmlText := `<?xml version="1.0" encoding="utf-8"?><body><AlarmEventList><AlarmEvent><channelId>0</channelId><status>none</status><AItype>none</AItype><smartAiTypeList><smartAiType><type>linger</type><index>0</index><subList><index>1</index></subList></smartAiType></smartAiTypeList></AlarmEvent></AlarmEventList></body>`

	state, matched, err := parseAlarmState(xmlText, 0, false)
	if err != nil {
		t.Fatalf("parseAlarmState() error = %v", err)
	}
	if !matched {
		t.Fatal("event was not matched")
	}
	if !state.MotionDetected {
		t.Fatal("a zone detection without an AI type must still report motion")
	}
	if !state.Active() {
		t.Fatal("state is not active")
	}
}

func TestPushListName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		xml  string
		want string
	}{
		{
			name: "day night event",
			xml:  "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n<body>\n<DayNightEventList version=\"1.1\">\n<DayNightEvent><channelId>0</channelId></DayNightEvent>\n</DayNightEventList>\n</body>",
			want: "DayNightEventList",
		},
		{
			name: "alarm event",
			xml:  `<?xml version="1.0" encoding="utf-8"?><body><AlarmEventList><AlarmEvent><channelId>3</channelId></AlarmEvent></AlarmEventList></body>`,
			want: "AlarmEventList",
		},
		{
			name: "empty payload",
			xml:  "",
			want: "unknown",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := pushListName(test.xml); got != test.want {
				t.Fatalf("pushListName() = %q, want %q", got, test.want)
			}
		})
	}
}

// Firmwares differ in the list element they wrap alarm events in. What counts
// is the AlarmEvent itself, not its parent, otherwise a camera reports
// detections that never reach the user.
func TestParseAlarmStateAcceptsAnyEventListName(t *testing.T) {
	xmlText := `<?xml version="1.0" encoding="UTF-8" ?>
<body>
<AlarmEventListV2>
<AlarmEvent>
<channelId>0</channelId>
<status>MD</status>
<AItype>people</AItype>
</AlarmEvent>
</AlarmEventListV2>
</body>`

	state, matched, err := parseAlarmState(xmlText, 0, false)
	if err != nil {
		t.Fatalf("parseAlarmState() error: %v", err)
	}
	if !matched {
		t.Fatal("event in an unknown list was dropped")
	}
	if !state.MotionDetected || !slices.Contains(state.AITypes, "people") {
		t.Fatalf("state = %+v, want motion with people", state)
	}
}

func TestParseAlarmReportsEventsForOtherChannels(t *testing.T) {
	xmlText := `<?xml version="1.0" encoding="UTF-8" ?>
<body>
<AlarmEventList>
<AlarmEvent>
<channelId>3</channelId>
<status>MD</status>
</AlarmEvent>
</AlarmEventList>
</body>`

	res, err := parseAlarm(xmlText, 0, false)
	if err != nil {
		t.Fatalf("parseAlarm() error: %v", err)
	}
	if res.matched {
		t.Fatal("an event for another channel must not match")
	}
	if !res.hasEvent {
		t.Fatal("hasEvent must report that the camera pushes alarms at all")
	}
	if !slices.Equal(res.channels, []uint8{3}) {
		t.Fatalf("channels = %v, want [3]", res.channels)
	}
}

func TestParseAlarmIgnoresDayNightPush(t *testing.T) {
	xmlText := `<?xml version="1.0" encoding="UTF-8" ?>
<body>
<DayNightEventList>
<DayNightEvent>
<channelId>0</channelId>
<status>day</status>
</DayNightEvent>
</DayNightEventList>
</body>`

	res, err := parseAlarm(xmlText, 0, false)
	if err != nil {
		t.Fatalf("parseAlarm() error: %v", err)
	}
	if res.hasEvent || res.matched {
		t.Fatalf("day/night push counted as an alarm: %+v", res)
	}
}

// A busy camera answers "not now", a camera without the command answers
// something else. Only the first deserves a retry.
func TestIsBusyStatus(t *testing.T) {
	if !isBusyStatus(&StatusError{MsgID: 31, Code: 400}) {
		t.Fatal("status 400 must count as busy")
	}
	if isBusyStatus(&StatusError{MsgID: 31, Code: 401}) {
		t.Fatal("status 401 must not count as busy")
	}
	if isBusyStatus(nil) {
		t.Fatal("no error must not count as busy")
	}
}

func TestParseYoloMapsClassesToAITypes(t *testing.T) {
	t.Parallel()

	xmlText := `<?xml version="1.0" encoding="UTF-8" ?>
<body>
<YoloWorldEventList>
<YoloWorldEvent>
<channel>0</channel>
<YoloWorldType><type>person</type></YoloWorldType>
<YoloWorldType><type>motor vehicle</type></YoloWorldType>
<YoloWorldType><type>non-motor vehicle</type></YoloWorldType>
<YoloWorldType><type>animal</type></YoloWorldType>
<YoloWorldType><type>package</type></YoloWorldType>
</YoloWorldEvent>
</YoloWorldEventList>
</body>`

	types, matched, err := parseYolo(xmlText, 0, false)
	if err != nil {
		t.Fatalf("parseYolo() error: %v", err)
	}
	if !matched {
		t.Fatal("parseYolo() did not match channel 0")
	}
	want := []string{"people", "vehicle", "non-motor vehicle", "dog_cat", "package"}
	if !slices.Equal(types, want) {
		t.Fatalf("aiTypes = %v, want %v", types, want)
	}
}

func TestParseYoloIgnoresOtherChannels(t *testing.T) {
	t.Parallel()

	xmlText := `<body><list><event><channel>2</channel><YoloWorldType><type>person</type></YoloWorldType></event></list></body>`

	types, matched, err := parseYolo(xmlText, 0, false)
	if err != nil {
		t.Fatalf("parseYolo() error: %v", err)
	}
	if matched || len(types) != 0 {
		t.Fatalf("an event for channel 2 matched channel 0: %v", types)
	}

	types, matched, err = parseYolo(xmlText, 0, true)
	if err != nil {
		t.Fatalf("parseYolo() error: %v", err)
	}
	if !matched || !slices.Equal(types, []string{"people"}) {
		t.Fatalf("a dual-lens camera must take every channel: %v", types)
	}
}

func TestParseYoloDetailWithSubTypes(t *testing.T) {
	t.Parallel()

	xmlText := `<body><list><event><channel>0</channel><YoloWorldType><type>motor vehicle</type><subTypeList><subType>sedan</subType></subTypeList><subTypeList><subType>pickup truck</subType></subTypeList></YoloWorldType></event></list></body>`

	types, _, err := parseYolo(xmlText, 0, false)
	if err != nil {
		t.Fatalf("parseYolo() error: %v", err)
	}
	if !slices.Equal(types, []string{"vehicle"}) {
		t.Fatalf("aiTypes = %v, want [vehicle]", types)
	}
}

func TestParseYoloSkipsEventsWithoutChannel(t *testing.T) {
	t.Parallel()

	types, matched, err := parseYolo(`<body><list><event><YoloWorldType><type>person</type></YoloWorldType></event></list></body>`, 0, false)
	if err != nil {
		t.Fatalf("parseYolo() error: %v", err)
	}
	if matched || len(types) != 0 {
		t.Fatalf("an event without channel counted: %v", types)
	}
}

func TestAlarmMergerBasicPushIsASnapshot(t *testing.T) {
	t.Parallel()

	var m AlarmMerger
	m.Alarm(AlarmState{MotionDetected: true})

	state, changed, err := m.Yolo(msgIDYoloAlarm, `<body><list><event><channel>0</channel><YoloWorldType><type>person</type></YoloWorldType></event></list></body>`, 0, false)
	if err != nil || !changed {
		t.Fatalf("Yolo() changed=%t err=%v, want a change", changed, err)
	}
	if !state.MotionDetected || !slices.Equal(state.AITypes, []string{"people"}) {
		t.Fatalf("merged state = %+v, want motion and people", state)
	}

	// a basic push that leaves our channel out ends its detections
	state, changed, err = m.Yolo(msgIDYoloAlarm, `<body><list><event><channel>1</channel><YoloWorldType><type>person</type></YoloWorldType></event></list></body>`, 0, false)
	if err != nil || !changed {
		t.Fatalf("Yolo() changed=%t err=%v, want a change", changed, err)
	}
	if !state.MotionDetected || len(state.AITypes) != 0 {
		t.Fatalf("merged state = %+v, want motion only", state)
	}
}

func TestAlarmMergerDetailedPushOnlyTouchesItsChannels(t *testing.T) {
	t.Parallel()

	var m AlarmMerger
	push := `<body><list><event><channel>0</channel><YoloWorldType><type>motor vehicle</type></YoloWorldType></event></list></body>`
	if _, changed, err := m.Yolo(msgIDYoloAlarmDetail, push, 0, false); err != nil || !changed {
		t.Fatalf("Yolo() changed=%t err=%v, want a change", changed, err)
	}
	if _, changed, _ := m.Yolo(msgIDYoloAlarmDetail, push, 0, false); changed {
		t.Fatal("the same push twice reported a change")
	}

	state, changed, err := m.Yolo(msgIDYoloAlarmDetail, `<body><list><event><channel>1</channel></event></list></body>`, 0, false)
	if err != nil || changed {
		t.Fatalf("a detailed push for channel 1 changed channel 0: changed=%t err=%v", changed, err)
	}
	if !slices.Equal(state.AITypes, []string{"vehicle"}) {
		t.Fatalf("aiTypes = %v, want [vehicle]", state.AITypes)
	}

	// a new alarm push keeps the yolo detections
	state = m.Alarm(AlarmState{Visitor: true})
	if !state.Visitor || !slices.Equal(state.AITypes, []string{"vehicle"}) {
		t.Fatalf("merged state = %+v, want visitor and vehicle", state)
	}

	m.Reset()
	if state = m.Alarm(AlarmState{}); state.Active() || state.Visitor {
		t.Fatalf("state after Reset = %+v, want nothing", state)
	}
}

func TestWithAITypesMergesWithoutDuplicates(t *testing.T) {
	t.Parallel()

	alarm := AlarmState{MotionDetected: true, AITypes: []string{"people"}}
	state := withAITypes(alarm, []string{"people", "non-motor vehicle"}, nil)

	if !state.MotionDetected {
		t.Fatal("motion got lost in the merge")
	}
	if !slices.Equal(state.AITypes, []string{"people", "non-motor vehicle"}) {
		t.Fatalf("aiTypes = %v, want [people non-motor vehicle]", state.AITypes)
	}
	if !slices.Equal(alarm.AITypes, []string{"people"}) {
		t.Fatalf("the merge changed the alarm state it started from: %v", alarm.AITypes)
	}
}
