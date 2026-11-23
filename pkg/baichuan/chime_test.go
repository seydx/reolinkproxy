package baichuan

import (
	"maps"
	"slices"
	"strings"
	"testing"
)

func TestParseChimes(t *testing.T) {
	t.Parallel()

	xmlText := `<?xml version="1.0" encoding="UTF-8" ?><body><dingdongList version="1.1">` +
		`<dingdongDeviceInfo><id>0</id><name>Flur</name><netState>2</netState><deviceType>chime</deviceType></dingdongDeviceInfo>` +
		`<dingdongDeviceInfo><id>3</id><name>Keller</name><netstate>0</netstate></dingdongDeviceInfo>` +
		`<dingdongDeviceInfo><id>7</id><name>Doorbell</name><netState>2</netState><deviceType>BatteryDB</deviceType></dingdongDeviceInfo>` +
		`</dingdongList></body>`

	chimes, err := parseChimes(xmlText)
	if err != nil {
		t.Fatal(err)
	}
	want := []Chime{{ID: 0, Name: "Flur", Online: true}, {ID: 3, Name: "Keller"}}
	if !slices.Equal(chimes, want) {
		t.Fatalf("chimes = %+v, want %+v", chimes, want)
	}
}

func TestParseChimeTones(t *testing.T) {
	t.Parallel()

	xmlText := `<?xml version="1.0" encoding="UTF-8" ?><body><dingdongCfg version="1.1">` +
		`<deviceCfg><id>0</id><version>1.0</version><alarminCfgList>` +
		`<alarminCfg><type>md</type><valid>1</valid><musicId>2</musicId></alarminCfg>` +
		`<alarminCfg><type>Visitor</type><valid>1</valid><musicId>0</musicId></alarminCfg>` +
		`</alarminCfgList></deviceCfg>` +
		`<deviceCfg><id>3</id><alarminCfg><type>visitor</type><valid>0</valid><musicId>5</musicId></alarminCfg></deviceCfg>` +
		`</dingdongCfg></body>`

	tones, err := parseChimeTones(xmlText, "visitor")
	if err != nil {
		t.Fatal(err)
	}
	if want := map[int]int{0: 0, 3: -1}; !maps.Equal(tones, want) {
		t.Fatalf("tones = %v, want %v", tones, want)
	}
}

func TestParseChimeParams(t *testing.T) {
	t.Parallel()

	params, err := parseChimeParams(`<?xml version="1.0" encoding="UTF-8" ?><body><dingdongDeviceOpt version="1.1"><id>0</id><name>Flur</name><volLevel>3</volLevel><ledState>1</ledState></dingdongDeviceOpt></body>`)
	if err != nil {
		t.Fatal(err)
	}
	if want := (ChimeParams{Name: "Flur", Volume: 3, LED: true}); params != want {
		t.Fatalf("params = %+v, want %+v", params, want)
	}
	if _, err := parseChimeParams(`<body><dingdongDeviceOpt><id>0</id></dingdongDeviceOpt></body>`); err == nil {
		t.Fatal("params without a volume must fail")
	}
}

func TestChimeBodiesKeepZeroValues(t *testing.T) {
	t.Parallel()

	ring, err := marshalXMLDocument(xmlDingDongOptBody[xmlDingDongRing]{
		Opt: xmlDingDongRing{Version: "1.1", ID: 0, Opt: "ringWithMusic", MusicID: 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	// tone 0 is a real tone, dropping the element would leave the chime silent
	if want := `<dingdongDeviceOpt version="1.1"><id>0</id><opt>ringWithMusic</opt><musicId>0</musicId></dingdongDeviceOpt>`; !strings.Contains(string(ring), want) {
		t.Fatalf("ring body = %s, want %s", ring, want)
	}

	set, err := marshalXMLDocument(xmlDingDongOptBody[xmlDingDongSetParam]{
		Opt: xmlDingDongSetParam{Version: "1.1", Opt: "setParam", ID: 2, VolLevel: 0, LedState: 0, Name: "Flur"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := `<dingdongDeviceOpt version="1.1"><opt>setParam</opt><id>2</id><volLevel>0</volLevel><ledState>0</ledState><name>Flur</name></dingdongDeviceOpt>`; !strings.Contains(string(set), want) {
		t.Fatalf("set body = %s, want %s", set, want)
	}
}
