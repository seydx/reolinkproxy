package baichuan

import "testing"

func TestParsePrivacyStates(t *testing.T) {
	t.Parallel()

	single := "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n<body>\n<sleepStatus version=\"1.1\">\n<sleep>1</sleep>\n</sleepStatus>\n</body>"
	if got := parsePrivacyStates(single); len(got) != 1 || !got[0] {
		t.Fatalf("single camera: got %v, want map[0:true]", got)
	}

	recorder := `<body><sleepStatus version="1.1"><status><channelId>0</channelId><sleep>0</sleep></status><status><channelId>2</channelId><sleep>1</sleep></status></sleepStatus></body>`
	got := parsePrivacyStates(recorder)
	if len(got) != 2 || got[0] || !got[2] {
		t.Fatalf("recorder: got %v, want map[0:false 2:true]", got)
	}
}
