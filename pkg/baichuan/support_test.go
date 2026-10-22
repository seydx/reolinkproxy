package baichuan

import "testing"

func TestDecodeChannelCapabilitiesDoorbellByChannelType(t *testing.T) {
	t.Parallel()

	if !decodeChannelCapabilities(xmlSupportItem{ChannelType: "Doorbell"}, false).Doorbell {
		t.Fatal("a Doorbell channel type without doorbellVersion must count as doorbell")
	}
	if decodeChannelCapabilities(xmlSupportItem{ChannelType: "ipc"}, false).Doorbell {
		t.Fatal("an ipc channel must not count as doorbell")
	}
}
