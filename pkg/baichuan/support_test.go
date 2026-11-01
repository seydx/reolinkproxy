package baichuan

import (
	"slices"
	"testing"
)

func TestDecodeChannelCapabilitiesDoorbellByChannelType(t *testing.T) {
	t.Parallel()

	if !decodeChannelCapabilities(xmlSupportItem{ChannelType: "Doorbell"}, false).Doorbell {
		t.Fatal("a Doorbell channel type without doorbellVersion must count as doorbell")
	}
	if decodeChannelCapabilities(xmlSupportItem{ChannelType: "ipc"}, false).Doorbell {
		t.Fatal("an ipc channel must not count as doorbell")
	}
}

func TestDecodeChannelCapabilitiesYoloWorld(t *testing.T) {
	t.Parallel()

	caps := decodeChannelCapabilities(xmlSupportItem{AIType: 1<<1 | 1<<17 | 1<<23}, false)
	want := []string{"people", "package", "non-motor vehicle"}
	if !slices.Equal(caps.AITypes, want) {
		t.Fatalf("aiTypes = %v, want %v", caps.AITypes, want)
	}
}
