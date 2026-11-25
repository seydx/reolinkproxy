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

func TestDecodeChannelCapabilitiesChimeAndTamper(t *testing.T) {
	t.Parallel()

	caps := decodeChannelCapabilities(xmlSupportItem{DoorbellVersion: 1, AIType: 1 << 25}, false)
	if !caps.Chime || !caps.Tamper {
		t.Fatalf("chime = %t, tamper = %t, want both", caps.Chime, caps.Tamper)
	}
	caps = decodeChannelCapabilities(xmlSupportItem{DoorbellVersion: 2, AIType: 1 << 24}, false)
	if caps.Chime || caps.Tamper {
		t.Fatalf("chime = %t, tamper = %t, want neither", caps.Chime, caps.Tamper)
	}
	if !decodeChannelCapabilities(xmlSupportItem{DoorbellVersion: 1 << 5}, false).Chime {
		t.Fatal("a chime that only listens must count")
	}
	if !decodeChannelCapabilities(xmlSupportItem{ChannelType: "Doorbell"}, false).Chime {
		t.Fatal("a doorbell behind an NVR without doorbellVersion must look for chimes")
	}
}
