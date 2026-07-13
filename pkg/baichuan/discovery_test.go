package baichuan

import "testing"

func buildDiscoveryReply(t *testing.T) []byte {
	t.Helper()

	data := make([]byte, discoveryReplyLen)
	copy(data[104:108], discoveryPing)
	copy(data[80:86], []byte{0xec, 0x71, 0xdb, 0x01, 0x02, 0x03})
	copy(data[108:], "192.168.1.42\x00")
	copy(data[132:], "Front Door\x00")
	copy(data[164:], "ec:71:db:01:02:03\x00")
	copy(data[58:], "IPC\x00")
	copy(data[228:], "95270000ABCDEF\x00")
	return data
}

func TestParseDiscoveryReply(t *testing.T) {
	t.Parallel()

	device, ok := parseDiscoveryReply(buildDiscoveryReply(t))
	if !ok {
		t.Fatal("parseDiscoveryReply() ok = false, want true")
	}
	if device.IP != "192.168.1.42" {
		t.Fatalf("IP = %q, want 192.168.1.42", device.IP)
	}
	if device.Name != "Front Door" {
		t.Fatalf("Name = %q, want Front Door", device.Name)
	}
	if device.MAC != "ec:71:db:01:02:03" {
		t.Fatalf("MAC = %q", device.MAC)
	}
	if device.UID != "95270000ABCDEF" {
		t.Fatalf("UID = %q", device.UID)
	}
	if device.Ident != "IPC" {
		t.Fatalf("Ident = %q", device.Ident)
	}
}

func TestParseDiscoveryReplyFallsBackToBinaryMAC(t *testing.T) {
	t.Parallel()

	data := buildDiscoveryReply(t)
	copy(data[164:182], make([]byte, 18))

	device, ok := parseDiscoveryReply(data)
	if !ok {
		t.Fatal("parseDiscoveryReply() ok = false, want true")
	}
	if device.MAC != "ec:71:db:01:02:03" {
		t.Fatalf("MAC = %q, want binary fallback ec:71:db:01:02:03", device.MAC)
	}
}

func TestParseDiscoveryReplyRejectsBadChecksum(t *testing.T) {
	t.Parallel()

	data := buildDiscoveryReply(t)
	data[104] = 0x00

	if _, ok := parseDiscoveryReply(data); ok {
		t.Fatal("parseDiscoveryReply() ok = true, want false")
	}
}

func TestParseDiscoveryReplyRejectsWrongLength(t *testing.T) {
	t.Parallel()

	if _, ok := parseDiscoveryReply(make([]byte, 100)); ok {
		t.Fatal("parseDiscoveryReply() ok = true, want false")
	}
}
