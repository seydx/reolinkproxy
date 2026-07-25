package baichuan

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"net"
	"time"
)

// DiscoveredDevice is one Reolink device found by LAN discovery.
type DiscoveredDevice struct {
	IP    string
	MAC   string
	Name  string
	Ident string
	UID   string
}

const (
	discoveryPingPort     = 2000
	discoveryListenPort   = 3000
	discoveryReplyLen     = 388
	discoveryPingInterval = 500 * time.Millisecond
)

// discoveryPing is the 4-byte magic the cameras expect; replies echo it as a
// checksum at offset 104.
var discoveryPing = binary.BigEndian.AppendUint32(nil, 0xAAAA0000)

// Discover broadcasts the Reolink discovery ping (UDP port 2000) on every
// IPv4 interface and collects device replies (UDP port 3000) until ctx is
// done or timeout elapses. Only devices on the same L2 broadcast domain
// answer. Port 3000 must be free — the cameras reply to that fixed port.
func Discover(ctx context.Context, timeout time.Duration) ([]DiscoveredDevice, error) {
	listener, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: discoveryListenPort})
	if err != nil {
		return nil, err
	}
	defer listener.Close()

	sender, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		return nil, err
	}
	defer sender.Close()
	if err := enableBroadcast(sender); err != nil {
		return nil, err
	}

	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	deadline := time.Now().Add(timeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}

	sendPing := func() {
		for _, ip := range ipv4Broadcasts() {
			_, _ = sender.WriteToUDP(discoveryPing, &net.UDPAddr{IP: ip, Port: discoveryPingPort})
		}
	}
	sendPing()
	nextPing := time.Now().Add(discoveryPingInterval)

	var devices []DiscoveredDevice
	seen := make(map[string]struct{})
	buf := make([]byte, 2048)

	for {
		if err := ctx.Err(); err != nil {
			return devices, err
		}
		now := time.Now()
		if now.After(deadline) {
			return devices, nil
		}
		if now.After(nextPing) {
			sendPing()
			nextPing = now.Add(discoveryPingInterval)
		}

		readDeadline := nextPing
		if deadline.Before(readDeadline) {
			readDeadline = deadline
		}
		if err := listener.SetReadDeadline(readDeadline); err != nil {
			return devices, err
		}

		n, _, err := listener.ReadFromUDP(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return devices, err
		}

		device, ok := parseDiscoveryReply(buf[:n])
		if !ok {
			continue
		}

		key := device.MAC
		if key == "" {
			key = device.IP
		}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		devices = append(devices, device)
	}
}

// parseDiscoveryReply decodes a 388-byte discovery reply. Layout (offsets
// reverse-engineered, see ha_reolink_discovery):
//
//	 80+6  binary MAC
//	104+4  echo of the 0xAAAA0000 ping (checksum)
//	108+20 IP string, zero padded
//	132+32 device name string, zero padded
//	164+18 MAC string, zero padded
//	 58+18 identifier string (e.g. "IPC"), zero padded
//	228+32 UID string, zero padded
func parseDiscoveryReply(data []byte) (DiscoveredDevice, bool) {
	if len(data) != discoveryReplyLen {
		return DiscoveredDevice{}, false
	}
	if !bytes.Equal(data[104:108], discoveryPing) {
		return DiscoveredDevice{}, false
	}

	device := DiscoveredDevice{
		IP:    nullTermString(data, 108, 20),
		MAC:   nullTermString(data, 164, 18),
		Name:  nullTermString(data, 132, 32),
		Ident: nullTermString(data, 58, 18),
		UID:   nullTermString(data, 228, 32),
	}
	if device.MAC == "" {
		device.MAC = formatMAC(data[80:86])
	}
	if device.IP == "" {
		return DiscoveredDevice{}, false
	}
	return device, true
}

func nullTermString(data []byte, offset int, maxLen int) string {
	end := min(offset+maxLen, len(data))
	segment := data[offset:end]
	if idx := bytes.IndexByte(segment, 0); idx >= 0 {
		segment = segment[:idx]
	}
	return string(segment)
}

func formatMAC(raw []byte) string {
	if bytes.Equal(raw, make([]byte, len(raw))) {
		return ""
	}
	out := make([]byte, 0, len(raw)*3)
	for i, b := range raw {
		if i > 0 {
			out = append(out, ':')
		}
		out = hex.AppendEncode(out, []byte{b})
	}
	return string(out)
}
