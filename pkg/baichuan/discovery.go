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
	// cameras on WiFi power save ignore broadcasts for ten seconds or more
	// before they start answering, so presence is watched continuously
	// instead of scanned on demand
	discoveryWatchInterval = 15 * time.Second
	discoveryBurstPings    = 3
	// dense pings right after start: a sleeping camera answered after ~11s of
	// them, and that is when the UI is most likely waiting for its first list
	discoveryWarmup = 20 * time.Second
)

// discoveryPing is the 4-byte magic the cameras expect; replies echo it as a
// checksum at offset 104.
var discoveryPing = binary.BigEndian.AppendUint32(nil, 0xAAAA0000)

// Discover broadcasts the Reolink discovery ping (UDP port 2000) on every
// IPv4 interface and collects device replies (UDP port 3000) until ctx is
// done or timeout elapses. Only devices on the same L2 broadcast domain
// answer. Port 3000 must be free — the cameras reply to that fixed port.
func Discover(ctx context.Context, timeout time.Duration) ([]DiscoveredDevice, error) {
	listener, sender, err := openDiscoverySockets()
	if err != nil {
		return nil, err
	}
	defer listener.Close()
	defer sender.Close()

	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	deadline := time.Now().Add(timeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}

	sendDiscoveryPing(sender)
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
			sendDiscoveryPing(sender)
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

// Watcher keeps the discovery socket open and pings on an interval, so a
// camera that starts answering late is already known by the time the UI asks
// for a device list. Only one watcher can run per host: the cameras reply to
// the fixed port 3000.
type Watcher struct {
	onDevice func(DiscoveredDevice)
	ping     chan struct{}
}

// NewWatcher returns a watcher that reports every discovery reply to onDevice,
// including repeats — the caller decides what a repeated sighting means.
func NewWatcher(onDevice func(DiscoveredDevice)) *Watcher {
	return &Watcher{onDevice: onDevice, ping: make(chan struct{}, 1)}
}

// Ping triggers a broadcast burst without waiting for the next interval.
// Never blocks: a burst already queued absorbs the request.
func (w *Watcher) Ping() {
	select {
	case w.ping <- struct{}{}:
	default:
	}
}

// Run watches for devices until ctx is done. It returns nil on cancellation
// and an error when the sockets cannot be opened or a read fails, so the
// caller can retry.
func (w *Watcher) Run(ctx context.Context) error {
	listener, sender, err := openDiscoverySockets()
	if err != nil {
		return err
	}
	defer listener.Close()
	defer sender.Close()

	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	go w.pingLoop(ctx, sender)

	buf := make([]byte, 2048)
	for {
		n, _, err := listener.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if device, ok := parseDiscoveryReply(buf[:n]); ok {
			w.onDevice(device)
		}
	}
}

func (w *Watcher) pingLoop(ctx context.Context, sender *net.UDPConn) {
	warmUntil := time.Now().Add(discoveryWarmup)

	for {
		wait := discoveryWatchInterval
		if time.Now().Before(warmUntil) {
			sendDiscoveryPing(sender)
			wait = discoveryPingInterval
		} else {
			w.burst(ctx, sender)
		}
		if !w.waitTick(ctx, wait) {
			return
		}
	}
}

// waitTick blocks until d elapses or a manual ping is requested. It reports
// whether the loop should continue.
func (w *Watcher) waitTick(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
	case <-w.ping:
	}
	return true
}

// burst sends several pings in a row. One broadcast is easily lost, and a
// sleeping camera answers only once it has seen a few.
func (w *Watcher) burst(ctx context.Context, sender *net.UDPConn) {
	for i := range discoveryBurstPings {
		sendDiscoveryPing(sender)
		if i == discoveryBurstPings-1 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(discoveryPingInterval):
		}
	}
}

func openDiscoverySockets() (listener *net.UDPConn, sender *net.UDPConn, err error) {
	listener, err = net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: discoveryListenPort})
	if err != nil {
		return nil, nil, err
	}

	sender, err = net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		listener.Close()
		return nil, nil, err
	}
	if err := enableBroadcast(sender); err != nil {
		listener.Close()
		sender.Close()
		return nil, nil, err
	}
	return listener, sender, nil
}

func sendDiscoveryPing(sender *net.UDPConn) {
	for _, ip := range ipv4Broadcasts() {
		_, _ = sender.WriteToUDP(discoveryPing, &net.UDPAddr{IP: ip, Port: discoveryPingPort})
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
