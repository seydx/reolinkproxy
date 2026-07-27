package bridge

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/shareed2k/reolinkproxy/pkg/baichuan"
)

type cameraDevice struct {
	cameraName string
	cfg        baichuan.Config
	log        Logger
	// reconnectBackoff spaces reconnect attempts; battery cameras use a much
	// longer backoff so a sleeping camera is not woken over and over.
	reconnectBackoff time.Duration

	// onState, when set, is invoked with true after a successful login and
	// false when the connection is dropped. Write-once before use.
	onState func(connected bool)

	// webhookFailed latches after the camera rejected the webhook
	// subscription (unsupported firmware) so it is not retried on every reconnect.
	webhookFailed bool

	mu     sync.Mutex
	client *baichuan.Client
	// dualLens caches the last login's dual-lens flag so webhook pushes can
	// be parsed while the camera sleeps
	dualLens bool
	// audioSeen remembers per stream whether audio ever showed up, so a stream
	// that carries audio gets the patient startup window on every restart
	audioSeen map[baichuan.Stream]bool
}

// audioKnown reports the remembered audio verdict for a stream, and whether
// one exists yet.
func (m *cameraDevice) audioKnown(stream baichuan.Stream) (bool, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen, ok := m.audioSeen[stream]
	return seen, ok
}

func (m *cameraDevice) setAudioKnown(stream baichuan.Stream, seen bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.audioSeen == nil {
		m.audioSeen = make(map[baichuan.Stream]bool)
	}
	m.audioSeen[stream] = seen
}

func newCameraDevice(cameraName string, cfg baichuan.Config, reconnectBackoff time.Duration, log Logger) *cameraDevice {
	if reconnectBackoff <= 0 {
		reconnectBackoff = 2 * time.Second
	}
	return &cameraDevice{
		cameraName:       cameraName,
		cfg:              cfg,
		reconnectBackoff: reconnectBackoff,
		log:              log,
	}
}

func (m *cameraDevice) Ensure(ctx context.Context) (*baichuan.Client, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.client != nil {
		if err := m.client.Err(); err == nil {
			return m.client, nil
		}
		m.closeLocked("")
	}

	client, err := baichuan.Dial(ctx, m.cfg)
	if err != nil {
		return nil, err
	}
	if err := client.Login(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}

	m.client = client
	m.dualLens = client.LoginDeviceInfo().IsDualLens()
	if m.onState != nil {
		go m.onState(true)
	}
	return client, nil
}

func (m *cameraDevice) DualLens() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dualLens
}

func (m *cameraDevice) WithClient(ctx context.Context, fn func(*baichuan.Client) error) error {
	client, err := m.Ensure(ctx)
	if err != nil {
		return err
	}

	err = fn(client)
	if err != nil {
		if closeErr := client.Err(); closeErr != nil {
			m.ResetIfCurrent(client, fmt.Sprintf("client closed: %v", closeErr))
		}
	}
	return err
}

func (m *cameraDevice) ResetIfCurrent(client *baichuan.Client, reason string) {
	if client == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.client != client {
		return
	}
	m.closeLocked(reason)
}

func (m *cameraDevice) Close(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeLocked(reason)
}

func (m *cameraDevice) closeLocked(reason string) {
	if m.client == nil {
		return
	}
	if reason != "" {
		m.log.Infof("camera %s reconnecting: %s", m.cameraName, reason)
	}
	_ = m.client.Close()
	m.client = nil
	if m.onState != nil {
		go m.onState(false)
	}
}

// StreamPackets pulls preview media for one channel/stream, reconnecting on
// failures. wantStream gates the preview session: while it returns false, the
// current session is torn down and no reconnect is attempted (idle
// disconnect); pass nil to always stream.
func (m *cameraDevice) StreamPackets(ctx context.Context, channel uint8, stream baichuan.Stream, wantStream func() bool) <-chan baichuan.MediaPacket {
	out := make(chan baichuan.MediaPacket, 50)

	go func() {
		defer close(out)

		for ctx.Err() == nil {
			if wantStream != nil && !wantStream() {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
				continue
			}

			client, err := m.Ensure(ctx)
			if err != nil {
				select {
				case <-ctx.Done():
					return
				case <-time.After(m.reconnectBackoff):
				}
				continue
			}

			reader, err := client.StartPreview(ctx, channel, stream)
			if err != nil {
				// A status error means the connection is fine but this stream
				// is rejected (e.g. profile not supported by the model). The
				// client is shared with the other streams and the event
				// listener, so don't reset it — just retry this stream slowly.
				var statusErr *baichuan.StatusError
				if errors.As(err, &statusErr) {
					m.log.Warnf("stream %s channel %d rejected by camera (%v), retrying in 5m", m.cameraName, channel, err)
					select {
					case <-ctx.Done():
						return
					case <-time.After(5 * time.Minute):
					}
					continue
				}

				m.ResetIfCurrent(client, fmt.Sprintf("start preview failed: %v", err))
				select {
				case <-ctx.Done():
					return
				case <-time.After(m.reconnectBackoff):
				}
				continue
			}

			if done := m.pumpPreview(ctx, client, channel, stream, reader, out, wantStream); done {
				return
			}

			// A media desync only affects this stream — keep the shared client
			// (event listener, other streams), stop the camera-side session
			// and restart the preview with a fresh parser.
			if reader.Err() != nil {
				_ = client.StopPreview(ctx, channel, stream)
			} else if wantStream == nil || wantStream() {
				m.ResetIfCurrent(client, "preview stream ended")
			}
			time.Sleep(100 * time.Millisecond) // brief wait before reconnect
		}
	}()

	return out
}

// pumpPreview forwards media packets from one preview session until it ends.
// done=true means the caller's context is finished; false asks for a session
// restart.
func (m *cameraDevice) pumpPreview(
	ctx context.Context,
	client *baichuan.Client,
	channel uint8,
	stream baichuan.Stream,
	reader *baichuan.MediaReader,
	out chan<- baichuan.MediaPacket,
	wantStream func() bool,
) bool {
	stallTimer := time.NewTimer(15 * time.Second)
	defer stallTimer.Stop()
	idleTicker := time.NewTicker(time.Second)
	defer idleTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return true
		case packet, ok := <-reader.Packets:
			if !ok {
				if err := reader.Err(); err != nil {
					m.log.Warnf("stream %s channel %d media desync, restarting preview: %v", m.cameraName, channel, err)
				}
				return false
			}
			select {
			case <-ctx.Done():
				return true
			case out <- packet:
			}
			if !stallTimer.Stop() {
				select {
				case <-stallTimer.C:
				default:
				}
			}
			stallTimer.Reset(15 * time.Second)
		case <-stallTimer.C:
			m.log.Warnf("stream %s channel %d stalled for 15s, reconnecting", m.cameraName, channel)
			m.ResetIfCurrent(client, "stream stalled for 15s")
			return false
		case <-idleTicker.C:
			if wantStream != nil && !wantStream() {
				m.log.Debugf("stream %s channel %d idle, stopping preview", m.cameraName, channel)
				if err := client.StopPreview(ctx, channel, stream); err != nil {
					m.ResetIfCurrent(client, fmt.Sprintf("idle preview stop failed: %v", err))
				}
				return false
			}
		}
	}
}

// eventHandlers receives decoded camera events from WatchEvents. Nil fields
// are skipped.
type eventHandlers struct {
	alarm             func(baichuan.AlarmState)
	battery           func(baichuan.BatteryInfo)
	sleep             func(sleeping bool)
	floodlight        func(on bool)
	motionUnsupported func()
	// pollBattery queries the battery state once per (re)connect so the
	// consumer has a value before the first spontaneous push.
	pollBattery bool
	// webhookURL, when set, is registered with the camera on every
	// (re)connect so it pushes events itself (battery cameras).
	webhookURL string
}

// WatchEvents establishes a persistent event listener session (motion/AI
// alarms + battery pushes), automatically reconnecting on failures and
// re-subscribing every 5 minutes.
func (m *cameraDevice) WatchEvents(ctx context.Context, channel uint8, handlers eventHandlers) {
	go func() {
		motionUnsupported := false

		for {
			if ctx.Err() != nil {
				return
			}

			client, err := m.Ensure(ctx)
			if err != nil {
				m.log.Warnf("events: camera connect error for %s: %v. retrying in %v...", m.cameraName, err, m.reconnectBackoff)
				select {
				case <-ctx.Done():
					return
				case <-time.After(m.reconnectBackoff):
				}
				continue
			}

			cancelAlarms, alarmsOK := m.setupAlarmListener(ctx, client, channel, handlers, &motionUnsupported)
			if !alarmsOK {
				select {
				case <-ctx.Done():
					return
				case <-time.After(m.reconnectBackoff):
				}
				continue
			}

			cancelBattery := func() {}
			if handlers.battery != nil {
				cancelBattery = client.ListenForBattery(ctx, channel, handlers.battery)
			}

			cancelSleep := func() {}
			if handlers.sleep != nil {
				cancelSleep = client.ListenForSleep(ctx, channel, handlers.sleep)
			}

			cancelFloodlight := func() {}
			if handlers.floodlight != nil {
				cancelFloodlight = client.ListenForFloodlight(ctx, channel, handlers.floodlight)
			}

			m.setupWebhookAndBatteryPoll(ctx, client, channel, handlers)

			if motionUnsupported && handlers.battery == nil {
				return
			}

			cancelAll := func() {
				cancelAlarms()
				cancelBattery()
				cancelSleep()
				cancelFloodlight()
			}

			select {
			case <-ctx.Done():
				cancelAll()
				return
			case <-client.Done():
				cancelAll()
				if err := client.Err(); err != nil && ctx.Err() == nil {
					m.ResetIfCurrent(client, fmt.Sprintf("event listener disconnected: %v", err))
					m.log.Warnf("events: listener disconnected for %s: %v. reconnecting...", m.cameraName, err)
				}
			case <-time.After(5 * time.Minute):
				cancelAll()
			}
		}
	}()
}

// setupAlarmListener subscribes the alarm listener. ok=false signals a
// transient error that warrants a reconnect; a permanently unsupported motion
// ability flips motionUnsupported instead.
func (m *cameraDevice) setupAlarmListener(ctx context.Context, client *baichuan.Client, channel uint8, handlers eventHandlers, motionUnsupported *bool) (cancel func(), ok bool) {
	if handlers.alarm == nil || *motionUnsupported {
		return func() {}, true
	}

	m.log.Debugf("events: establishing alarm listener for %s...", m.cameraName)
	cancelAlarms, err := client.ListenForAlarms(ctx, channel, handlers.alarm)
	if err == nil {
		return cancelAlarms, true
	}

	var missingAbility *baichuan.MissingAbilityError
	var statusErr *baichuan.StatusError
	if (errors.As(err, &missingAbility) && missingAbility.Name == "motion") ||
		(errors.As(err, &statusErr) && statusErr.MsgID == 31 && statusErr.Code == 400) {
		m.log.Warnf("events: alarm listener unsupported for %s: %v", m.cameraName, err)
		*motionUnsupported = true
		if handlers.motionUnsupported != nil {
			handlers.motionUnsupported()
		}
		return func() {}, true
	}

	m.ResetIfCurrent(client, fmt.Sprintf("alarm listener error: %v", err))
	m.log.Warnf("events: alarm listener error for %s: %v. retrying in %v...", m.cameraName, err, m.reconnectBackoff)
	return nil, false
}

// setupWebhookAndBatteryPoll registers the event webhook (battery cameras)
// and kicks off the initial battery query for the current connection.
func (m *cameraDevice) setupWebhookAndBatteryPoll(ctx context.Context, client *baichuan.Client, channel uint8, handlers eventHandlers) {
	if handlers.webhookURL != "" && !m.webhookFailed {
		if err := m.subscribeWebhook(ctx, client, handlers.webhookURL); err != nil {
			// Unsupported firmware answers with a status error — log once and
			// stop retrying; events keep flowing over TCP.
			var statusErr *baichuan.StatusError
			if errors.As(err, &statusErr) {
				m.log.Warnf("events: camera %s does not support webhooks, battery events rely on the TCP listener: %v", m.cameraName, err)
				m.webhookFailed = true
			} else {
				m.log.Warnf("events: webhook subscription failed for %s: %v", m.cameraName, err)
			}
		}
	}

	if handlers.pollBattery && handlers.battery != nil {
		go func() {
			pollCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			if info, err := client.GetBattery(pollCtx, channel); err == nil {
				handlers.battery(*info)
			}
		}()
	}
}

// subscribeWebhook enables the camera's event push webhook, verifying the
// registered URL like reolink_aio does.
func (m *cameraDevice) subscribeWebhook(ctx context.Context, client *baichuan.Client, url string) error {
	if _, _, err := client.GetWebhook(ctx); err != nil {
		return err
	}
	if err := client.SetWebhook(ctx, true, url); err != nil {
		return err
	}

	enabled, gotURL, err := client.GetWebhook(ctx)
	if err != nil {
		return err
	}
	if !enabled || gotURL != url {
		return fmt.Errorf("webhook verification failed: enabled=%t url=%q", enabled, gotURL)
	}
	m.log.Infof("events: camera %s pushes events to %s", m.cameraName, url)
	return nil
}

type resilientTalkSession struct {
	device     *cameraDevice
	channel    uint8
	mu         sync.Mutex
	session    *baichuan.TalkSession
	sampleRate int
	samplesPB  int
	bytesPB    int
	closed     bool
}

func (s *resilientTalkSession) SampleRate() int {
	return s.sampleRate
}

func (s *resilientTalkSession) SamplesPerBlock() int {
	return s.samplesPB
}

func (s *resilientTalkSession) BytesPerBlock() int {
	return s.bytesPB
}

func (s *resilientTalkSession) WriteADPCMBlock(ctx context.Context, block []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errors.New("talk session closed")
	}

	if s.session != nil {
		err := s.session.WriteADPCMBlock(ctx, block)
		if err == nil {
			return nil
		}
		// Failure, clear session and fall through to reconnect
		client, _ := s.device.Ensure(ctx)
		s.device.ResetIfCurrent(client, fmt.Sprintf("talk write error: %v", err))
		s.session = nil
	}

	// Try to reconnect once
	client, err := s.device.Ensure(ctx)
	if err != nil {
		return nil // Drop audio quietly while reconnecting
	}

	newSession, err := client.StartTalk(ctx, s.channel)
	if err != nil {
		s.device.ResetIfCurrent(client, fmt.Sprintf("talk restart error: %v", err))
		return nil // Drop audio quietly
	}

	s.session = newSession
	// Discard error on fresh write; if it fails, it'll retry next time
	_ = s.session.WriteADPCMBlock(ctx, block)
	return nil
}

func (s *resilientTalkSession) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.session != nil {
		return s.session.Close(ctx)
	}
	return nil
}

func (m *cameraDevice) StartTalk(ctx context.Context, channel uint8) (*resilientTalkSession, error) {
	client, err := m.Ensure(ctx)
	if err != nil {
		return nil, err
	}

	session, err := client.StartTalk(ctx, channel)
	if err != nil {
		m.ResetIfCurrent(client, fmt.Sprintf("initial talk start error: %v", err))
		return nil, err
	}

	return &resilientTalkSession{
		device:     m,
		channel:    channel,
		session:    session,
		sampleRate: session.SampleRate(),
		samplesPB:  session.SamplesPerBlock(),
		bytesPB:    session.BytesPerBlock(),
	}, nil
}
