// Package bridge exposes Reolink cameras (Baichuan protocol) as local RTSP
// streams with two-way audio, without video transcoding. Cameras can be added
// and removed at runtime; consumers point any RTSP client (ffmpeg, go2rtc,
// VLC) at the returned URLs.
package bridge

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gortsplib "github.com/bluenviron/gortsplib/v5"

	"github.com/shareed2k/reolinkproxy/pkg/baichuan"
)

// Options configures the shared RTSP server and media pacing.
type Options struct {
	// RTSPAddress is the RTSP listen address (default "127.0.0.1:8554").
	RTSPAddress string
	// RTPAddress / RTCPAddress enable RTSP-over-UDP transport when both are
	// set (e.g. ":8000" / ":8001"). Empty disables UDP; TCP always works.
	RTPAddress  string
	RTCPAddress string
	// EnableRTCPSenderReports re-enables periodic RTCP SRs. Off by default:
	// some receivers (e.g. FFmpeg) re-anchor decode time on each SR, causing
	// non-monotonic DTS warnings.
	EnableRTCPSenderReports bool
	// WebhookAddress, when set (e.g. ":8557"), enables the HTTP listener that
	// battery cameras push events to instead of a persistent TCP subscription
	// that would keep them awake. Must be reachable from the camera network.
	WebhookAddress string
	// LogPackets enables verbose per-packet logging.
	LogPackets bool
	// Logger receives bridge logs. Defaults to NopLogger.
	Logger Logger
}

func (o *Options) applyDefaults() {
	if o.RTSPAddress == "" {
		o.RTSPAddress = "127.0.0.1:8554"
	}
	if o.Logger == nil {
		o.Logger = NopLogger{}
	}
}

// Bridge hosts one RTSP server and any number of Reolink cameras.
type Bridge struct {
	opts    Options
	log     Logger
	handler *rtspServerHandler
	server  *gortsplib.Server
	webhook *webhookServer

	mu      sync.Mutex
	cameras map[string]*Camera
	started bool
	closed  bool
}

// New creates a Bridge. Call Start before adding cameras.
func New(opts Options) *Bridge {
	opts.applyDefaults()
	b := &Bridge{
		opts:    opts,
		log:     opts.Logger,
		handler: newRTSPServerHandler(opts.Logger),
		cameras: make(map[string]*Camera),
	}
	if opts.WebhookAddress != "" {
		b.webhook = newWebhookServer(opts.WebhookAddress, opts.Logger)
	}
	return b
}

// Start launches the RTSP server.
func (b *Bridge) Start() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return fmt.Errorf("bridge is closed")
	}
	if b.started {
		return nil
	}

	server := &gortsplib.Server{
		Handler:                  b.handler,
		RTSPAddress:              b.opts.RTSPAddress,
		UDPRTPAddress:            b.opts.RTPAddress,
		UDPRTCPAddress:           b.opts.RTCPAddress,
		DisableRTCPSenderReports: !b.opts.EnableRTCPSenderReports,
		WriteQueueSize:           4096,
	}
	b.handler.server = server

	if err := server.Start(); err != nil {
		return fmt.Errorf("start rtsp server: %w", err)
	}
	b.server = server
	b.started = true
	b.log.Infof("rtsp server listening at %s", b.opts.RTSPAddress)
	return nil
}

// Close removes all cameras and stops the RTSP server.
func (b *Bridge) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	cameras := make([]*Camera, 0, len(b.cameras))
	for _, cam := range b.cameras {
		cameras = append(cameras, cam)
	}
	b.cameras = make(map[string]*Camera)
	server := b.server
	b.mu.Unlock()

	for _, cam := range cameras {
		cam.stop()
	}
	if server != nil {
		server.Close()
	}
	if b.webhook != nil {
		b.webhook.close()
	}
}

// AddCamera registers a camera and starts its stream/talk pipelines. It does
// not wait for the camera to come online — connection retries run in the
// background, so an offline camera is added successfully and starts streaming
// once reachable.
func (b *Bridge) AddCamera(cfg CameraConfig) (*Camera, error) {
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil, fmt.Errorf("bridge is closed")
	}
	if !b.started {
		return nil, fmt.Errorf("bridge is not started")
	}
	if _, exists := b.cameras[cfg.Name]; exists {
		return nil, fmt.Errorf("camera %q already exists", cfg.Name)
	}

	ctx, cancel := context.WithCancel(context.Background())

	reconnectBackoff := 2 * time.Second
	if cfg.BatteryCamera {
		reconnectBackoff = 30 * time.Second
	}
	camLog := b.log
	if cfg.Logger != nil {
		camLog = cfg.Logger
	}
	device := newCameraDevice(cfg.Name, baichuan.Config{
		Host:     cfg.Host,
		Port:     cfg.Port,
		UID:      cfg.UID,
		Username: cfg.Username,
		Password: cfg.Password,
		Timeout:  cfg.Timeout,
	}, reconnectBackoff, camLog)

	talkPath := talkPathForCamera(cfg.RTSPPath)
	talkPublisher := newRTSPTalkPublisher(
		talkPath,
		cfg.Name,
		uint8(cfg.Channel), //#nosec G115
		device,
		cfg.TalkVolume,
		camLog,
	)
	b.handler.addTalk(talkPath, talkPublisher)

	var motionState *cameraMotionState
	if cfg.PauseOnMotion {
		motionState = newCameraMotionState()
	}

	cam := &Camera{
		bridge: b,
		cfg:    cfg,
		device: device,
		motion: motionState,
		cancel: cancel,
	}
	cam.liveCatchUp.Store(int64(*cfg.LiveCatchUp))

	// Battery cameras get a webhook so events arrive without a persistent
	// TCP subscription keeping them awake; the TCP listener stays as backup.
	webhookURL := ""
	if cfg.BatteryCamera && b.webhook != nil {
		if err := b.webhook.ensureStarted(); err != nil {
			b.log.Warnf("camera %s: webhook server unavailable, battery events rely on the TCP listener: %v", cfg.Name, err)
		} else {
			b.webhook.register(cfg.Name, cam)
			webhookURL = b.webhookURL(cfg.Name)
		}
	}

	// Wire event dispatch before anything can connect, so the first login
	// already reports its connection state.
	device.onState = cam.handleConnection
	device.WatchEvents(ctx, uint8(cfg.Channel), eventHandlers{ //#nosec G115
		alarm:      cam.handleAlarm,
		battery:    cam.handleBattery,
		sleep:      cam.handleSleep,
		floodlight: cam.handleFloodlight,
		motionUnsupported: func() {
			if motionState != nil {
				motionState.markUnsupported()
			}
		},
		pollBattery: cfg.BatteryCamera,
		webhookURL:  webhookURL,
	})

	metas, streamPaths := b.setupCameraStreams(ctx, cam, cfg, device, talkPublisher, motionState)
	cam.streams = metas
	cam.paths = make([]string, 0, 1+len(streamPaths))
	cam.paths = append(cam.paths, talkPath)
	cam.paths = append(cam.paths, streamPaths...)

	b.cameras[cfg.Name] = cam
	b.log.Infof("camera %s added (%d stream path(s))", cfg.Name, len(cam.paths))
	return cam, nil
}

// RemoveCamera stops a camera's pipelines and unregisters its RTSP paths.
func (b *Bridge) RemoveCamera(name string) error {
	b.mu.Lock()
	cam, ok := b.cameras[name]
	if ok {
		delete(b.cameras, name)
	}
	b.mu.Unlock()

	if !ok {
		return fmt.Errorf("camera %q not found", name)
	}
	cam.stop()
	b.log.Infof("camera %s removed", name)
	return nil
}

// Camera returns a registered camera by name.
func (b *Bridge) Camera(name string) (*Camera, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	cam, ok := b.cameras[name]
	return cam, ok
}

// setupCameraStreams registers the RTSP paths for every configured stream
// profile (plus two-way variants and talk-profile aliases) and starts one
// runStream pipeline per profile. It returns the stream metadata and every
// registered path for later cleanup.
func (b *Bridge) setupCameraStreams(
	ctx context.Context,
	cam *Camera,
	cfg CameraConfig,
	device *cameraDevice,
	talkPublisher *rtspTalkPublisher,
	motionState *cameraMotionState,
) ([]*streamMetadata, []string) {
	preferredTalkProfile := cfg.preferredTalkProfile()
	basePath := strings.TrimPrefix(cfg.RTSPPath, "/")
	multi := len(cfg.Streams) > 1

	var (
		metas                  []*streamMetadata
		paths                  []string
		preferredMeta          *streamMetadata
		preferredHandler       *rtspStreamHandler
		preferredTwoWayHandler *rtspStreamHandler
	)

	for _, s := range cfg.Streams {
		path := basePath
		if multi {
			path = basePath + "_" + s
		}

		metaPath := path
		isPreferred := multi && preferredTalkProfile != "" && s == preferredTalkProfile
		if isPreferred {
			metaPath = basePath
		}

		meta := &streamMetadata{
			cameraName: cfg.Name,
			name:       s,
			path:       metaPath,
		}
		if isPreferred {
			preferredMeta = meta
		} else {
			metas = append(metas, meta)
		}

		streamHandler := newRTSPStreamHandler(path)
		streamHandler.attachServer(b.handler.server)
		b.handler.addStream(path, streamHandler)
		paths = append(paths, path)

		twoWayPath := twoWayPathForStream(path)
		twoWayHandler := newRTSPStreamHandler(twoWayPath)
		twoWayHandler.attachServer(b.handler.server)
		twoWayHandler.setExtraMedias(newBackChannelMedia())
		streamHandler.addMirror(twoWayHandler)
		b.handler.addStream(twoWayPath, twoWayHandler)
		b.handler.addTalkAlias(twoWayPath, talkPublisher)
		paths = append(paths, twoWayPath)

		if isPreferred {
			preferredHandler = streamHandler
			preferredTwoWayHandler = twoWayHandler
		}

		b.log.Debugf("stream registered camera=%s stream=%s path=%s", cfg.Name, s, path)
		b.log.Debugf("two-way stream registered camera=%s stream=%s path=%s", cfg.Name, s, twoWayPath)

		var wantStream func() bool
		if cfg.IdleDisconnect {
			wantStream = idleGate(streamHandler, cfg.IdleTimeout)
		}

		var hint *AudioHint
		if h, ok := cfg.AudioHints[s]; ok {
			hint = &h
		}
		profile := s

		go b.runStream(
			ctx,
			device,
			uint8(cfg.Channel), //#nosec G115
			parseStream(s),
			streamHandler,
			meta,
			cfg.streamPauseConfig(motionState),
			wantStream,
			hint,
			func(observed AudioHint) { cam.reportAudioHint(profile, observed) },
			&cam.liveCatchUp,
		)
	}

	if preferredMeta != nil {
		metas = append([]*streamMetadata{preferredMeta}, metas...)
	}
	if multi && preferredHandler != nil {
		b.handler.addStream(basePath, preferredHandler)
		paths = append(paths, basePath)
		b.log.Debugf("stream alias registered camera=%s stream=%s path=%s", cfg.Name, preferredTalkProfile, basePath)
		if preferredTwoWayHandler != nil {
			twoWayBasePath := twoWayPathForStream(basePath)
			b.handler.addStream(twoWayBasePath, preferredTwoWayHandler)
			b.handler.addTalkAlias(twoWayBasePath, talkPublisher)
			paths = append(paths, twoWayBasePath)
			b.log.Debugf("two-way stream alias registered camera=%s stream=%s path=%s", cfg.Name, preferredTalkProfile, twoWayBasePath)
		}
	}

	return metas, paths
}

// idleGate reports whether a stream should stay connected: true while the
// handler (or a mirror) has playing clients or recent DESCRIBE/SETUP
// interest, false once it has been idle for timeout. Called from a single
// goroutine (the camera's StreamPackets loop).
func idleGate(handler *rtspStreamHandler, timeout time.Duration) func() bool {
	var idleSince time.Time
	return func() bool {
		if handler.hasClients() || handler.interestedSince(time.Now().Add(-timeout)) {
			idleSince = time.Time{}
			return true
		}
		if idleSince.IsZero() {
			idleSince = time.Now()
			return true
		}
		return time.Since(idleSince) < timeout
	}
}

// Camera is one bridged Reolink camera with its registered RTSP paths.
type Camera struct {
	bridge *Bridge
	cfg    CameraConfig
	device *cameraDevice
	motion *cameraMotionState
	cancel context.CancelFunc
	// liveCatchUp is read per frame by the running streams, so a change
	// applies without restarting them
	liveCatchUp atomic.Int64
	paths       []string
	streams     []*streamMetadata
	events      cameraEvents
}

// Name returns the camera name.
func (c *Camera) Name() string {
	return c.cfg.Name
}

// SetLiveCatchUp changes how far the picture may trail live before the stream
// drops the backlog, zero turns catching up off. Applies to the next frame.
func (c *Camera) SetLiveCatchUp(d time.Duration) {
	if d < 0 {
		d = 0
	}
	c.liveCatchUp.Store(int64(d))
}

// StreamInfo describes one exposed stream profile with the media parameters
// learned from the camera. Width/Height/FPS/codecs stay zero until the first
// packets arrive.
type StreamInfo struct {
	Profile         string // "main", "sub" or "extern"
	Path            string
	URL             string
	Width           uint32
	Height          uint32
	FPS             uint8
	VideoCodec      string
	AudioCodec      string
	AudioSampleRate int
	AudioChannels   int
}

// Streams returns the current metadata for every exposed stream profile,
// ordered with the talk-profile alias first (matching URL layout).
func (c *Camera) Streams() []StreamInfo {
	out := make([]StreamInfo, 0, len(c.streams))
	for _, m := range c.streams {
		s := m.snapshot()
		out = append(out, StreamInfo{
			Profile:         s.Name,
			Path:            s.Path,
			URL:             c.bridge.rtspURL(s.Path),
			Width:           s.Width,
			Height:          s.Height,
			FPS:             s.FPS,
			VideoCodec:      s.VideoCodec,
			AudioCodec:      s.AudioCodec,
			AudioSampleRate: s.AudioSampleRate,
			AudioChannels:   s.AudioChannels,
		})
	}
	return out
}

// StreamURL returns the RTSP playback URL for a stream profile ("main",
// "sub", "extern"). An empty profile returns the base path (the talk-profile
// alias when configured, otherwise the single stream).
func (c *Camera) StreamURL(profile string) string {
	return c.bridge.rtspURL(c.pathForProfile(profile))
}

// TwoWayURL returns the RTSP URL that additionally advertises the ONVIF
// audio backchannel for talkback on the given profile.
func (c *Camera) TwoWayURL(profile string) string {
	return c.bridge.rtspURL(twoWayPathForStream(c.pathForProfile(profile)))
}

// TalkURL returns the dedicated RTSP publish path that accepts a mono
// PCMA/PCMU/L16 publisher and forwards it as camera talkback.
func (c *Camera) TalkURL() string {
	return c.bridge.rtspURL(talkPathForCamera(c.cfg.RTSPPath))
}

func (c *Camera) pathForProfile(profile string) string {
	basePath := strings.TrimPrefix(c.cfg.RTSPPath, "/")
	profile = normalizeProfileName(profile)
	if profile == "" || len(c.cfg.Streams) == 1 {
		return basePath
	}
	return basePath + "_" + profile
}

func (c *Camera) stop() {
	c.cancel()
	c.device.Close("camera removed")
	c.bridge.handler.removePaths(c.paths)
	if c.bridge.webhook != nil {
		c.bridge.webhook.unregister(c.cfg.Name)
	}
}

func (b *Bridge) rtspURL(path string) string {
	return buildURL("rtsp", advertisedAuthority(b.opts.RTSPAddress, ""), path)
}
