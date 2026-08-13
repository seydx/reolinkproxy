package bridge

import (
	"fmt"
	"strings"
	"time"
)

// CameraConfig describes one Reolink camera to expose through the bridge.
type CameraConfig struct {
	// Name identifies the camera and doubles as the default RTSP path prefix.
	Name string
	// Logger receives everything logged for this camera (connection, streams).
	// Falls back to the bridge logger when nil.
	Logger Logger
	// Host is the camera IP for direct Baichuan TCP. Takes precedence over UID.
	Host string
	// Port is the Baichuan TCP port (default 9000).
	Port int
	// UID connects via local UDP broadcast discovery (same L2 segment only).
	UID string
	// Username and Password are the camera's local account credentials.
	Username string
	Password string
	// Timeout bounds connect/login attempts (default 10s).
	Timeout time.Duration
	// Streams lists the profiles to expose: "main", "sub", "extern" (default ["main"]).
	Streams []string
	// Channel selects the camera channel on multi-channel devices (NVR/Hub).
	Channel int
	// RTSPPath overrides the base RTSP path (default "<Name>/stream").
	RTSPPath string
	// TalkProfile selects which profile gets the clean base-path alias (and
	// therefore the default two-way variant). Must be one of Streams.
	TalkProfile string
	// TalkVolume scales talkback audio in percent (default 100).
	TalkVolume int
	// PauseOnMotion stops publishing RTP after motion has been inactive for PauseTimeout.
	PauseOnMotion bool
	// PauseOnClient stops publishing RTP while no RTSP client is playing.
	PauseOnClient bool
	// PauseTimeout is the motion-inactive duration before pausing (default 1s).
	PauseTimeout time.Duration
	// LiveCatchUp bounds how far the picture may trail live before the stream
	// drops the backlog and resumes at the next keyframe. Nil uses the
	// default, zero turns catching up off and passes late video on as it is.
	LiveCatchUp *time.Duration
	// IdleDisconnect stops the underlying Baichuan preview session after a
	// stream has had no RTSP clients or DESCRIBE/SETUP interest for
	// IdleTimeout, and restarts it on the next client.
	IdleDisconnect bool
	// IdleTimeout is the no-client duration before the preview stops (default 30s).
	IdleTimeout time.Duration
	// BatteryCamera uses a much longer reconnect backoff (30s instead of 2s)
	// so a sleeping battery camera is not woken over and over.
	BatteryCamera bool
	// AudioHints carries what a previous run learned about each profile's
	// audio, keyed by profile name. A stream whose audio is known starts with
	// the track already declared instead of waiting for the first packet; a
	// stream known to be silent is exposed without waiting at all. Both are
	// hints: the stream corrects them when reality differs.
	AudioHints map[string]AudioHint
}

// AudioHint describes a profile's audio as last observed. Present with an
// empty Codec means the profile carries no audio.
type AudioHint struct {
	Codec      string // "aac", "pcma", or empty for none
	SampleRate int
	Channels   int
}

// known reports whether the hint carries a usable audio configuration.
func (h AudioHint) known() bool {
	return h.Codec != "" && h.SampleRate > 0 && h.Channels > 0
}

// ApplyDefaults fills unset fields with their documented defaults.
func (c *CameraConfig) ApplyDefaults() {
	if c.Port == 0 {
		c.Port = 9000
	}
	if len(c.Streams) == 0 {
		c.Streams = []string{"main"}
	}
	normalized := make([]string, 0, len(c.Streams))
	for _, s := range c.Streams {
		if name := normalizeProfileName(s); name != "" {
			normalized = append(normalized, name)
		}
	}
	c.Streams = normalized
	if c.RTSPPath == "" {
		c.RTSPPath = c.Name + "/stream"
	}
	if c.Timeout == 0 {
		c.Timeout = 10 * time.Second
	}
	c.TalkProfile = normalizeProfileName(c.TalkProfile)
	if c.TalkVolume == 0 {
		c.TalkVolume = 100
	}
	if c.LiveCatchUp == nil {
		fallback := defaultLiveCatchUp
		c.LiveCatchUp = &fallback
	}
	if c.PauseTimeout == 0 {
		c.PauseTimeout = time.Second
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = 30 * time.Second
	}
}

// Validate reports whether the configuration is usable. Call ApplyDefaults first.
func (c *CameraConfig) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("camera name is required")
	}
	if c.Host == "" && c.UID == "" {
		return fmt.Errorf("camera host or uid is required")
	}
	if c.TalkProfile != "" && !c.hasStream(c.TalkProfile) {
		return fmt.Errorf("camera talk profile %q must be one of configured streams %v", c.TalkProfile, c.Streams)
	}
	return nil
}

func normalizeProfileName(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func (c *CameraConfig) hasStream(name string) bool {
	name = normalizeProfileName(name)
	for _, stream := range c.Streams {
		if stream == name {
			return true
		}
	}
	return false
}

func (c *CameraConfig) preferredTalkProfile() string {
	if c.hasStream(c.TalkProfile) {
		return c.TalkProfile
	}
	return ""
}
