package bridge

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/shareed2k/reolinkproxy/pkg/baichuan"
)

// webhookServer receives event pushes from battery cameras. It is started
// lazily by the first battery camera and routes POST /<cameraName> to that
// camera's push handler. Camera names are UUIDs in practice, which keeps the
// paths unguessable.
type webhookServer struct {
	log    Logger
	server *http.Server

	mu      sync.Mutex
	cameras map[string]*Camera
	started bool
}

func newWebhookServer(address string, log Logger) *webhookServer {
	ws := &webhookServer{
		log:     log,
		cameras: make(map[string]*Camera),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", ws.handle)
	ws.server = &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return ws
}

// ensureStarted starts the listener on first use.
func (ws *webhookServer) ensureStarted() error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.started {
		return nil
	}

	errCh := make(chan error, 1)
	go func() {
		if err := ws.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
			ws.log.Errorf("webhook server error: %v", err)
		}
	}()

	// ListenAndServe reports bind errors asynchronously; give it a moment so
	// callers learn about an occupied port instead of registering cameras on
	// a dead listener.
	select {
	case err := <-errCh:
		return err
	case <-time.After(200 * time.Millisecond):
	}

	ws.started = true
	ws.log.Infof("webhook server listening at %s", ws.server.Addr)
	return nil
}

func (ws *webhookServer) close() {
	ws.mu.Lock()
	started := ws.started
	ws.started = false
	ws.mu.Unlock()
	if started {
		_ = ws.server.Close()
	}
}

func (ws *webhookServer) register(name string, cam *Camera) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.cameras[name] = cam
}

func (ws *webhookServer) unregister(name string) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	delete(ws.cameras, name)
}

func (ws *webhookServer) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	name := strings.Trim(r.URL.Path, "/")
	ws.mu.Lock()
	cam := ws.cameras[name]
	ws.mu.Unlock()
	if cam == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

	cam.handleWebhookPush(body)
	w.WriteHeader(http.StatusOK)
}

// webhookURL builds the push URL a camera should POST to.
func (b *Bridge) webhookURL(cameraName string) string {
	return fmt.Sprintf("http://%s/%s", advertisedAuthority(b.opts.WebhookAddress, ""), cameraName)
}

// handleWebhookPush decodes one webhook POST from the camera and dispatches
// it through the same event handlers as TCP pushes.
func (c *Camera) handleWebhookPush(body []byte) {
	push, err := baichuan.ParseWebhookPush(body)
	if err != nil {
		c.bridge.log.Debugf("camera %s webhook: %v", c.cfg.Name, err)
		return
	}

	channel := uint8(c.cfg.Channel) //#nosec G115

	if push.XML != "" {
		switch push.Cmd {
		case 33:
			if state, matched, err := baichuan.ParseAlarmStateXML(push.XML, channel); err == nil && matched {
				c.handleAlarm(state)
			}
		case 252, 253:
			for _, info := range baichuan.ParseBatteryXML(push.XML) {
				if info.ChannelID == int(channel) {
					c.handleBattery(info)
				}
			}
		case 145:
			if sleeping, ok := baichuan.ParseSleepStates(push.XML)[int(channel)]; ok {
				c.handleSleep(sleeping)
			}
		default:
			c.bridge.log.Debugf("camera %s webhook: unhandled cmd %d", c.cfg.Name, push.Cmd)
		}
		return
	}

	// Simple event form: sleep/wake transitions; a wake carries the reason.
	switch push.Data["event"] {
	case "sleep":
		c.handleSleep(true)
	case "wake":
		c.handleSleep(false)
		switch push.Data["reason"] {
		case "doorbell":
			c.handleAlarm(baichuan.AlarmState{Visitor: true})
		case "pir":
			c.handleAlarm(baichuan.AlarmState{MotionDetected: true})
		}
	case "test":
	default:
		c.bridge.log.Debugf("camera %s webhook: unhandled event %q", c.cfg.Name, push.Data["event"])
	}
}
