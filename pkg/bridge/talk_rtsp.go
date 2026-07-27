package bridge

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/bluenviron/gortsplib/v5"
	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	gformat "github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/pion/rtp"
)

type rtspTalkPublisher struct {
	path       string
	cameraName string
	channel    uint8
	device     *cameraDevice
	talkVolume int
	log        Logger

	mu     sync.Mutex
	active *rtspTalkSessionState
	stream *gortsplib.ServerStream
}

// rtspTalkSessionState is the camera's single talk bridge. go2rtc opens two
// RTSP sessions on the two-way path (the reconnected producer and a separate
// consumer for the audio) and either may carry the speech, so every session
// feeds this one bridge instead of replacing the others.
type rtspTalkSessionState struct {
	publisher *rtspTalkPublisher
	path      string
	ctx       context.Context
	cancel    context.CancelFunc
	pcmCh     chan []int16
	done      chan struct{}
	stopOnce  sync.Once
	refs      int // sessions feeding this bridge, guarded by publisher.mu
}

// Live speech is worth less the later it arrives, so the queue holds only a
// few packets and drops the oldest beyond that. A deep queue turns every
// hiccup into permanent delay, because input and output run at the same rate
// and the backlog never drains.
const rtspTalkPCMQueueSize = 8

func newDedicatedTalkMedia() *description.Media {
	return &description.Media{
		Type:    description.MediaTypeAudio,
		Control: "trackID=0",
		Formats: []gformat.Format{
			&gformat.G711{
				PayloadTyp:   0,
				MULaw:        true,
				SampleRate:   8000,
				ChannelCount: 1,
			},
			&gformat.G711{
				PayloadTyp:   8,
				MULaw:        false,
				SampleRate:   8000,
				ChannelCount: 1,
			},
		},
	}
}

func newBackChannelMedia() *description.Media {
	media := newDedicatedTalkMedia()
	media.Control = "trackID=2"
	media.IsBackChannel = true
	return media
}

func newRTSPTalkPublisher(
	path string,
	cameraName string,
	channel uint8,
	device *cameraDevice,
	talkVolume int,
	log Logger,
) *rtspTalkPublisher {
	return &rtspTalkPublisher{
		path:       strings.TrimPrefix(path, "/"),
		cameraName: cameraName,
		channel:    channel,
		device:     device,
		talkVolume: talkVolume,
		log:        log,
	}
}

func talkPathForCamera(rtspPath string) string {
	rtspPath = strings.Trim(strings.TrimSpace(rtspPath), "/")
	if rtspPath == "" {
		return "talk"
	}
	return rtspPath + "_talk"
}

func twoWayPathForStream(rtspPath string) string {
	rtspPath = strings.Trim(strings.TrimSpace(rtspPath), "/")
	if rtspPath == "" {
		return "twoway"
	}
	return rtspPath + "_twoway"
}

func (s *rtspTalkSessionState) close() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		if s.publisher != nil {
			s.publisher.finish(s)
		}
		if s.cancel != nil {
			s.cancel()
		}
	})
}

func (p *rtspTalkPublisher) finish(state *rtspTalkSessionState) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.active == state {
		p.active = nil
	}
}

func (p *rtspTalkPublisher) describe(server *gortsplib.Server) (*gortsplib.ServerStream, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stream != nil {
		return p.stream, nil
	}

	desc := &description.Session{
		Medias: []*description.Media{newDedicatedTalkMedia()},
	}

	stream := &gortsplib.ServerStream{Desc: desc, Server: server}
	if err := stream.Initialize(); err != nil {
		return nil, err
	}
	p.stream = stream
	return stream, nil
}

func (p *rtspTalkPublisher) announce(ctx *gortsplib.ServerHandlerOnAnnounceCtx) (*base.Response, error) {
	if _, err := selectTalkInput(ctx.Description); err != nil {
		return &base.Response{StatusCode: base.StatusBadRequest}, err
	}
	return &base.Response{StatusCode: base.StatusOK}, nil
}

// ensureSessionState attaches the session to the camera's talk bridge, creating
// the bridge on first use. The bool reports whether this call created it, so
// only one pipeline goroutine is started.
func (p *rtspTalkPublisher) ensureSessionState(session *gortsplib.ServerSession) (*rtspSessionState, *rtspTalkSessionState, bool) {
	state := attachSessionState(session)
	if state == nil {
		return nil, nil, false
	}
	if state.talk != nil && state.talk.publisher == p {
		return state, state.talk, false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.active != nil {
		select {
		case <-p.active.ctx.Done():
			p.active = nil
		default:
		}
	}

	created := p.active == nil
	if created {
		bridgeCtx, cancel := context.WithCancel(context.Background())
		p.active = &rtspTalkSessionState{
			publisher: p,
			ctx:       bridgeCtx,
			cancel:    cancel,
			pcmCh:     make(chan []int16, rtspTalkPCMQueueSize),
			done:      make(chan struct{}),
		}
	}

	p.active.refs++
	state.talk = p.active
	return state, p.active, created
}

// release drops one session's hold on the talk bridge and tears it down once
// the last session is gone.
func (p *rtspTalkPublisher) release(active *rtspTalkSessionState) {
	if active == nil {
		return
	}

	p.mu.Lock()
	active.refs--
	last := active.refs <= 0
	p.mu.Unlock()

	if last {
		active.close()
	}
}

func (p *rtspTalkPublisher) bindInputs(session *gortsplib.ServerSession, inputs []*rtspTalkInput, active *rtspTalkSessionState) *rtspTalkInput {
	var primary *rtspTalkInput

	for _, input := range inputs {
		if input == nil {
			continue
		}
		if primary == nil {
			primary = input
		}

		current := input
		var inputFormat gformat.Format
		if current.g711 != nil {
			inputFormat = current.g711
		} else {
			inputFormat = current.lpcm
		}

		session.OnPacketRTP(current.media, inputFormat, func(pkt *rtp.Packet) {
			pcm, err := current.decode(pkt)
			if err != nil {
				p.log.Warnf("talk decode error: %v", err)
				return
			}
			if len(pcm) == 0 {
				return
			}

			if p.talkVolume != 100 {
				applyTalkVolume(pcm, p.talkVolume)
			}

			enqueueTalkPCM(active.ctx, active.pcmCh, pcm)
		})
	}

	return primary
}

func (p *rtspTalkPublisher) startBridge(session *gortsplib.ServerSession, path string, inputs []*rtspTalkInput) error {
	if existing := attachSessionState(session); existing != nil && existing.talk != nil && existing.talk.publisher == p {
		return nil
	}

	state, active, created := p.ensureSessionState(session)
	if state == nil || active == nil {
		return fmt.Errorf("failed to initialize talk session state")
	}

	// every session feeds the bridge, whichever one the speech arrives on
	primary := p.bindInputs(session, inputs, active)
	if primary == nil {
		return fmt.Errorf("talkback input is not configured")
	}

	if !created {
		return nil
	}
	active.path = strings.TrimPrefix(path, "/")

	go func() {
		defer p.finish(active)
		defer active.close()
		defer close(active.done)
		pipeline := newTalkbackPipeline(p.cameraName, p.channel, p.device, p.talkVolume, p.log)
		pipeline.run(active.ctx, active.pcmCh, primary, active.path)
	}()

	return nil
}

func (p *rtspTalkPublisher) record(ctx *gortsplib.ServerHandlerOnRecordCtx) (*base.Response, error) {
	desc := ctx.Session.AnnouncedDescription()
	if desc == nil && p.stream != nil {
		desc = p.stream.Desc
	}
	input, err := selectTalkInput(desc)
	if err != nil {
		return &base.Response{StatusCode: base.StatusBadRequest}, err
	}

	if err := p.startBridge(ctx.Session, ctx.Path, []*rtspTalkInput{input}); err != nil {
		return &base.Response{StatusCode: base.StatusBadRequest}, err
	}

	return &base.Response{StatusCode: base.StatusOK}, nil
}

func (p *rtspTalkPublisher) startBackChannel(session *gortsplib.ServerSession, path string) error {
	p.log.Debugf("talk starting backchannel for path %s", path)
	inputs, err := selectBackChannelInputs(session.Medias())
	if err != nil {
		p.log.Warnf("talk failed to select backchannel inputs: %v", err)
		return err
	}
	return p.startBridge(session, path, inputs)
}

func (h *rtspServerHandler) OnAnnounce(ctx *gortsplib.ServerHandlerOnAnnounceCtx) (*base.Response, error) {
	talk := h.getTalk(ctx.Path)
	if talk == nil {
		return &base.Response{StatusCode: base.StatusNotFound}, fmt.Errorf("rtsp announce: talk path not found for %q", ctx.Path)
	}
	return talk.announce(ctx)
}

func (h *rtspServerHandler) OnRecord(ctx *gortsplib.ServerHandlerOnRecordCtx) (*base.Response, error) {
	talk := h.getTalk(ctx.Path)
	if talk == nil {
		return &base.Response{StatusCode: base.StatusNotFound}, fmt.Errorf("rtsp record: talk path not found for %q", ctx.Path)
	}
	return talk.record(ctx)
}
