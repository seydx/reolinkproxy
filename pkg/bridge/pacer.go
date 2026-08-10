package bridge

import (
	"context"
	"time"

	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/pion/rtp"
)

// pacedFrame is one logical emission: one or more RTP packets sharing the same
// media time (e.g. one AAC AU or one HEVC AU split across RTP fragments).
// keyframe marks a video I-frame, audio marks independently decodable audio.
type pacedFrame struct {
	pkts     []*rtp.Packet
	media    *description.Media
	mediaUS  uint64
	keyframe bool
	audio    bool
}

// a media time this far from the anchor is a clock jump, not a schedule; the
// bound also keeps the microsecond arithmetic away from overflow
const pacerSkewLimit = time.Hour

// paceCursor maps media time to wall time through a single anchor.
type paceCursor struct {
	maxLead        time.Duration
	initialLatency time.Duration

	anchorMedia uint64
	anchorWall  time.Time
	anchored    bool
}

// schedule returns when the frame at mediaUS should go out. A target too far
// ahead means the camera clock jumped, too far behind means we cannot keep up;
// either way the anchor is worthless, and re-anchoring moves every track
// together so their spacing survives.
func (c *paceCursor) schedule(mediaUS uint64, now time.Time) time.Time {
	if !c.anchored {
		c.anchorMedia, c.anchorWall, c.anchored = mediaUS, now.Add(c.initialLatency), true
	}

	target := c.anchorWall.Add(mediaOffset(mediaUS, c.anchorMedia))
	if target.After(now.Add(c.maxLead)) || target.Before(now.Add(-c.maxLead)) {
		c.anchorMedia, c.anchorWall = mediaUS, now
		return now
	}
	return target
}

// mediaPacer smooths bursty upstream media onto the wire. Audio and video share
// one pacer: with a cursor each, one track drains ahead of the other and the
// media-time relationship the camera sent them with is lost on the wire.
type mediaPacer struct {
	ch      chan pacedFrame
	cursor  paceCursor
	handler *rtspStreamHandler
	log     Logger

	// enqueue-side state; enqueue is only called from the runStream goroutine
	awaitKeyframe   bool
	droppedFrames   int
	lastOverflowLog time.Time
}

// enqueue sends a paced frame to the pacer goroutine. A full queue means the
// writer cannot keep up; dropping single frames mid-GOP would leave every
// reader with broken references, so the whole backlog is flushed and video
// stays muted until the next keyframe starts a clean sequence. Audio frames
// are independent and keep flowing.
func (p *mediaPacer) enqueue(item pacedFrame) {
	if p.awaitKeyframe {
		switch {
		case item.keyframe:
			p.awaitKeyframe = false
			p.reportDropped()
		case item.audio:
			// independently decodable, passes through the video mute
		default:
			p.droppedFrames++
			return
		}
	}

	select {
	case p.ch <- item:
		return
	default:
	}

	for {
		select {
		case <-p.ch:
			p.droppedFrames++
			continue
		default:
		}
		break
	}

	if item.keyframe || item.audio {
		select {
		case p.ch <- item:
		default:
			p.droppedFrames++
		}
	} else {
		p.droppedFrames++
	}

	if item.keyframe {
		p.reportDropped()
	} else {
		p.awaitKeyframe = true
	}
}

// reportDropped logs how many frames an overflow cost, rate-limited to once
// per minute to avoid log spam under sustained overload.
func (p *mediaPacer) reportDropped() {
	if p.droppedFrames == 0 {
		return
	}
	dropped := p.droppedFrames
	p.droppedFrames = 0

	now := time.Now()
	if now.Sub(p.lastOverflowLog) < 60*time.Second {
		return
	}
	p.lastOverflowLog = now
	p.log.Warnf("media pacer overflow (cap=%d); dropped %d frame(s), resuming at a keyframe", cap(p.ch), dropped)
}

// run drains pacedFrame values from the channel until ctx is cancelled or the
// channel closes, emitting each frame at the wall time its media time maps to.
func (p *mediaPacer) run(ctx context.Context) {
	for {
		var item pacedFrame
		select {
		case <-ctx.Done():
			return
		case it, ok := <-p.ch:
			if !ok {
				return
			}
			item = it
		}

		now := time.Now()
		target := p.cursor.schedule(item.mediaUS, now)

		if target.After(now) {
			timer := time.NewTimer(time.Until(target))
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
			}
		}

		for _, pkt := range item.pkts {
			if p.handler != nil && item.media != nil && pkt != nil {
				p.handler.writePacket(item.media, pkt)
			}
		}
	}
}

// mediaOffset returns how far mediaUS sits from the anchor, clamped so a
// nonsense timestamp cannot overflow the duration arithmetic.
func mediaOffset(mediaUS uint64, anchorUS uint64) time.Duration {
	limit := int64(pacerSkewLimit / time.Microsecond)
	delta := min(max(int64(mediaUS)-int64(anchorUS), -limit), limit) //#nosec G115
	return time.Duration(delta) * time.Microsecond
}
