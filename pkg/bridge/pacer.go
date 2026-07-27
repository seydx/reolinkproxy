package bridge

import (
	"context"
	"sync"
	"time"

	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/pion/rtp"
)

// pacedFrame is one logical emission: one or more RTP packets sharing the same
// media time (e.g. one AAC AU or one HEVC AU split across RTP fragments).
type pacedFrame struct {
	pkts    []*rtp.Packet
	media   *description.Media
	mediaUS uint64
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

	overflowMu      sync.Mutex
	lastOverflowLog time.Time
}

// enqueue sends a paced frame to the pacer goroutine. A full queue drops the
// oldest frame, so a stalled writer cannot pin a permanent backlog. Overflow
// is logged at most once per minute.
func (p *mediaPacer) enqueue(item pacedFrame) {
	select {
	case p.ch <- item:
		return
	default:
	}

	select {
	case <-p.ch:
	default:
	}
	select {
	case p.ch <- item:
	default:
	}
	p.warnOverflowOnce()
}

// warnOverflowOnce logs a queue-overflow warning, rate-limited to once per
// minute to avoid log spam under sustained overload.
func (p *mediaPacer) warnOverflowOnce() {
	p.overflowMu.Lock()
	defer p.overflowMu.Unlock()
	now := time.Now()
	if now.Sub(p.lastOverflowLog) < 60*time.Second {
		return
	}
	p.lastOverflowLog = now
	p.log.Warnf("media pacer queue overflow (cap=%d); dropping frame", cap(p.ch))
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
