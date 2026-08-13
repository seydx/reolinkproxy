package bridge

import (
	"context"
	"time"

	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/gortsplib/v5/pkg/format/rtph264"
	"github.com/bluenviron/gortsplib/v5/pkg/format/rtph265"
	"github.com/pion/rtp"

	"github.com/shareed2k/reolinkproxy/pkg/baichuan"
	"github.com/shareed2k/reolinkproxy/pkg/media"
)

const (
	// describeReadyTimeout is how long a DESCRIBE is held while the path is not
	// exposed yet. Everything before setReady (first keyframe plus the audio
	// window) has to fit in here, or clients time out instead of waiting.
	describeReadyTimeout = 10 * time.Second
	// audioProbeWindow gives a stream whose audio is unknown or expected enough
	// time to prove it, while leaving room for a long-GOP first keyframe.
	audioProbeWindow = 5 * time.Second
	// audioSilentWindow exposes a stream that had no audio last time quickly,
	// while still leaving room for audio that was switched on since.
	audioSilentWindow = 2 * time.Second
	// audioConfirmWindow bounds how long a track declared from a previous run
	// may stay empty before it is dropped again, e.g. after the microphone was
	// switched off in the camera app.
	audioConfirmWindow = 20 * time.Second
)

//nolint:gocyclo
func (b *Bridge) runStream(
	ctx context.Context,
	device *cameraDevice,
	channel uint8,
	stream baichuan.Stream,
	handler *rtspStreamHandler,
	meta *streamMetadata,
	pauseCfg streamPauseConfig,
	wantStream func() bool,
	hint *AudioHint,
	onHint func(AudioHint),
	liveCatchUp time.Duration,
) {
	// per-camera logger, so stream messages name the camera they belong to
	log := device.log
	logPackets := b.opts.LogPackets
	var (
		firstVideo   bool
		videoFormat  format.Format
		videoEncoder any
		paused       bool
		pauseReason  string
		lastWidth    uint32
		lastHeight   uint32
		timeline     mediaTimeline
		videoRTP     rtpTimestampGuard
		lastVideoUS  uint64
		haveVideoUS  bool
	)

	live := liveEdge{log: log, name: meta.name, maxLag: liveCatchUp}

	videoMedia := &description.Media{
		Type:    description.MediaTypeVideo,
		Control: "trackID=0",
	}

	// Audio-decision window: the RTSP session goes video-only if no audio shows
	// up in time, and that verdict holds for the whole session. Streams whose
	// audio is unknown or known-present get the patient window, a stream that
	// had no audio last time is exposed quickly.
	audioWindow := audioProbeWindow
	if seen, known := device.audioKnown(stream); known && !seen {
		audioWindow = audioSilentWindow
	}
	var startupDeadline time.Time
	var confirmDeadline time.Time

	// audio that misses the window still teaches the next session to be patient
	rebuildForAudio := false
	audio := &audioPublisher{log: log, onLateAudio: func() {
		device.setAudioKnown(stream, true)
		rebuildForAudio = true
	}}

	// what the last run saw decides how this one starts: a known track is
	// declared upfront so audio flows from the first second, a profile known to
	// be silent is exposed without waiting for audio that never comes
	declared := false
	switch {
	case hint == nil:
	case hint.known() && hint.Codec == "aac":
		if err := audio.declareAAC(hint.SampleRate, hint.Channels); err != nil {
			log.Warnf("stream %s could not reuse the known audio format: %v", meta.name, err)
		} else {
			declared = true
			meta.setAudioAAC(hint.SampleRate, hint.Channels)
			log.Debugf("stream %s starting with the known AAC track (%d Hz, %d ch)", meta.name, hint.SampleRate, hint.Channels)
		}
	case hint.Codec == "":
		audioWindow = 0
	}

	controlTicker := time.NewTicker(time.Second)
	defer controlTicker.Stop()

	updatePauseState := func(now time.Time) bool {
		nextPaused, nextReason := pauseCfg.shouldPause(now, handler)
		if nextPaused != paused || nextReason != pauseReason {
			if nextPaused {
				log.Debugf("stream %s paused: %s", meta.name, nextReason)
			} else if paused {
				log.Debugf("stream %s resumed", meta.name)
			}
			paused = nextPaused
			pauseReason = nextReason
		}
		return paused
	}

	streamCh := device.StreamPackets(ctx, channel, stream, wantStream)

	for {
		select {
		case <-ctx.Done():
			return

		case packet, ok := <-streamCh:
			if !ok {
				return
			}

			switch packet.Kind {
			case baichuan.MediaPacketInfoV1, baichuan.MediaPacketInfoV2:
				meta.setVideoInfo(packet.Width, packet.Height, packet.FPS, "")
				if packet.Width != lastWidth || packet.Height != lastHeight {
					lastWidth, lastHeight = packet.Width, packet.Height
					log.Debugf("stream %s info size=%dx%d fps=%d", meta.name, packet.Width, packet.Height, packet.FPS)
				}

			case baichuan.MediaPacketIFrame, baichuan.MediaPacketPFrame:
				if packet.Codec != "H265" && packet.Codec != "H264" {
					if !firstVideo {
						log.Warnf("stream %s skipping unsupported codec %q", meta.name, packet.Codec)
					}
					continue
				}

				nalus := media.SplitAnnexB(packet.Data)
				switch packet.Codec {
				case "H265":
					nalus = media.FilterH265DecodableNALs(nalus)
					nalus = media.ReorderH265NALsForAccessUnit(nalus)
				case "H264":
					nalus = media.ReorderH264NALsForAccessUnit(nalus)
				}
				if len(nalus) == 0 {
					continue
				}
				if !packet.HasTimestamp {
					log.Debugf("stream %s skipping video packet without timestamp", meta.name)
					continue
				}
				continuousUS, cameraUS := timeline.video(packet.TimestampMicrosecs)
				lastVideoUS, haveVideoUS = continuousUS, true

				if videoFormat == nil {
					meta.setVideoCodec(packet.Codec)
					switch packet.Codec {
					case "H265":
						h265Format := &format.H265{PayloadTyp: 96}
						videoFormat = h265Format
						enc, err := h265Format.CreateEncoder()
						if err != nil {
							log.Warnf("stream %s create h265 encoder: %v", meta.name, err)
							videoFormat = nil
							continue
						}
						videoEncoder = enc
					default:
						h264Format := &format.H264{PayloadTyp: 96, PacketizationMode: 1}
						videoFormat = h264Format
						enc, err := h264Format.CreateEncoder()
						if err != nil {
							log.Warnf("stream %s create h264 encoder: %v", meta.name, err)
							videoFormat = nil
							continue
						}
						videoEncoder = enc
					}
					videoMedia.Formats = []format.Format{videoFormat}
				}

				var readyToExpose bool
				var clockRate int

				switch packet.Codec {
				case "H265":
					h265Format := videoFormat.(*format.H265)
					clockRate = h265Format.ClockRate()
					vps, sps, pps := media.ExtractH265Params(nalus)
					if vps != nil || sps != nil || pps != nil {
						h265Format.VPS = coalesce(vps, h265Format.VPS)
						h265Format.SPS = coalesce(sps, h265Format.SPS)
						h265Format.PPS = coalesce(pps, h265Format.PPS)
					}
					readyToExpose = h265Format.VPS != nil && h265Format.SPS != nil && h265Format.PPS != nil
				default:
					h264Format := videoFormat.(*format.H264)
					clockRate = h264Format.ClockRate()
					sps, pps := media.ExtractH264Params(nalus)
					if sps != nil || pps != nil {
						h264Format.SPS = coalesce(sps, h264Format.SPS)
						h264Format.PPS = coalesce(pps, h264Format.PPS)
					}
					readyToExpose = h264Format.SPS != nil && h264Format.PPS != nil
				}

				if !handler.ready() {
					if !readyToExpose {
						if packet.Kind == baichuan.MediaPacketIFrame && logPackets {
							log.Debugf("stream %s waiting for parameter sets before exposing RTSP path", meta.name)
						}
						continue
					}
					if startupDeadline.IsZero() {
						startupDeadline = time.Now().Add(audioWindow)
					}
					if audio.awaitingStartupDecision(startupDeadline) {
						continue
					}

					if err := handler.setReady(videoMedia, audio.mediaDescription()); err != nil {
						log.Warnf("stream %s prepare rtsp stream: %v", meta.name, err)
						continue
					}
					device.setAudioKnown(stream, audio.ready())
					if declared && confirmDeadline.IsZero() {
						confirmDeadline = time.Now().Add(audioConfirmWindow)
					}
					reportAudioHint(onHint, audio)
				} else if declared && !audio.published && !confirmDeadline.IsZero() && time.Now().After(confirmDeadline) {
					// the track carried over from the last run never got a
					// packet, e.g. the microphone was switched off since
					if packet.Kind != baichuan.MediaPacketIFrame {
						continue
					}
					declared = false
					confirmDeadline = time.Time{}
					log.Infof("stream %s dropping the carried-over audio track, the camera sends none", meta.name)
					audio.dropDeclaration()
					if onHint != nil {
						onHint(AudioHint{})
					}
					handler.reset()
					continue
				} else if declared && audio.published && !confirmDeadline.IsZero() {
					confirmDeadline = time.Time{}
				} else if rebuildForAudio {
					// wait for a keyframe so the rebuilt session starts decodable
					if packet.Kind != baichuan.MediaPacketIFrame {
						continue
					}
					rebuildForAudio = false
					log.Infof("stream %s restarting session to carry the late audio track", meta.name)
					handler.reset()
					continue
				}

				now := time.Now()
				streamPaused := updatePauseState(now)
				if live.behind(cameraUS, packet.Kind == baichuan.MediaPacketIFrame, now) {
					continue
				}

				var pkts []*rtp.Packet
				var err error
				switch packet.Codec {
				case "H265":
					pkts, err = videoEncoder.(*rtph265.Encoder).Encode(nalus)
					if err == nil {
						media.FixH265AggregationTemporalID(pkts)
					}
				default:
					pkts, err = videoEncoder.(*rtph264.Encoder).Encode(nalus)
				}

				if err != nil {
					log.Warnf("stream %s encode rtp: %v", meta.name, err)
					continue
				}

				rawVideoRTP := rtpTimestampForClock(continuousUS, clockRate)
				if !streamPaused {
					ts := videoRTP.next(rawVideoRTP)
					for _, pkt := range pkts {
						pkt.Timestamp = ts
						handler.writePacket(videoMedia, pkt)
					}
				}

				if !firstVideo || logPackets {
					firstVideo = true
					log.Debugf("stream %s video packet kind=%s codec=%s nalus=%d bytes=%d ts_us=%d", meta.name, packet.Kind, packet.Codec, len(nalus), len(packet.Data), packet.TimestampMicrosecs)
				}

			case baichuan.MediaPacketAAC:
				timestamp := audioTimestampForPacket(packet, &timeline, lastVideoUS, haveVideoUS)
				if err := audio.processAAC(packet.Data, timestamp, handler, meta, !updatePauseState(time.Now()) && !live.catchingUp); err != nil {
					log.Warnf("stream %s audio publish error: %v", meta.name, err)
				}

			case baichuan.MediaPacketADPCM:
				timestamp := audioTimestampForPacket(packet, &timeline, lastVideoUS, haveVideoUS)
				if err := audio.processADPCM(packet.Data, timestamp, handler, meta, !updatePauseState(time.Now()) && !live.catchingUp); err != nil {
					log.Warnf("stream %s audio adpcm publish error: %v", meta.name, err)
				}
			}

		case <-controlTicker.C:
			updatePauseState(time.Now())
		}
	}
}

// defaultLiveCatchUp is how far the picture may fall behind the live edge
// before the stream skips ahead. Bursts after a short network stall stay
// under it.
const defaultLiveCatchUp = 3 * time.Second

// liveEdge measures how far the media clock has fallen behind the wall clock.
// A camera on a lossy link queues video it cannot send and delivers it late in
// bursts; passing that backlog on would leave every viewer permanently behind,
// so the stream drops it and resumes at the next keyframe.
type liveEdge struct {
	log  Logger
	name string
	// maxLag of zero passes late video on untouched, so nothing a recorder
	// might want is dropped
	maxLag time.Duration

	wallAnchor  time.Time
	mediaAnchor uint64
	anchored    bool
	catchingUp  bool
	lag         time.Duration
	skipped     int
	lastLog     time.Time
}

// behind reports whether this frame should be dropped to get back to live.
func (l *liveEdge) behind(mediaUS uint64, keyframe bool, now time.Time) bool {
	if l.maxLag <= 0 {
		return false
	}
	if !l.anchored {
		l.anchor(mediaUS, now)
		return false
	}

	lag := now.Sub(l.wallAnchor) - time.Duration(mediaUS-l.mediaAnchor)*time.Microsecond
	switch {
	case lag < 0:
		// the media clock caught up with the wall clock, this is the live edge
		l.anchor(mediaUS, now)
	case lag > l.maxLag:
		l.catchingUp = true
		l.lag = lag
	}

	if !l.catchingUp {
		return false
	}
	if !keyframe {
		l.skipped++
		return true
	}

	l.catchingUp = false
	l.anchor(mediaUS, now)
	l.report()
	return false
}

func (l *liveEdge) anchor(mediaUS uint64, now time.Time) {
	l.wallAnchor, l.mediaAnchor, l.anchored = now, mediaUS, true
}

// report logs a completed catch-up, rate-limited to once per minute so a
// permanently struggling link does not fill the log.
func (l *liveEdge) report() {
	skipped, lag := l.skipped, l.lag
	l.skipped, l.lag = 0, 0
	if skipped == 0 || l.log == nil {
		return
	}

	now := time.Now()
	if now.Sub(l.lastLog) < time.Minute {
		return
	}
	l.lastLog = now
	l.log.Warnf("stream %s ran %s behind live, skipped %d frame(s) to catch up; the camera cannot deliver its video in time",
		l.name, lag.Round(time.Second), skipped)
}

const (
	// video frames arrive back to back, so a consecutive delta far from the
	// frame duration is a camera clock hiccup, not a schedule
	maxTrustedVideoDeltaUS = 500_000
	// stands in for the average until a trusted delta was seen (25 fps)
	fallbackVideoDeltaUS = 40_000
	videoDeltaEMAAlpha   = 0.1
)

// mediaTimeline turns the camera clock into one continuous timeline for both
// tracks. Untrusted video deltas are replaced with the smoothed frame delta,
// and the correction is carried as an offset so audio timestamps land on the
// same corrected timeline instead of drifting away from the video.
type mediaTimeline struct {
	unwrapper  timestampUnwrapper
	offset     int64
	lastVideo  uint64
	avgDeltaUS float64
	haveVideo  bool
}

// video returns the timestamp to publish and the raw camera time behind it.
// The raw value skips the gap correction, so a caller measuring how far the
// picture trails live cannot mistake a corrected gap for a delivery backlog.
func (t *mediaTimeline) video(ts32 uint32) (mediaUS uint64, rawUS uint64) {
	raw := t.unwrapper.unwrap(ts32)
	us := t.applyOffset(raw)
	if !t.haveVideo {
		t.haveVideo = true
		t.lastVideo = us
		return us, raw
	}
	delta := int64(us) - int64(t.lastVideo) //#nosec G115
	switch {
	case delta <= 0 || delta > maxTrustedVideoDeltaUS:
		step := int64(t.avgDeltaUS)
		if step <= 0 {
			step = fallbackVideoDeltaUS
		}
		t.offset += delta - step
		us = t.lastVideo + uint64(step) //#nosec G115
	case t.avgDeltaUS == 0:
		t.avgDeltaUS = float64(delta)
	default:
		t.avgDeltaUS += (float64(delta) - t.avgDeltaUS) * videoDeltaEMAAlpha
	}
	t.lastVideo = us
	return us, raw
}

func (t *mediaTimeline) audio(ts32 uint32) uint64 {
	return t.applyOffset(t.unwrapper.unwrap(ts32))
}

func (t *mediaTimeline) applyOffset(raw uint64) uint64 {
	us := int64(raw) - t.offset //#nosec G115
	if us < 0 {
		return 0
	}
	return uint64(us)
}

type timestampUnwrapper struct {
	highest uint64
	offset  uint64
	baseSet bool
	// nowUnixMicro is optional; when nil, time.Now().UnixMicro is used (first sample anchors to wall clock).
	nowUnixMicro func() int64
}

func (u *timestampUnwrapper) unwrap(ts32 uint32) uint64 {
	if !u.baseSet {
		nowFn := func() int64 { return time.Now().UnixMicro() }
		if u.nowUnixMicro != nil {
			nowFn = u.nowUnixMicro
		}
		micros := nowFn()
		if micros < 0 {
			micros = 0
		}
		systemMicro := uint64(micros)
		u.offset = systemMicro - uint64(ts32)
		u.highest = uint64(ts32)
		u.baseSet = true
		return systemMicro
	}

	continuous := unwrapTimestamp(ts32, u.highest)
	if continuous > u.highest {
		u.highest = continuous
	}
	return continuous + u.offset
}

type rtpTimestampGuard struct {
	offset uint32
	last   uint32
	set    bool
}

func (g *rtpTimestampGuard) next(ts uint32) uint32 {
	if !g.set {
		g.last = ts
		g.set = true
		return ts
	}
	adjusted := ts + g.offset
	if ts == g.last {
		g.offset = g.last + 1 - ts
		adjusted = g.last + 1
	} else if !rtpTimestampAfter(adjusted, g.last) {
		jumpBackward := uint32(int32(g.last - adjusted))
		if jumpBackward > 90000 {
			g.offset = g.last + 1 - ts
			adjusted = ts + g.offset
		} else {
			adjusted = g.last + 1
		}
	}
	g.last = adjusted
	return adjusted
}

func (g *rtpTimestampGuard) applyBaseToPackets(pkts []*rtp.Packet, base uint32, duration uint32) uint32 {
	if len(pkts) == 0 {
		return base
	}

	sum := base + pkts[0].Timestamp //#nosec G115
	first := sum + g.offset
	if g.set && sum == g.last {
		g.offset = 0
		first = sum
	}
	if g.set && rtpTimestampBefore(first, g.last) {
		jumpBackward := uint32(int32(g.last - first))
		if jumpBackward > 90000 {
			g.offset = g.last - sum
			first = sum + g.offset
		} else {
			first = g.last
		}
	}

	adjusted := first
	if duration == 0 {
		g.last = adjusted
	} else {
		g.last = adjusted + duration
	}
	g.set = true
	return adjusted - pkts[0].Timestamp
}

func rtpTimestampAfter(ts uint32, prev uint32) bool {
	return int32(ts-prev) > 0 //#nosec G115
}

func rtpTimestampBefore(ts uint32, prev uint32) bool {
	return int32(ts-prev) < 0 //#nosec G115
}

// reportAudioHint hands the observed audio configuration to the caller so it
// can be reused on the next start. An unconfigured publisher reports silence.
func reportAudioHint(onHint func(AudioHint), audio *audioPublisher) {
	if onHint == nil {
		return
	}
	onHint(audio.hint())
}

// audioTimestampForPacket resolves the media time of an audio packet. AAC
// frames carry no timestamp of their own, so the video clock anchors them and
// both tracks end up describing the same media time. Camera audio and video
// run on one clock (measured: 6.080s of samples against a 6.058s video span),
// so counting samples keeps them together from that anchor on.
func audioTimestampForPacket(packet baichuan.MediaPacket, timeline *mediaTimeline, videoUS uint64, haveVideoUS bool) mediaTimestamp {
	if packet.HasTimestamp {
		return mediaTimestamp{
			Microseconds:  timeline.audio(packet.TimestampMicrosecs),
			Valid:         true,
			Authoritative: true,
		}
	}
	if haveVideoUS {
		return mediaTimestamp{Microseconds: videoUS, Valid: true}
	}
	return mediaTimestamp{}
}

func unwrapTimestamp(ts32 uint32, highest64 uint64) uint64 {
	if highest64 == 0 {
		return uint64(ts32)
	}

	high32 := highest64 >> 32
	cand1 := (high32 << 32) | uint64(ts32)

	cand2 := cand1
	if cand1 >= 0x100000000 {
		cand2 = cand1 - 0x100000000
	}

	cand3 := cand1 + 0x100000000

	absDiff := func(a, b uint64) uint64 {
		if a > b {
			return a - b
		}
		return b - a
	}

	bestCand := cand1
	bestDiff := absDiff(cand1, highest64)

	if diff2 := absDiff(cand2, highest64); diff2 < bestDiff {
		bestCand = cand2
		bestDiff = diff2
	}
	if diff3 := absDiff(cand3, highest64); diff3 < bestDiff {
		bestCand = cand3
	}

	return bestCand
}

func parseStream(v string) baichuan.Stream {
	switch v {
	case "sub":
		return baichuan.StreamSub
	case "extern":
		return baichuan.StreamExtern
	default:
		return baichuan.StreamMain
	}
}
