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
) {
	// per-camera logger, so stream and pacer messages name the camera they belong to
	log := device.log
	logPackets := b.opts.LogPackets
	var (
		firstVideo       bool
		videoFormat      format.Format
		videoEncoder     any
		paused           bool
		pauseReason      string
		lastWidth        uint32
		lastHeight       uint32
		streamTimestamps timestampUnwrapper
		videoRTP         rtpTimestampGuard
		lastVideoUS      uint64
		haveVideoUS      bool
	)

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

	pacer := &mediaPacer{
		ch:      make(chan pacedFrame, 600),
		cursor:  paceCursor{maxLead: b.opts.PacerMaxLead, initialLatency: b.opts.PacerInitialLatency},
		handler: handler,
		log:     log,
	}
	go pacer.run(ctx)

	// audio that misses the window still teaches the next session to be patient
	audio := &audioPublisher{audioPacer: pacer, log: log, onLateAudio: func() { device.setAudioKnown(stream, true) }}

	emitVideo := func(pkts []*rtp.Packet, continuousUS uint64) {
		if len(pkts) == 0 {
			return
		}
		pacer.enqueue(pacedFrame{pkts: pkts, media: videoMedia, mediaUS: continuousUS})
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
				continuousUS := streamTimestamps.unwrap(packet.TimestampMicrosecs)
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
				}

				streamPaused := updatePauseState(time.Now())

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
					}
					emitVideo(pkts, continuousUS)
				}

				if !firstVideo || logPackets {
					firstVideo = true
					log.Debugf("stream %s video packet kind=%s codec=%s nalus=%d bytes=%d ts_us=%d", meta.name, packet.Kind, packet.Codec, len(nalus), len(packet.Data), packet.TimestampMicrosecs)
				}

			case baichuan.MediaPacketAAC:
				timestamp := audioTimestampForPacket(packet, &streamTimestamps, lastVideoUS, haveVideoUS)
				if err := audio.processAAC(packet.Data, timestamp, handler, meta, !updatePauseState(time.Now())); err != nil {
					log.Warnf("stream %s audio publish error: %v", meta.name, err)
				}

			case baichuan.MediaPacketADPCM:
				timestamp := audioTimestampForPacket(packet, &streamTimestamps, lastVideoUS, haveVideoUS)
				if err := audio.processADPCM(packet.Data, timestamp, handler, meta, !updatePauseState(time.Now())); err != nil {
					log.Warnf("stream %s audio adpcm publish error: %v", meta.name, err)
				}
			}

		case <-controlTicker.C:
			updatePauseState(time.Now())
		}
	}
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

// audioTimestampForPacket resolves the media time of an audio packet. AAC
// frames carry no timestamp of their own, so the video clock anchors them and
// both tracks end up describing the same media time. Camera audio and video
// run on one clock (measured: 6.080s of samples against a 6.058s video span),
// so counting samples keeps them together from that anchor on.
func audioTimestampForPacket(packet baichuan.MediaPacket, audioTimestamps *timestampUnwrapper, videoUS uint64, haveVideoUS bool) mediaTimestamp {
	if packet.HasTimestamp {
		return mediaTimestamp{
			Microseconds:  audioTimestamps.unwrap(packet.TimestampMicrosecs),
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
