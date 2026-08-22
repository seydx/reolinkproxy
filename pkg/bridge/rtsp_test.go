package bridge

import (
	"bytes"
	"encoding/binary"
	"testing"

	gortsplib "github.com/bluenviron/gortsplib/v5"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/mpeg4audio"
	"github.com/pion/rtp"

	"github.com/shareed2k/reolinkproxy/pkg/baichuan"
	"github.com/shareed2k/reolinkproxy/pkg/media"
)

func TestReorderH265NALsForAccessUnit(t *testing.T) {
	t.Parallel()

	vps := []byte{0x40, 0x01, 0xaa}
	sei := []byte{0x4E, 0x01, 0xbb}
	slice := []byte{0x26, 0x01, 0xcc}

	// Camera order: slice first, then VPS/SEI — reorder should move non-VCL ahead of VCL.
	got := media.ReorderH265NALsForAccessUnit([][]byte{slice, vps, sei})
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	if !bytes.Equal(got[0], vps) || !bytes.Equal(got[1], sei) || !bytes.Equal(got[2], slice) {
		t.Fatalf("order = %x %x %x, want vps sei slice", got[0], got[1], got[2])
	}

	// Already-correct order is unchanged.
	unchanged := media.ReorderH265NALsForAccessUnit([][]byte{vps, sei, slice})
	want := [][]byte{vps, sei, slice}
	for i := range want {
		if i >= len(unchanged) || !bytes.Equal(unchanged[i], want[i]) {
			t.Fatalf("identity reorder at %d: got %d slices", i, len(unchanged))
		}
	}
}

func TestFixH265AggregationTemporalID(t *testing.T) {
	t.Parallel()

	firstNALU := []byte{0x40, 0x01, 0xaa, 0xbb}
	payload := make([]byte, 2+2+len(firstNALU))
	payload[0] = 48 << 1
	payload[1] = 0x00
	binary.BigEndian.PutUint16(payload[2:4], uint16(len(firstNALU)))
	copy(payload[4:], firstNALU)

	pkt := &rtp.Packet{Payload: payload}
	media.FixH265AggregationTemporalID([]*rtp.Packet{pkt})

	if got, want := pkt.Payload[0], (firstNALU[0]&0x81)|(48<<1); got != want {
		t.Fatalf("payload[0] = %#x, want %#x", got, want)
	}
	if got, want := pkt.Payload[1], firstNALU[1]; got != want {
		t.Fatalf("payload[1] = %#x, want %#x", got, want)
	}
}

func TestParseAACAccessUnits(t *testing.T) {
	t.Parallel()

	raw, err := mpeg4audio.ADTSPackets{
		&mpeg4audio.ADTSPacket{
			Type:          mpeg4audio.ObjectTypeAACLC,
			SampleRate:    16000,
			ChannelConfig: 1,
			AU:            []byte{0x11, 0x22, 0x33},
		},
		&mpeg4audio.ADTSPacket{
			Type:          mpeg4audio.ObjectTypeAACLC,
			SampleRate:    16000,
			ChannelConfig: 1,
			AU:            []byte{0x44, 0x55},
		},
	}.Marshal()
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	aus, cfg, err := media.ParseAACAccessUnits(raw)
	if err != nil {
		t.Fatalf("media.ParseAACAccessUnits() error = %v", err)
	}
	if got, want := len(aus), 2; got != want {
		t.Fatalf("len(aus) = %d, want %d", got, want)
	}
	if got, want := cfg.SampleRate, 16000; got != want {
		t.Fatalf("cfg.SampleRate = %d, want %d", got, want)
	}
	if got, want := int(cfg.ChannelConfig), 1; got != want {
		t.Fatalf("cfg.ChannelCount = %d, want %d", got, want)
	}
}

func TestTimestampUnwrapperWrapsForward(t *testing.T) {
	t.Parallel()

	var timestamps timestampUnwrapper
	timestamps.nowUnixMicro = func() int64 { return 0xfffffff0 }
	if got, want := timestamps.unwrap(0xfffffff0), uint64(0xfffffff0); got != want {
		t.Fatalf("first unwrap = %d, want %d", got, want)
	}
	if got, want := timestamps.unwrap(20), uint64(0x100000014); got != want {
		t.Fatalf("wrapped unwrap = %d, want %d", got, want)
	}
}

func TestRTPTimestampGuardClampsBackwardJitter(t *testing.T) {
	t.Parallel()

	var timestamps rtpTimestampGuard
	if got, want := timestamps.next(1_492_479), uint32(1_492_479); got != want {
		t.Fatalf("first next = %d, want %d", got, want)
	}
	if got, want := timestamps.next(1_487_732), uint32(1_492_480); got != want {
		t.Fatalf("backward next = %d, want %d", got, want)
	}
	if got, want := timestamps.next(1_492_480), uint32(1_492_481); got != want {
		t.Fatalf("equal next = %d, want %d", got, want)
	}
}

func TestRTPTimestampGuardAllowsForwardWrap(t *testing.T) {
	t.Parallel()

	var timestamps rtpTimestampGuard
	if got, want := timestamps.next(0xfffffff0), uint32(0xfffffff0); got != want {
		t.Fatalf("first next = %d, want %d", got, want)
	}
	if got, want := timestamps.next(20), uint32(20); got != want {
		t.Fatalf("wrapped next = %d, want %d", got, want)
	}
}

func TestRTPTimestampGuardClampsAudioRangeStart(t *testing.T) {
	t.Parallel()

	var timestamps rtpTimestampGuard
	pkts := []*rtp.Packet{{Header: rtp.Header{Timestamp: 0}}}

	if got, want := timestamps.applyBaseToPackets(pkts, 190_464, 1024), uint32(190_464); got != want {
		t.Fatalf("first base = %d, want %d", got, want)
	}
	if got, want := timestamps.applyBaseToPackets(pkts, 191_475, 1024), uint32(191_488); got != want {
		t.Fatalf("backward base = %d, want %d", got, want)
	}
	if got, want := timestamps.applyBaseToPackets(pkts, 192_512, 1024), uint32(192_512); got != want {
		t.Fatalf("equal-to-end base = %d, want %d", got, want)
	}
}

func TestRTPTimestampGuardShiftsAudioPacketBatch(t *testing.T) {
	t.Parallel()

	var timestamps rtpTimestampGuard
	pkts := []*rtp.Packet{
		{Header: rtp.Header{Timestamp: 0}},
		{Header: rtp.Header{Timestamp: 1024}},
	}

	if got, want := timestamps.applyBaseToPackets(pkts, 1000, 2048), uint32(1000); got != want {
		t.Fatalf("first base = %d, want %d", got, want)
	}
	if got, want := timestamps.applyBaseToPackets(pkts, 2000, 2048), uint32(3048); got != want {
		t.Fatalf("shifted base = %d, want %d", got, want)
	}
}

func TestAudioTimestampForPacketHasNoTimeBeforeFirstVideoFrame(t *testing.T) {
	t.Parallel()

	var timeline mediaTimeline

	got := audioTimestampForPacket(baichuan.MediaPacket{Kind: baichuan.MediaPacketAAC}, &timeline, 0, false)
	want := mediaTimestamp{}
	if got != want {
		t.Fatalf("audioTimestampForPacket() = %+v, want %+v", got, want)
	}
	if timeline.unwrapper.highest != 0 {
		t.Fatalf("timeline.unwrapper.highest = %d, want 0", timeline.unwrapper.highest)
	}
}

func TestAudioTimestampForPacketAnchorsToVideoClock(t *testing.T) {
	t.Parallel()

	var timeline mediaTimeline

	got := audioTimestampForPacket(baichuan.MediaPacket{Kind: baichuan.MediaPacketAAC}, &timeline, 138189977, true)
	want := mediaTimestamp{Microseconds: 138189977, Valid: true}
	if got != want {
		t.Fatalf("audioTimestampForPacket() = %+v, want %+v", got, want)
	}
	if got.Authoritative {
		t.Fatal("video-derived timestamp must not re-anchor every frame")
	}
}

// Both tracks must land on the same media time, otherwise a consumer has no
// way to line them up. Timestamp taken from a real capture of an E321.
func TestAudioAndVideoRTPDescribeTheSameMediaTime(t *testing.T) {
	t.Parallel()

	const videoUS = 138189977

	videoRTP := rtpTimestampForClock(videoUS, 90000)
	audioTS := audioTimestampForPacket(baichuan.MediaPacket{Kind: baichuan.MediaPacketAAC}, &mediaTimeline{}, videoUS, true)
	audioRTP, ok := rtpTimestampForMediaTime(audioTS, 16000)
	if !ok {
		t.Fatal("audio timestamp not resolved")
	}

	videoSeconds := float64(videoRTP) / 90000
	audioSeconds := float64(audioRTP) / 16000
	if diff := videoSeconds - audioSeconds; diff > 0.001 || diff < -0.001 {
		t.Fatalf("tracks drift apart: video %.6fs vs audio %.6fs", videoSeconds, audioSeconds)
	}
}

func TestAudioTimestampForPacketUsesAuthoritativePacketTimestamp(t *testing.T) {
	t.Parallel()

	var timeline mediaTimeline
	timeline.unwrapper.nowUnixMicro = func() int64 { return 1234 }
	packet := baichuan.MediaPacket{
		Kind:               baichuan.MediaPacketAAC,
		TimestampMicrosecs: 1234,
		HasTimestamp:       true,
	}

	// a packet with its own timestamp wins over the video anchor
	got := audioTimestampForPacket(packet, &timeline, 999999, true)
	want := mediaTimestamp{
		Microseconds:  1234,
		Valid:         true,
		Authoritative: true,
	}
	if got != want {
		t.Fatalf("audioTimestampForPacket() = %+v, want %+v", got, want)
	}
	if timeline.unwrapper.highest != 1234 {
		t.Fatalf("timeline.unwrapper.highest = %d, want 1234", timeline.unwrapper.highest)
	}
}

func TestCoalesceRejectsTruncatedNALUs(t *testing.T) {
	t.Parallel()

	fallback := []byte{0x67, 0x42, 0x00, 0x1f}

	// Test nil
	if got := coalesce(nil, fallback); !bytes.Equal(got, fallback) {
		t.Errorf("coalesce(nil) = %x, want %x", got, fallback)
	}

	// Test 1-byte truncated NALU
	truncated := []byte{0x67}
	if got := coalesce(truncated, fallback); !bytes.Equal(got, fallback) {
		t.Errorf("coalesce(truncated) = %x, want %x", got, fallback)
	}

	// Test valid NALU
	valid := []byte{0x67, 0x42, 0x00, 0x20}
	if got := coalesce(valid, fallback); !bytes.Equal(got, valid) {
		t.Errorf("coalesce(valid) = %x, want %x", got, valid)
	}
}

// A stream whose audio starts late used to stay mute for its whole life, and
// with hot mode that life is hours. Resetting lets the next setReady rebuild
// the session with the audio track.
func TestResetLetsTheSessionBeRebuiltWithAudio(t *testing.T) {
	t.Parallel()

	server := &gortsplib.Server{RTSPAddress: "127.0.0.1:0"}
	if err := server.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer server.Close()

	handler := newRTSPStreamHandler("cam/stream")
	handler.attachServer(server)

	video := &description.Media{
		Type:    description.MediaTypeVideo,
		Control: "trackID=0",
		Formats: []format.Format{&format.H264{PayloadTyp: 96, PacketizationMode: 1}},
	}
	audio := &description.Media{
		Type:    description.MediaTypeAudio,
		Control: "trackID=1",
		Formats: []format.Format{&format.G711{PayloadTyp: 8, SampleRate: 8000, ChannelCount: 1}},
	}

	if err := handler.setReady(video); err != nil {
		t.Fatalf("setReady() error = %v", err)
	}
	if got := len(handler.stream.Desc.Medias); got != 1 {
		t.Fatalf("video-only session has %d medias, want 1", got)
	}

	// setReady alone keeps the existing session, that is what made late audio permanent
	if err := handler.setReady(video, audio); err != nil {
		t.Fatalf("setReady() error = %v", err)
	}
	if got := len(handler.stream.Desc.Medias); got != 1 {
		t.Fatalf("session changed without a reset: %d medias", got)
	}

	handler.reset()
	if handler.ready() {
		t.Fatal("handler still ready after reset")
	}
	if err := handler.setReady(video, audio); err != nil {
		t.Fatalf("setReady() after reset error = %v", err)
	}
	if got := len(handler.stream.Desc.Medias); got != 2 {
		t.Fatalf("rebuilt session has %d medias, want video and audio", got)
	}
}

// A profile whose audio was seen once starts with the track declared, so the
// session never has to wait for the first packet or rebuild for it.
func TestDeclareAACBuildsTheTrackFromAHint(t *testing.T) {
	t.Parallel()

	publisher := &audioPublisher{log: NopLogger{}}
	if publisher.ready() {
		t.Fatal("publisher ready before anything was declared")
	}

	if err := publisher.declareAAC(16000, 1); err != nil {
		t.Fatalf("declareAAC() error = %v", err)
	}
	if !publisher.ready() {
		t.Fatal("declared publisher is not ready")
	}
	if got := publisher.hint(); got != (AudioHint{Codec: "aac", SampleRate: 16000, Channels: 1}) {
		t.Fatalf("hint() = %+v", got)
	}

	// the camera switching format must invalidate the declaration
	if !publisher.mismatchesDeclared(8000, 1) {
		t.Fatal("a different sample rate must count as a mismatch")
	}
	if publisher.mismatchesDeclared(16000, 1) {
		t.Fatal("the declared format must not count as a mismatch")
	}

	publisher.dropDeclaration()
	if publisher.ready() {
		t.Fatal("publisher still ready after the declaration was dropped")
	}
	if got := publisher.hint(); got != (AudioHint{}) {
		t.Fatalf("dropped publisher reports %+v, want silence", got)
	}
}
