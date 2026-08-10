package bridge

import (
	"testing"
	"time"
)

func TestMediaOffsetClampsNonsenseTimestamps(t *testing.T) {
	t.Parallel()

	if got := mediaOffset(1_500_000, 1_000_000); got != 500*time.Millisecond {
		t.Fatalf("mediaOffset() = %v, want 500ms", got)
	}
	if got := mediaOffset(1_000_000, 1_500_000); got != -500*time.Millisecond {
		t.Fatalf("mediaOffset() = %v, want -500ms", got)
	}
	// whatever the timestamp, the offset stays inside the limit so the
	// duration arithmetic downstream cannot overflow
	for _, mediaUS := range []uint64{1 << 62, 1 << 63, ^uint64(0)} {
		if got := mediaOffset(mediaUS, 0); got > pacerSkewLimit || got < -pacerSkewLimit {
			t.Fatalf("mediaOffset(%d) = %v, outside ±%v", mediaUS, got, pacerSkewLimit)
		}
	}
}

// Audio and video run through one cursor, so the wall-clock gap between two
// frames must mirror the media-time gap the camera sent them with. With a
// cursor per track this is exactly what drifted apart. Timestamps are the real
// interleaving of an E321: video every ~66.6ms, one AAC frame in between.
func TestPaceCursorKeepsTracksOnOneSchedule(t *testing.T) {
	t.Parallel()

	cursor := paceCursor{maxLead: 3 * time.Second}
	now := time.Unix(1000, 0)

	video1 := cursor.schedule(138189977, now)
	audio1 := cursor.schedule(138189977+33_000, now)
	video2 := cursor.schedule(138256598, now)

	if got := audio1.Sub(video1); got != 33*time.Millisecond {
		t.Fatalf("audio sits %v after video, want 33ms", got)
	}
	if got := video2.Sub(video1); got != 66621*time.Microsecond {
		t.Fatalf("video spacing %v, want 66.621ms", got)
	}
}

func TestPaceCursorAppliesInitialLatencyOnce(t *testing.T) {
	t.Parallel()

	cursor := paceCursor{maxLead: 3 * time.Second, initialLatency: 1500 * time.Millisecond}
	now := time.Unix(1000, 0)

	first := cursor.schedule(1_000_000, now)
	second := cursor.schedule(1_040_000, now)

	if got := first.Sub(now); got != 1500*time.Millisecond {
		t.Fatalf("first frame scheduled %v out, want the initial latency", got)
	}
	if got := second.Sub(first); got != 40*time.Millisecond {
		t.Fatalf("second frame %v after the first, want its media gap of 40ms", got)
	}
}

func TestPaceCursorReanchorsOnClockJump(t *testing.T) {
	t.Parallel()

	cursor := paceCursor{maxLead: 3 * time.Second}
	now := time.Unix(1000, 0)
	cursor.schedule(1_000_000, now)

	// camera clock jumps a minute ahead: emit now and start a fresh anchor
	jumped := cursor.schedule(61_000_000, now)
	if !jumped.Equal(now) {
		t.Fatalf("jumped frame scheduled at %v, want now", jumped)
	}

	next := cursor.schedule(61_040_000, now)
	if got := next.Sub(now); got != 40*time.Millisecond {
		t.Fatalf("frame after the jump %v out, want 40ms from the new anchor", got)
	}
}

// A frame whose target has passed goes out immediately, which is how a burst
// drains, but the anchor must survive so the following frames stay paced.
func TestPaceCursorDrainsLateFramesWithoutLosingTheAnchor(t *testing.T) {
	t.Parallel()

	cursor := paceCursor{maxLead: 3 * time.Second}
	start := time.Unix(1000, 0)
	first := cursor.schedule(1_000_000, start)

	late := start.Add(time.Second)
	target := cursor.schedule(1_500_000, late)
	if !target.Before(late) {
		t.Fatal("a frame whose media time already passed must not be delayed")
	}
	if got := target.Sub(first); got != 500*time.Millisecond {
		t.Fatalf("target moved to %v after the first frame, want its media gap of 500ms", got)
	}
}

func testPacer(capacity int) *mediaPacer {
	return &mediaPacer{ch: make(chan pacedFrame, capacity), log: NopLogger{}}
}

func videoFrame(mediaUS uint64, keyframe bool) pacedFrame {
	return pacedFrame{mediaUS: mediaUS, keyframe: keyframe}
}

func audioFrame(mediaUS uint64) pacedFrame {
	return pacedFrame{mediaUS: mediaUS, audio: true}
}

// A full queue means the writer cannot keep up. Everything queued is stale;
// forwarding single survivors mid-GOP would hand every reader broken
// references, so the backlog is flushed and video stays off until a keyframe.
func TestPacerOverflowFlushesBacklogAndWaitsForKeyframe(t *testing.T) {
	t.Parallel()

	p := testPacer(4)
	for i := range 4 {
		p.enqueue(videoFrame(uint64(i), i == 0))
	}
	// overflow with a P-frame: flush everything, mute video
	p.enqueue(videoFrame(4, false))
	if len(p.ch) != 0 {
		t.Fatalf("queue holds %d frames after overflow, want 0", len(p.ch))
	}

	// P-frames stay muted, a keyframe resumes
	p.enqueue(videoFrame(5, false))
	if len(p.ch) != 0 {
		t.Fatal("P-frame was queued while waiting for a keyframe")
	}
	p.enqueue(videoFrame(6, true))
	if len(p.ch) != 1 {
		t.Fatalf("queue holds %d frames after the keyframe, want 1", len(p.ch))
	}
	got := <-p.ch
	if got.mediaUS != 6 || !got.keyframe {
		t.Fatalf("resumed with frame %d (keyframe=%t), want the keyframe 6", got.mediaUS, got.keyframe)
	}
}

// An overflow hit by a keyframe itself needs no waiting: the flushed queue has
// room and the keyframe starts a clean sequence right away.
func TestPacerOverflowOnKeyframeResumesImmediately(t *testing.T) {
	t.Parallel()

	p := testPacer(2)
	p.enqueue(videoFrame(0, true))
	p.enqueue(videoFrame(1, false))
	p.enqueue(videoFrame(2, true))

	if len(p.ch) != 1 {
		t.Fatalf("queue holds %d frames, want just the new keyframe", len(p.ch))
	}
	if got := <-p.ch; got.mediaUS != 2 {
		t.Fatalf("queued frame %d, want the keyframe 2", got.mediaUS)
	}
	if p.awaitKeyframe {
		t.Fatal("pacer still waits for a keyframe")
	}
}

// Audio frames are independently decodable, so they keep flowing while video
// waits for its keyframe.
func TestPacerKeepsAudioFlowingWhileVideoWaits(t *testing.T) {
	t.Parallel()

	p := testPacer(2)
	p.enqueue(videoFrame(0, true))
	p.enqueue(videoFrame(1, false))
	p.enqueue(videoFrame(2, false)) // overflow, video muted

	p.enqueue(audioFrame(3))
	if len(p.ch) != 1 {
		t.Fatalf("queue holds %d frames, want the audio frame", len(p.ch))
	}
	if !p.awaitKeyframe {
		t.Fatal("audio must not end the wait for a video keyframe")
	}
}
