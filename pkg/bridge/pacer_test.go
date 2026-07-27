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
