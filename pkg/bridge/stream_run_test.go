package bridge

import "testing"

func testTimeline() *mediaTimeline {
	return &mediaTimeline{unwrapper: timestampUnwrapper{nowUnixMicro: func() int64 { return 0 }}}
}

func TestTimelineTrustsSteadyDeltas(t *testing.T) {
	tl := testTimeline()
	base := tl.video(1_000_000)
	for i := uint32(1); i <= 5; i++ {
		got := tl.video(1_000_000 + i*50_000)
		if want := base + uint64(i)*50_000; got != want {
			t.Fatalf("frame %d: got %d, want %d", i, got, want)
		}
	}
}

func TestTimelineClampsForwardJumpAndKeepsAudioAligned(t *testing.T) {
	tl := testTimeline()
	tl.video(1_000_000)
	last := tl.video(1_050_000)
	if got := tl.video(1_100_000); got != last+50_000 {
		t.Fatalf("steady delta changed: got %d", got)
	}

	jumped := tl.video(31_100_000)
	if jumped != last+100_000 {
		t.Fatalf("jump not clamped to avg delta: got %d, want %d", jumped, last+100_000)
	}

	audio := tl.audio(31_150_000)
	if audio != jumped+50_000 {
		t.Fatalf("audio not on corrected timeline: got %d, want %d", audio, jumped+50_000)
	}
}

func TestTimelineClampsBackwardJump(t *testing.T) {
	tl := testTimeline()
	tl.video(1_000_000)
	last := tl.video(1_040_000)

	stepped := tl.video(500_000)
	if stepped != last+40_000 {
		t.Fatalf("backward jump not clamped: got %d, want %d", stepped, last+40_000)
	}
	if next := tl.video(540_000); next != stepped+40_000 {
		t.Fatalf("timeline broken after backward clamp: got %d, want %d", next, stepped+40_000)
	}
}

func TestTimelineFallbackDeltaBeforeAverage(t *testing.T) {
	tl := testTimeline()
	first := tl.video(1_000_000)
	if got := tl.video(10_000_000); got != first+fallbackVideoDeltaUS {
		t.Fatalf("fallback delta not used: got %d, want %d", got, first+fallbackVideoDeltaUS)
	}
}
