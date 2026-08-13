package bridge

import (
	"testing"
	"time"
)

func TestLiveEdgeKeepsASteadyStreamFlowing(t *testing.T) {
	live := liveEdge{maxLag: defaultLiveCatchUp}
	start := time.Unix(1000, 0)

	for i := range 100 {
		at := start.Add(time.Duration(i) * 50 * time.Millisecond)
		mediaUS := uint64(i) * 50_000
		if live.behind(mediaUS, i%20 == 0, at) {
			t.Fatalf("frame %d dropped from a stream that is on time", i)
		}
	}
}

func TestLiveEdgeSkipsBacklogUntilKeyframe(t *testing.T) {
	live := liveEdge{maxLag: defaultLiveCatchUp}
	start := time.Unix(1000, 0)
	live.behind(0, true, start)

	// the camera went quiet for 10s, then dumps its backlog in one burst
	burst := start.Add(10 * time.Second)
	if !live.behind(50_000, false, burst) {
		t.Fatal("backlogged frame was passed on instead of dropped")
	}
	for i := 2; i < 20; i++ {
		if !live.behind(uint64(i)*50_000, false, burst) {
			t.Fatalf("frame %d of the backlog was not dropped", i)
		}
	}

	if live.behind(20*50_000, true, burst) {
		t.Fatal("keyframe must end the catch-up")
	}
	if live.catchingUp {
		t.Fatal("still catching up after the keyframe")
	}
	if live.behind(21*50_000, false, burst.Add(50*time.Millisecond)) {
		t.Fatal("frame after the resync was dropped")
	}
}

// A camera that stops sending for a while and then resumes with current
// pictures is live again, even though its clock jumped. Only a camera handing
// us old pictures is actually behind, and the two are told apart by whether
// the camera clock moved with the wall clock.
func TestLiveEdgeIgnoresACameraGapButCatchesUpOnABacklog(t *testing.T) {
	start := time.Unix(1000, 0)

	gap := testTimeline()
	gapLive := liveEdge{maxLag: defaultLiveCatchUp}
	_, raw := gap.video(1_000_000)
	gapLive.behind(raw, true, start)

	// 5s of silence, then the camera continues where its clock now stands
	_, raw = gap.video(6_066_000)
	if gapLive.behind(raw, false, start.Add(5*time.Second)) {
		t.Fatal("dropped a live frame after a camera gap")
	}

	backlog := testTimeline()
	backlogLive := liveEdge{maxLag: defaultLiveCatchUp}
	_, raw = backlog.video(1_000_000)
	backlogLive.behind(raw, true, start)

	// 5s of silence, then the camera delivers what it buffered meanwhile
	_, raw = backlog.video(1_066_000)
	if !backlogLive.behind(raw, false, start.Add(5*time.Second)) {
		t.Fatal("kept a stale frame instead of catching up")
	}
}

func TestLiveEdgeToleratesShortStalls(t *testing.T) {
	live := liveEdge{maxLag: defaultLiveCatchUp}
	start := time.Unix(1000, 0)
	live.behind(0, true, start)

	// a 2s hiccup stays under the threshold, nothing is dropped
	late := start.Add(2 * time.Second)
	if live.behind(50_000, false, late) {
		t.Fatal("a short stall must not drop frames")
	}
}

func testTimeline() *mediaTimeline {
	return &mediaTimeline{unwrapper: timestampUnwrapper{nowUnixMicro: func() int64 { return 0 }}}
}

func TestTimelineTrustsSteadyDeltas(t *testing.T) {
	tl := testTimeline()
	base, _ := tl.video(1_000_000)
	for i := uint32(1); i <= 5; i++ {
		got, _ := tl.video(1_000_000 + i*50_000)
		if want := base + uint64(i)*50_000; got != want {
			t.Fatalf("frame %d: got %d, want %d", i, got, want)
		}
	}
}

func TestTimelineClampsForwardJumpAndKeepsAudioAligned(t *testing.T) {
	tl := testTimeline()
	tl.video(1_000_000)
	last, _ := tl.video(1_050_000)
	if got, _ := tl.video(1_100_000); got != last+50_000 {
		t.Fatalf("steady delta changed: got %d", got)
	}

	jumped, _ := tl.video(31_100_000)
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
	last, _ := tl.video(1_040_000)

	stepped, _ := tl.video(500_000)
	if stepped != last+40_000 {
		t.Fatalf("backward jump not clamped: got %d, want %d", stepped, last+40_000)
	}
	if next, _ := tl.video(540_000); next != stepped+40_000 {
		t.Fatalf("timeline broken after backward clamp: got %d, want %d", next, stepped+40_000)
	}
}

func TestTimelineFallbackDeltaBeforeAverage(t *testing.T) {
	tl := testTimeline()
	first, _ := tl.video(1_000_000)
	if got, _ := tl.video(10_000_000); got != first+fallbackVideoDeltaUS {
		t.Fatalf("fallback delta not used: got %d, want %d", got, first+fallbackVideoDeltaUS)
	}
}

func TestLiveEdgeOffPassesEverythingOn(t *testing.T) {
	live := liveEdge{}
	start := time.Unix(1000, 0)
	live.behind(0, true, start)

	if live.behind(50_000, false, start.Add(time.Minute)) {
		t.Fatal("dropped a frame although catching up is switched off")
	}
}
