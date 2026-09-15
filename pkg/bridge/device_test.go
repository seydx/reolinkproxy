package bridge

import (
	"context"
	"testing"
	"time"

	"github.com/shareed2k/reolinkproxy/pkg/baichuan"
)

func testDevice() *cameraDevice {
	return newCameraDevice("cam", baichuan.Config{Host: "127.0.0.1"}, time.Second, NopLogger{})
}

func TestAwaitPushOnlyConfirmsOnARealPush(t *testing.T) {
	t.Parallel()

	device := testDevice()
	if device.awaitPush(context.Background(), 20*time.Millisecond) {
		t.Fatal("silence was taken as proof that the camera reaches the webhook")
	}

	device.notePush()
	if !device.awaitPush(context.Background(), time.Second) {
		t.Fatal("a push that already arrived was not counted")
	}
}

func TestAwaitPushWakesOnAPushThatArrivesLate(t *testing.T) {
	t.Parallel()

	device := testDevice()
	go func() {
		time.Sleep(30 * time.Millisecond)
		device.notePush()
		device.notePush() // latching twice must not panic
	}()

	if !device.awaitPush(context.Background(), 2*time.Second) {
		t.Fatal("a push arriving during the wait was missed")
	}
}

func TestSessionEndsOnTheFirstPush(t *testing.T) {
	t.Parallel()

	device := testDevice()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sessionCtx, endSession := context.WithCancel(ctx)
	go device.endSessionWhenReachable(sessionCtx, endSession)

	select {
	case <-sessionCtx.Done():
		t.Fatal("the session ended before the camera proved anything")
	case <-time.After(30 * time.Millisecond):
	}

	device.notePush()
	select {
	case <-sessionCtx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("the session was kept open although the camera reached the webhook")
	}
}

func TestUIDFallbackReprobesTCPAfterTheInterval(t *testing.T) {
	t.Parallel()

	device := testDevice() // Host set, no UID preference yet

	// a transient TCP failure fell back to UID
	device.noteTransportLocked(true)
	if !device.cfg.PreferUID {
		t.Fatal("the UID fallback was not recorded")
	}
	if device.shouldReprobeTCPLocked() {
		t.Fatal("re-probed TCP immediately after falling back")
	}

	// the fallback ages past the interval, the next reconnect must retry TCP
	device.preferUIDSince = time.Now().Add(-uidReprobeInterval - time.Second)
	if !device.shouldReprobeTCPLocked() {
		t.Fatal("stayed on UID past the re-probe interval")
	}
}

func TestTCPRecoveryLeavesTheUIDFallback(t *testing.T) {
	t.Parallel()

	device := testDevice()
	device.noteTransportLocked(true)

	// TCP answered again on a later connection
	device.noteTransportLocked(false)
	if device.cfg.PreferUID {
		t.Fatal("kept preferring UID after TCP came back")
	}
}

func TestFrequentUIDReconnectsDoNotResetTheReprobeClock(t *testing.T) {
	t.Parallel()

	device := testDevice()
	device.noteTransportLocked(true)
	device.preferUIDSince = time.Now().Add(-uidReprobeInterval + 30*time.Second)
	stamped := device.preferUIDSince

	// reconnecting over UID while already preferring it must not push the clock
	for i := 0; i < 5; i++ {
		device.noteTransportLocked(true)
	}
	if !device.preferUIDSince.Equal(stamped) {
		t.Fatal("a plain UID reconnect reset the re-probe clock, so a flapping camera never retries TCP")
	}
}

func TestPureUIDCameraNeverReprobesTCP(t *testing.T) {
	t.Parallel()

	device := newCameraDevice("cam", baichuan.Config{UID: "abc"}, time.Second, NopLogger{})
	device.noteTransportLocked(true)
	device.preferUIDSince = time.Now().Add(-uidReprobeInterval - time.Hour)
	if device.shouldReprobeTCPLocked() {
		t.Fatal("a camera without a Host has no TCP path to re-probe")
	}
}
