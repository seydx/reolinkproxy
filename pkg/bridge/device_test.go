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
