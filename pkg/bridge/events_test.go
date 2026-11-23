package bridge

import (
	"slices"
	"testing"

	"github.com/shareed2k/reolinkproxy/pkg/baichuan"
)

func TestHandleAlarmReportsTamperChanges(t *testing.T) {
	t.Parallel()

	var cam Camera
	var got []bool
	cam.OnTamper(func(tampered bool) { got = append(got, tampered) })
	for _, tamper := range []bool{false, true, true, false} {
		cam.handleAlarm(baichuan.AlarmState{Tamper: tamper})
	}
	if want := []bool{false, true, false}; !slices.Equal(got, want) {
		t.Fatalf("tamper reports = %v, want %v", got, want)
	}
}
