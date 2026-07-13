package bridge

import (
	"sync"
	"time"

	"github.com/bluenviron/gortsplib/v5"
)

type rtspSessionState struct {
	stream  *rtspStreamHandler
	playing bool
	talk    *rtspTalkSessionState
}

type cameraMotionSnapshot struct {
	Known       bool
	Active      bool
	Unsupported bool
	ChangedAt   time.Time
}

type cameraMotionState struct {
	mu       sync.RWMutex
	snapshot cameraMotionSnapshot
}

func newCameraMotionState() *cameraMotionState {
	return &cameraMotionState{
		snapshot: cameraMotionSnapshot{ChangedAt: time.Now()},
	}
}

func (s *cameraMotionState) snapshotCopy() cameraMotionSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot
}

func (s *cameraMotionState) setActive(active bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot.Known && s.snapshot.Active == active {
		return
	}

	s.snapshot.Known = true
	s.snapshot.Active = active
	s.snapshot.ChangedAt = time.Now()
}

func (s *cameraMotionState) markUnsupported() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.Unsupported = true
}

type streamPauseConfig struct {
	OnMotion bool
	OnClient bool
	Timeout  time.Duration
	Motion   *cameraMotionState
}

func (c CameraConfig) streamPauseConfig(motion *cameraMotionState) streamPauseConfig {
	return streamPauseConfig{
		OnMotion: c.PauseOnMotion,
		OnClient: c.PauseOnClient,
		Timeout:  c.PauseTimeout,
		Motion:   motion,
	}
}

func (p streamPauseConfig) shouldPause(now time.Time, handler *rtspStreamHandler) (bool, string) {
	if p.OnClient && handler != nil && !handler.hasClients() {
		return true, "no rtsp client"
	}

	if !p.OnMotion || p.Motion == nil {
		return false, ""
	}

	snapshot := p.Motion.snapshotCopy()
	if snapshot.Unsupported || !snapshot.Known || snapshot.Active {
		return false, ""
	}

	timeout := p.Timeout
	if timeout <= 0 {
		timeout = time.Second
	}
	if now.Sub(snapshot.ChangedAt) >= timeout {
		return true, "no motion"
	}

	return false, ""
}

func attachSessionState(session *gortsplib.ServerSession) *rtspSessionState {
	if session == nil {
		return nil
	}
	if state, ok := session.UserData().(*rtspSessionState); ok && state != nil {
		return state
	}

	state := &rtspSessionState{}
	session.SetUserData(state)
	return state
}

func attachSessionToStream(session *gortsplib.ServerSession, stream *rtspStreamHandler) *rtspSessionState {
	state := attachSessionState(session)
	if state != nil && stream != nil {
		state.stream = stream
	}
	return state
}
