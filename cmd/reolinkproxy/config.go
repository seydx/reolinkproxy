package main

import (
	"fmt"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/shareed2k/reolinkproxy/pkg/bridge"
)

// envCameraConfig mirrors the REOLINK_CAMERA_<n>_* env layout. The yaml tag
// (uppercased) is the env field name.
type envCameraConfig struct {
	Name           string        `yaml:"name"`
	Host           string        `yaml:"host"`
	Port           int           `yaml:"port"`
	UID            string        `yaml:"uid"`
	Username       string        `yaml:"username"`
	Password       string        `yaml:"password"`
	Timeout        time.Duration `yaml:"timeout"`
	Stream         string        `yaml:"stream"`
	Channel        int           `yaml:"channel"`
	RTSPPath       string        `yaml:"rtsp_path"`
	TalkProfile    string        `yaml:"talk_profile"`
	TalkVolume     int           `yaml:"talk_volume"`
	PauseOnMotion  bool          `yaml:"pause_on_motion"`
	PauseOnClient  bool          `yaml:"pause_on_client"`
	PauseTimeout   time.Duration `yaml:"pause_timeout"`
	IdleDisconnect bool          `yaml:"idle_disconnect"`
	IdleTimeout    time.Duration `yaml:"idle_timeout"`
	BatteryCamera  bool          `yaml:"battery_camera"`
}

var (
	cameraEnvKeyRE   = regexp.MustCompile(`^REOLINK_CAMERA_(\d+)_([A-Z0-9_]+)$`)
	cameraConfigType = reflect.TypeOf(envCameraConfig{})
	durationType     = reflect.TypeOf(time.Duration(0))
)

func loadCamerasFromEnv() ([]bridge.CameraConfig, error) {
	return loadCamerasFromEntries(os.Environ())
}

func loadCamerasFromEntries(entries []string) ([]bridge.CameraConfig, error) {
	fieldIndexes := cameraEnvFieldIndexes()
	camerasByIndex := make(map[int]*envCameraConfig)

	for _, entry := range entries {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}

		matches := cameraEnvKeyRE.FindStringSubmatch(key)
		if len(matches) != 3 {
			continue
		}

		cameraIndex, err := strconv.Atoi(matches[1])
		if err != nil {
			return nil, fmt.Errorf("%s: invalid camera index: %w", key, err)
		}

		fieldIndex, found := fieldIndexes[matches[2]]
		if !found {
			continue
		}

		camera := camerasByIndex[cameraIndex]
		if camera == nil {
			camera = &envCameraConfig{}
			camerasByIndex[cameraIndex] = camera
		}

		field := reflect.ValueOf(camera).Elem().Field(fieldIndex)
		if err := setFieldFromEnv(field, value, key); err != nil {
			return nil, err
		}
	}

	if len(camerasByIndex) == 0 {
		return nil, nil
	}

	indexes := make([]int, 0, len(camerasByIndex))
	for cameraIndex := range camerasByIndex {
		indexes = append(indexes, cameraIndex)
	}
	sort.Ints(indexes)

	cameras := make([]bridge.CameraConfig, 0, len(indexes))
	for _, cameraIndex := range indexes {
		camera := camerasByIndex[cameraIndex].toBridgeConfig()
		camera.ApplyDefaults()
		if err := camera.Validate(); err != nil {
			return nil, fmt.Errorf("REOLINK_CAMERA_%d_*: %w", cameraIndex, err)
		}
		cameras = append(cameras, camera)
	}

	return cameras, nil
}

func (c *envCameraConfig) toBridgeConfig() bridge.CameraConfig {
	return bridge.CameraConfig{
		Name:           c.Name,
		Host:           c.Host,
		Port:           c.Port,
		UID:            c.UID,
		Username:       c.Username,
		Password:       c.Password,
		Timeout:        c.Timeout,
		Streams:        splitCameraStreams(c.Stream),
		Channel:        c.Channel,
		RTSPPath:       c.RTSPPath,
		TalkProfile:    c.TalkProfile,
		TalkVolume:     c.TalkVolume,
		PauseOnMotion:  c.PauseOnMotion,
		PauseOnClient:  c.PauseOnClient,
		PauseTimeout:   c.PauseTimeout,
		IdleDisconnect: c.IdleDisconnect,
		IdleTimeout:    c.IdleTimeout,
		BatteryCamera:  c.BatteryCamera,
	}
}

func splitCameraStreams(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		name := strings.ToLower(strings.TrimSpace(part))
		if name == "" {
			continue
		}
		out = append(out, name)
	}
	return out
}

func cameraEnvFieldIndexes() map[string]int {
	out := make(map[string]int, cameraConfigType.NumField())

	for i := range cameraConfigType.NumField() {
		tag := strings.Split(cameraConfigType.Field(i).Tag.Get("yaml"), ",")[0]
		if tag == "" || tag == "-" {
			continue
		}
		out[strings.ToUpper(tag)] = i
	}

	return out
}

func setFieldFromEnv(field reflect.Value, rawValue string, envKey string) error {
	if field.Type() == durationType {
		duration, err := time.ParseDuration(rawValue)
		if err != nil {
			return fmt.Errorf("%s: invalid duration %q", envKey, rawValue)
		}
		field.SetInt(int64(duration))
		return nil
	}

	switch field.Kind() {
	case reflect.String:
		field.SetString(rawValue)
	case reflect.Bool:
		value, err := strconv.ParseBool(rawValue)
		if err != nil {
			return fmt.Errorf("%s: invalid bool %q", envKey, rawValue)
		}
		field.SetBool(value)
	case reflect.Int:
		value, err := strconv.Atoi(rawValue)
		if err != nil {
			return fmt.Errorf("%s: invalid int %q", envKey, rawValue)
		}
		field.SetInt(int64(value))
	default:
		return fmt.Errorf("%s: unsupported field type %s", envKey, field.Type())
	}

	return nil
}
