package bridge

import (
	"context"

	"github.com/shareed2k/reolinkproxy/pkg/baichuan"
)

// Battery queries the current battery state. Mains-powered cameras return an
// error.
func (c *Camera) Battery(ctx context.Context) (BatteryState, error) {
	var state BatteryState
	err := c.device.WithClient(ctx, func(client *baichuan.Client) error {
		info, err := client.GetBattery(ctx, c.channel())
		if err != nil {
			return err
		}
		state = batteryStateFromInfo(*info)
		return nil
	})
	return state, err
}

// Snapshot requests an on-demand JPEG directly from the camera (no stream
// start required). Bound the wait with a context deadline — a sleeping
// battery camera first has to wake up.
func (c *Camera) Snapshot(ctx context.Context) ([]byte, error) {
	var img []byte
	err := c.device.WithClient(ctx, func(client *baichuan.Client) error {
		var err error
		img, err = client.Snap(ctx, c.channel())
		return err
	})
	return img, err
}

// SetSiren switches the camera siren on or off.
func (c *Camera) SetSiren(ctx context.Context, on bool) error {
	return c.device.WithClient(ctx, func(client *baichuan.Client) error {
		return client.Siren(ctx, c.channel(), boolToInt(on))
	})
}

// SetWhiteLed switches the white spotlight LED on or off.
func (c *Camera) SetWhiteLed(ctx context.Context, on bool) error {
	return c.device.WithClient(ctx, func(client *baichuan.Client) error {
		return client.SetWhiteLed(ctx, c.channel(), boolToInt(on))
	})
}

// PTZ sends a pan/tilt/zoom command ("left", "right", "up", "down",
// "leftUp", …, "zoomInc", "zoomDec", "stop") at the given speed.
func (c *Camera) PTZ(ctx context.Context, command string, speed int) error {
	return c.device.WithClient(ctx, func(client *baichuan.Client) error {
		return client.PTZControl(ctx, c.channel(), command, speed)
	})
}

// PTZPreset moves the camera to a stored PTZ preset.
func (c *Camera) PTZPreset(ctx context.Context, presetID int) error {
	return c.device.WithClient(ctx, func(client *baichuan.Client) error {
		return client.PTZPreset(ctx, c.channel(), presetID)
	})
}

// PTZPresets lists the PTZ positions stored on the camera.
func (c *Camera) PTZPresets(ctx context.Context) ([]baichuan.PTZPreset, error) {
	var presets []baichuan.PTZPreset
	err := c.device.WithClient(ctx, func(client *baichuan.Client) error {
		var err error
		presets, err = client.GetPTZPresets(ctx, c.channel())
		return err
	})
	return presets, err
}

// PlayQuickReply plays a stored quick-reply audio file (doorbells).
func (c *Camera) PlayQuickReply(ctx context.Context, fileID int) error {
	return c.device.WithClient(ctx, func(client *baichuan.Client) error {
		return client.PlayQuickReply(ctx, c.channel(), fileID)
	})
}

// Reboot restarts the camera.
func (c *Camera) Reboot(ctx context.Context) error {
	return c.device.WithClient(ctx, func(client *baichuan.Client) error {
		return client.Reboot(ctx, c.channel())
	})
}

func (c *Camera) channel() uint8 {
	return uint8(c.cfg.Channel) //#nosec G115
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
