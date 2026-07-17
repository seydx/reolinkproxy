package baichuan

import (
	"context"
	"encoding/xml"
	"fmt"
)

// execCommand sends a channel-scoped XML command: header channel is 1-based
// (reolink_aio semantics) and the target channel rides in the extension XML.
func (c *Client) execCommand(ctx context.Context, msgID uint32, channel uint8, body any) (*Message, error) {
	return c.exec(ctx, msgID, headerChannelID(channel), channelExtension(channel), body)
}

// execHostCommand sends a device-scoped XML command on the host channel (250).
// NVRs/HomeHubs reject host-level commands addressed to a stream channel.
func (c *Client) execHostCommand(ctx context.Context, msgID uint32, body any) (*Message, error) {
	return c.exec(ctx, msgID, channelIDHost, nil, body)
}

func (c *Client) exec(ctx context.Context, msgID uint32, headerChannel uint8, extension []byte, body any) (*Message, error) {
	if err := c.Login(ctx); err != nil {
		return nil, err
	}

	var bodyBytes []byte
	var err error
	if body != nil {
		bodyBytes, err = marshalXMLDocument(body)
		if err != nil {
			return nil, fmt.Errorf("marshal xml: %w", err)
		}
	}

	resp, err := c.sendRequest(ctx, request{
		MsgID:     msgID,
		ChannelID: headerChannel,
		Class:     classModernWithOffset,
		Extension: extension,
		Body:      bodyBytes,
	})
	if err != nil {
		return nil, err
	}

	if err := resp.success(); err != nil {
		return nil, err
	}

	return resp, nil
}

// Siren triggers the camera's internal siren alarm to sound continuously (manual mode).
func (c *Client) Siren(ctx context.Context, channel uint8, enable int) error {
	_, err := c.execCommand(ctx, msgIDPlayAudio, channel, xmlAudioPlayInfoBody{
		AudioPlayInfo: xmlAudioPlayInfo{Version: "1.1", ChannelID: channel, PlayMode: 2, PlayDuration: 10, PlayTimes: 1, OnOff: enable},
	})
	return err
}

// SirenTimes triggers the camera's internal siren alarm to sound for a specific number of times.
func (c *Client) SirenTimes(ctx context.Context, channel uint8, times int) error {
	_, err := c.execCommand(ctx, msgIDPlayAudio, channel, xmlAudioPlayInfoBody{
		AudioPlayInfo: xmlAudioPlayInfo{Version: "1.1", ChannelID: channel, PlayMode: 0, PlayDuration: 10, PlayTimes: times, OnOff: 1},
	})
	return err
}

// SirenHub triggers the Hub's internal siren alarm to sound continuously (manual mode).
func (c *Client) SirenHub(ctx context.Context, enable int) error {
	_, err := c.execCommand(ctx, msgIDPlayAudio, 0, xmlAudioPlayInfoBody{
		AudioPlayInfo: xmlAudioPlayInfo{Version: "1.1", PlayMode: 2, PlayDuration: 10, PlayTimes: 1, OnOff: enable},
	})
	return err
}

// SirenHubTimes triggers the Hub's internal siren alarm to sound for a specific number of times.
func (c *Client) SirenHubTimes(ctx context.Context, times int) error {
	_, err := c.execCommand(ctx, msgIDPlayAudio, 0, xmlAudioPlayInfoBody{
		AudioPlayInfo: xmlAudioPlayInfo{Version: "1.1", PlayMode: 0, PlayDuration: 10, PlayTimes: times, OnOff: 1},
	})
	return err
}

// SetWhiteLed enables or disables the white LED (floodlight) via the manual
// floodlight control (msg 288; msg 290 is the scheduled-tasks config and
// rejects direct switching).
func (c *Client) SetWhiteLed(ctx context.Context, channel uint8, status int) error {
	body, err := marshalXMLDocument(xmlFloodlightManualBody{
		FloodlightManual: xmlFloodlightManual{Version: "1", ChannelID: channel, Status: status, Duration: 180},
	})
	if err != nil {
		return fmt.Errorf("marshal xml: %w", err)
	}

	resp, err := c.sendRequest(ctx, request{
		MsgID:     msgIDFloodlightManual,
		ChannelID: channel,
		Class:     classModernWithOffset,
		Extension: channelExtension(channel),
		Body:      body,
	})
	if err != nil {
		return err
	}
	return resp.success()
}

// GetWhiteLed retrieves the current state of the floodlight.
func (c *Client) GetWhiteLed(ctx context.Context, channel uint8) (*Message, error) {
	return c.execCommand(ctx, msgIDWhiteLedGet, channel, nil)
}

// SetPrivacyMode puts the camera into privacy/sleep mode.
func (c *Client) SetPrivacyMode(ctx context.Context, channel uint8, enable int) error {
	_, err := c.execCommand(ctx, msgIDPrivacyModeSet, channel, xmlSleepStateBody{
		SleepState: xmlSleepState{Version: "1.1", Operate: 2, Sleep: enable},
	})
	return err
}

// GetPrivacyMode retrieves the current privacy/sleep mode state.
func (c *Client) GetPrivacyMode(ctx context.Context, channel uint8) (*Message, error) {
	return c.execCommand(ctx, msgIDPrivacyModeGet, channel, nil)
}

// SetAutoFocus enables or disables auto-focus on supported cameras.
func (c *Client) SetAutoFocus(ctx context.Context, channel uint8, disable int) error {
	_, err := c.execCommand(ctx, msgIDAutoFocusSet, channel, xmlAutoFocusBody{
		AutoFocus: xmlAutoFocus{Version: "1.1", ChannelID: channel, Disable: disable},
	})
	return err
}

// GetAutoFocus retrieves the current state of auto-focus.
func (c *Client) GetAutoFocus(ctx context.Context, channel uint8) (*Message, error) {
	return c.execCommand(ctx, msgIDAutoFocusGet, channel, nil)
}

// RingChimeWithTone rings the chime using a specific tone
func (c *Client) RingChimeWithTone(ctx context.Context, channel uint8, chimeID int, toneID int) error {
	_, err := c.execCommand(ctx, msgIDDingDongOpt2, channel, xmlDingDongOptBody{
		DingdongDeviceOpt: xmlDingDongOpt{Version: "1.1", Opt: "ringWithMusic", ID: chimeID, MusicID: toneID},
	})
	return err
}

// GetChimeConfig retrieves the configuration of a paired chime
func (c *Client) GetChimeConfig(ctx context.Context, channel uint8) (*Message, error) {
	return c.execCommand(ctx, msgIDDingDongGet, channel, nil)
}

// SetChimeSilentMode sets the silent mode (DND) on the chime for a specific duration (in minutes)
func (c *Client) SetChimeSilentMode(ctx context.Context, channel uint8, chimeID int, time int) error {
	_, err := c.execCommand(ctx, msgIDDingDongSilentSet, channel, xmlDingDongSilentBody{
		DingdongSilentMode: xmlDingDongSilentMode{Version: "1.1", ID: chimeID, Time: time, Type: 63},
	})
	return err
}

// PlayQuickReply plays a pre-recorded audio file on the camera's speaker.
func (c *Client) PlayQuickReply(ctx context.Context, channel uint8, fileID int) error {
	_, err := c.execCommand(ctx, msgIDQuickReplyPlay, channel, xmlAudioFileInfoBody{
		AudioFileInfo: xmlAudioFileInfo{Version: "1.1", ChannelID: channel, ID: fileID, Timeout: 0},
	})
	return err
}

// PTZControl sends a raw PTZ command to the camera.
func (c *Client) PTZControl(ctx context.Context, channel uint8, command string, speed int) error {
	if speed == 0 {
		speed = 32
	}
	_, err := c.execCommand(ctx, msgIDPTZControl, channel, xmlPtzControlBody{
		PtzControl: xmlPtzControl{Version: "1.1", ChannelID: channel, Command: command, Speed: speed},
	})
	return err
}

// PTZPreset moves the camera to a saved PTZ preset ID.
func (c *Client) PTZPreset(ctx context.Context, channel uint8, presetID int) error {
	_, err := c.execCommand(ctx, msgIDPTZControlPreset, channel, xmlPtzPresetBody{
		PtzPreset: xmlPtzPreset{Version: "1.1", ChannelID: channel, PresetList: xmlPtzPresetList{Preset: xmlPtzPresetItem{ID: presetID, Command: "toPos"}}},
	})
	return err
}

// PtzGuard sets the guard position or patrol for a PTZ camera.
func (c *Client) PtzGuard(ctx context.Context, channel uint8, enable int, cmdStr string, timeout int, setPos int) error {
	_, err := c.execCommand(ctx, msgIDPtzGuardSet, channel, xmlPtzGuardBody{
		PtzGuard: xmlPtzGuard{Version: "1.1", ChannelID: channel, Benable: enable, Command: cmdStr, Timeout: timeout, NeedSetPos: setPos},
	})
	return err
}

// Ptz3DLocation zooms or centers the camera onto a specific 3D box region.
func (c *Client) Ptz3DLocation(ctx context.Context, channel uint8, posX, posY, posWidth, posHeight, speed, width, height int) error {
	_, err := c.execCommand(ctx, msgIDPtz3DLocation, channel, xmlPtz3DLocationBody{
		Ptz3DLocation: xmlPtz3DLocation{Version: "1.1", ChannelID: channel, PosX: posX, PosY: posY, PosWidth: posWidth, PosHeight: posHeight, Speed: speed, Width: width, Height: height},
	})
	return err
}

// Reboot sends a reboot command to the camera channel.
func (c *Client) Reboot(ctx context.Context, channel uint8) error {
	_, err := c.execCommand(ctx, msgIDReboot, channel, xmlRebootBody{
		Reboot: xmlReboot{Channel: channel},
	})
	return err
}

// RawCommand sends an arbitrary Baichuan command and returns the reply XML.
// A debugging/exploration helper for probing firmware behavior; body may be
// empty for query-style commands.
func (c *Client) RawCommand(ctx context.Context, msgID uint32, channel uint8, body string) (string, error) {
	if err := c.Login(ctx); err != nil {
		return "", err
	}

	var bodyBytes []byte
	if body != "" {
		bodyBytes = append([]byte(xml.Header), []byte(body)...)
	}

	resp, err := c.sendRequest(ctx, request{
		MsgID:     msgID,
		ChannelID: channel,
		Class:     classModernWithOffset,
		Body:      bodyBytes,
	})
	if err != nil {
		return "", err
	}

	return resp.XML, nil
}

// DevInfo describes the device as reported by GetDevInfo (msg 80).
type DevInfo struct {
	Name            string `xml:"name"`
	Type            string `xml:"type"`
	SerialNumber    string `xml:"serialNumber"`
	HardwareVersion string `xml:"hardwareVersion"`
	FirmwareVersion string `xml:"firmwareVersion"`
	ItemNo          string `xml:"itemNo"`
	Detail          string `xml:"detail"`
}

type devInfoMessage struct {
	DevInfo *DevInfo `xml:"DevInfo"`
	// VersionInfo is the alternative payload name used by some firmwares.
	VersionInfo *DevInfo `xml:"VersionInfo"`
}

// GetDevInfo retrieves the device information (model, serial, firmware).
func (c *Client) GetDevInfo(ctx context.Context) (*DevInfo, error) {
	resp, err := c.execHostCommand(ctx, msgIDGetDevInfo, nil)
	if err != nil {
		return nil, err
	}

	var payload devInfoMessage
	if err := xml.Unmarshal([]byte(resp.XML), &payload); err != nil {
		return nil, fmt.Errorf("failed to parse device info XML: %w", err)
	}

	info := payload.DevInfo
	if info == nil {
		info = payload.VersionInfo
	}
	if info == nil {
		return nil, fmt.Errorf("no DevInfo in response")
	}
	return info, nil
}

// GetBattery retrieves battery status from the camera for the given channel.
func (c *Client) GetBattery(ctx context.Context, channel uint8) (*BatteryInfo, error) {
	resp, err := c.execCommand(ctx, msgIDBatteryInfo, channel, nil)
	if err != nil {
		return nil, err
	}

	var payload BatteryMessage
	if err := xml.Unmarshal([]byte(resp.XML), &payload); err != nil {
		return nil, fmt.Errorf("failed to parse battery XML: %w", err)
	}

	if payload.BatteryInfo == nil {
		return nil, fmt.Errorf("no BatteryInfo in response")
	}

	return payload.BatteryInfo, nil
}
