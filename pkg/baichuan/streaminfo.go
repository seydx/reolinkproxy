package baichuan

import (
	"context"
	"encoding/xml"
	"fmt"
)

// StreamProfile describes one stream the camera actually offers, decoded from
// the StreamInfoList report (msg 146).
type StreamProfile struct {
	// Name is the bridge profile name: "main", "sub" or "extern".
	Name      string
	Width     int
	Height    int
	Framerate int
}

type xmlStreamInfoListBody struct {
	StreamInfoList *struct {
		StreamInfos []xmlStreamInfo `xml:"StreamInfo"`
	} `xml:"StreamInfoList"`
}

type xmlStreamInfo struct {
	ChannelBits  int              `xml:"channelBits"`
	EncodeTables []xmlEncodeTable `xml:"encodeTable"`
}

type xmlEncodeTable struct {
	Type       string `xml:"type"`
	Resolution struct {
		Width  int `xml:"width"`
		Height int `xml:"height"`
	} `xml:"resolution"`
	DefaultFramerate int `xml:"defaultFramerate"`
}

// StreamProfiles returns the streams a channel offers, ordered main, extern,
// sub. Devices list multiple encode tables per stream type (selectable
// resolutions); the highest resolution is reported.
func (c *Client) StreamProfiles(ctx context.Context, channel uint8) ([]StreamProfile, error) {
	// Host-level query: the list carries every channel; the ChannelBits filter
	// below selects the requested one.
	resp, err := c.execHostCommand(ctx, msgIDStreamInfoList, nil)
	if err != nil {
		return nil, err
	}

	var body xmlStreamInfoListBody
	if err := xml.Unmarshal([]byte(resp.XML), &body); err != nil {
		return nil, fmt.Errorf("parse stream info XML: %w", err)
	}
	if body.StreamInfoList == nil {
		return nil, fmt.Errorf("no StreamInfoList in response")
	}

	best := make(map[string]StreamProfile)
	for _, info := range body.StreamInfoList.StreamInfos {
		if info.ChannelBits>>channel&1 != 1 {
			continue
		}
		for _, table := range info.EncodeTables {
			name := profileNameForStreamType(table.Type)
			if name == "" {
				continue
			}
			candidate := StreamProfile{
				Name:      name,
				Width:     table.Resolution.Width,
				Height:    table.Resolution.Height,
				Framerate: table.DefaultFramerate,
			}
			if current, ok := best[name]; !ok || candidate.Width*candidate.Height > current.Width*current.Height {
				best[name] = candidate
			}
		}
	}

	var profiles []StreamProfile
	for _, name := range []string{"main", "extern", "sub"} {
		if profile, ok := best[name]; ok {
			profiles = append(profiles, profile)
		}
	}
	if len(profiles) == 0 {
		return nil, fmt.Errorf("camera reported no streams")
	}
	return profiles, nil
}

// OccupiedChannels returns the channels that actually offer streams —
// on an NVR/Hub these are the ports with a camera attached.
func (c *Client) OccupiedChannels(ctx context.Context) ([]int, error) {
	resp, err := c.execHostCommand(ctx, msgIDStreamInfoList, nil)
	if err != nil {
		return nil, err
	}

	var body xmlStreamInfoListBody
	if err := xml.Unmarshal([]byte(resp.XML), &body); err != nil {
		return nil, fmt.Errorf("parse stream info XML: %w", err)
	}
	if body.StreamInfoList == nil {
		return nil, fmt.Errorf("no StreamInfoList in response")
	}

	var mask uint64
	for _, info := range body.StreamInfoList.StreamInfos {
		mask |= uint64(info.ChannelBits) //#nosec G115
	}

	var channels []int
	for ch := 0; ch < 64; ch++ {
		if mask>>ch&1 == 1 {
			channels = append(channels, ch)
		}
	}
	return channels, nil
}

func profileNameForStreamType(streamType string) string {
	switch streamType {
	case "mainStream":
		return "main"
	case "subStream":
		return "sub"
	case "externStream":
		return "extern"
	default:
		return ""
	}
}
