package baichuan

import (
	"context"
	"encoding/xml"
	"fmt"
	"strings"
)

const abilityInfoTokenList = "system, streaming, PTZ, IO, security, replay, disk, network, alarm, record, video, image"

type abilityAccess uint8

const (
	abilityNone abilityAccess = iota
	abilityReadOnly
	abilityReadWrite
)

// MissingAbilityError reports that the logged-in user does not have the
// requested permission on the camera.
type MissingAbilityError struct {
	Name      string
	Requested string
	Actual    string
}

func (e *MissingAbilityError) Error() string {
	return fmt.Sprintf("missing %s access for %q (actual: %s)", e.Requested, e.Name, e.Actual)
}

type xmlAbilityInfoExtension struct {
	XMLName  xml.Name `xml:"Extension"`
	Version  string   `xml:"version,attr"`
	UserName string   `xml:"userName"`
	Token    string   `xml:"token"`
}

type xmlAbilityInfoBody struct {
	XMLName     xml.Name        `xml:"body"`
	AbilityInfo *xmlAbilityInfo `xml:"AbilityInfo"`
}

type xmlAbilityInfo struct {
	UserName  string               `xml:"userName"`
	System    *xmlAbilityInfoToken `xml:"system"`
	Network   *xmlAbilityInfoToken `xml:"network"`
	Alarm     *xmlAbilityInfoToken `xml:"alarm"`
	Image     *xmlAbilityInfoToken `xml:"image"`
	Video     *xmlAbilityInfoToken `xml:"video"`
	Security  *xmlAbilityInfoToken `xml:"security"`
	Replay    *xmlAbilityInfoToken `xml:"replay"`
	PTZ       *xmlAbilityInfoToken `xml:"PTZ"`
	IO        *xmlAbilityInfoToken `xml:"IO"`
	Streaming *xmlAbilityInfoToken `xml:"streaming"`
}

type xmlAbilityInfoToken struct {
	SubModules []xmlAbilitySubModule `xml:"subModule"`
}

type xmlAbilitySubModule struct {
	ChannelID    *uint8 `xml:"channelId"`
	AbilityValue string `xml:"abilityValue"`
}

func (c *Client) requireAbilityRW(ctx context.Context, channel uint8, name string) error {
	access, err := c.abilityAccess(ctx, channel, name)
	if err != nil {
		return err
	}

	switch access {
	case abilityReadWrite:
		return nil
	case abilityReadOnly:
		return &MissingAbilityError{Name: name, Requested: "write", Actual: "read"}
	default:
		return &MissingAbilityError{Name: name, Requested: "write", Actual: "none"}
	}
}

func (c *Client) abilityAccess(ctx context.Context, channel uint8, name string) (abilityAccess, error) {
	abilities, err := c.getAbilityInfo(ctx, channel)
	if err != nil {
		return abilityNone, err
	}

	return abilities[strings.ToLower(name)], nil
}

func (c *Client) getAbilityInfo(ctx context.Context, channel uint8) (map[string]abilityAccess, error) {
	xmlText, err := c.AbilityInfoXML(ctx)
	if err != nil {
		return nil, err
	}

	abilities, err := parseAbilityInfo(xmlText, channel)
	if err != nil {
		return nil, fmt.Errorf("parse ability info: %w", err)
	}

	return abilities, nil
}

// AbilityInfoXML returns the raw AbilityInfo XML the camera reports for the
// logged-in user — the source of truth for capability detection (motion, ptz,
// siren, floodlight, …). The table covers every channel of the device.
func (c *Client) AbilityInfoXML(ctx context.Context) (string, error) {
	if err := c.Login(ctx); err != nil {
		return "", err
	}

	ext, err := buildAbilityInfoExtension(c.cfg.Username)
	if err != nil {
		return "", fmt.Errorf("build ability extension: %w", err)
	}

	// Host-level: the ability table covers all channels; the parse below
	// filters by the requested one.
	resp, err := c.sendRequest(ctx, request{
		MsgID:     msgIDAbilityInfo,
		ChannelID: channelIDHost,
		Class:     classModernWithOffset,
		Extension: ext,
	})
	if err != nil {
		return "", fmt.Errorf("query ability info: %w", err)
	}

	return resp.XML, nil
}

func buildAbilityInfoExtension(username string) ([]byte, error) {
	return marshalXMLDocument(xmlAbilityInfoExtension{
		Version:  "1.1",
		UserName: username,
		Token:    abilityInfoTokenList,
	})
}

func parseAbilityInfo(xmlText string, channel uint8) (map[string]abilityAccess, error) {
	var body xmlAbilityInfoBody
	if err := xml.Unmarshal([]byte(xmlText), &body); err != nil {
		return nil, err
	}
	if body.AbilityInfo == nil {
		return nil, fmt.Errorf("ability info missing from response")
	}

	abilities := make(map[string]abilityAccess)
	tokens := []*xmlAbilityInfoToken{
		body.AbilityInfo.System,
		body.AbilityInfo.Network,
		body.AbilityInfo.Alarm,
		body.AbilityInfo.Image,
		body.AbilityInfo.Video,
		body.AbilityInfo.Security,
		body.AbilityInfo.Replay,
		body.AbilityInfo.PTZ,
		body.AbilityInfo.IO,
		body.AbilityInfo.Streaming,
	}

	for _, token := range tokens {
		if token == nil {
			continue
		}
		for _, subModule := range token.SubModules {
			if subModule.ChannelID != nil && *subModule.ChannelID != channel {
				continue
			}
			mergeAbilities(abilities, subModule.AbilityValue)
		}
	}

	return abilities, nil
}

func mergeAbilities(dst map[string]abilityAccess, raw string) {
	for _, token := range strings.Split(raw, ",") {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}

		idx := strings.LastIndex(token, "_")
		if idx <= 0 || idx == len(token)-1 {
			continue
		}

		name := strings.ToLower(token[:idx])
		var access abilityAccess
		switch token[idx+1:] {
		case "rw":
			access = abilityReadWrite
		case "ro":
			access = abilityReadOnly
		default:
			continue
		}

		if access > dst[name] {
			dst[name] = access
		}
	}
}
