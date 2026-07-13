// Package main provides reolinkcli, a test harness for exercising the
// Baichuan library against real cameras: discovery, device info, abilities,
// snapshots, event listening, controls and raw protocol exploration.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/shareed2k/reolinkproxy/pkg/baichuan"
)

const usage = `reolinkcli — Baichuan test harness

Usage:
  reolinkcli discover [-t 5s]
  reolinkcli <conn flags> info
  reolinkcli <conn flags> abilities
  reolinkcli <conn flags> support
  reolinkcli <conn flags> battery
  reolinkcli <conn flags> snap [-o snapshot.jpg]
  reolinkcli <conn flags> listen [-d 60s]
  reolinkcli <conn flags> preview [-stream main] [-d 10s]
  reolinkcli <conn flags> siren <on|off>
  reolinkcli <conn flags> led <on|off>
  reolinkcli <conn flags> ptz <left|right|up|down|zoomInc|zoomDec|stop> [-speed 16] [-d 1s]
  reolinkcli <conn flags> raw -msg <id> [-body '<xml>']

Connection flags (before the command):
  -host <ip>     camera IP (Baichuan TCP)
  -uid <uid>     camera UID (broadcast, same L2 segment)
  -user <name>   username (default admin)
  -pass <pw>     password
  -port <n>      Baichuan port (default 9000)
  -channel <n>   channel for NVR/Hub (default 0)
  -cam <alias>   load connection settings from the credentials file
  -creds <path>  credentials file (default ~/.reolinkcli.json)

Credentials file format:
  { "cameras": { "<alias>": { "host": "…", "uid": "…", "username": "admin", "password": "…" } } }
`

func main() {
	conn := flag.NewFlagSet("reolinkcli", flag.ExitOnError)
	host := conn.String("host", "", "camera IP")
	uid := conn.String("uid", "", "camera UID")
	user := conn.String("user", "admin", "username")
	pass := conn.String("pass", "", "password")
	port := conn.Int("port", 9000, "baichuan port")
	channel := conn.Int("channel", 0, "channel")
	cam := conn.String("cam", "", "camera alias from credentials file")
	creds := conn.String("creds", defaultCredsPath(), "credentials file")
	conn.Usage = func() { fmt.Print(usage) }
	_ = conn.Parse(os.Args[1:])

	if *cam != "" {
		entry, err := loadCredentials(*creds, *cam)
		exitOn(err)
		if *host == "" {
			*host = entry.Host
		}
		if *uid == "" {
			*uid = entry.UID
		}
		if entry.Username != "" {
			*user = entry.Username
		}
		if *pass == "" {
			*pass = entry.Password
		}
		if entry.Port != 0 {
			*port = entry.Port
		}
		if entry.Channel != 0 {
			*channel = entry.Channel
		}
	}

	args := conn.Args()
	if len(args) == 0 {
		fmt.Print(usage)
		os.Exit(2)
	}
	command, args := args[0], args[1:]

	ctx := context.Background()

	if command == "discover" {
		fs := flag.NewFlagSet("discover", flag.ExitOnError)
		timeout := fs.Duration("t", 5*time.Second, "scan duration")
		_ = fs.Parse(args)
		runDiscover(ctx, *timeout)
		return
	}

	switch command {
	case "info", "abilities", "support", "battery", "snap", "listen", "preview", "siren", "led", "ptz", "raw":
	default:
		fmt.Print(usage)
		os.Exit(2)
	}

	if *host == "" && *uid == "" {
		fmt.Fprintln(os.Stderr, "error: -host or -uid is required")
		fmt.Print(usage)
		os.Exit(2)
	}

	client := dial(ctx, baichuan.Config{
		Host:     *host,
		UID:      *uid,
		Username: *user,
		Password: *pass,
		Port:     *port,
		Timeout:  15 * time.Second,
	})
	defer func() { _ = client.Close() }()
	ch := uint8(*channel) //#nosec G115

	switch command {
	case "info":
		info, err := client.GetDevInfo(ctx, ch)
		exitOn(err)
		fmt.Printf("name:     %s\ntype:     %s\nserial:   %s\nhardware: %s\nfirmware: %s\nitemNo:   %s\ndetail:   %s\n",
			info.Name, info.Type, info.SerialNumber, info.HardwareVersion, info.FirmwareVersion, info.ItemNo, info.Detail)

	case "abilities":
		xmlText, err := client.AbilityInfoXML(ctx, ch)
		exitOn(err)
		fmt.Println(xmlText)

	case "support":
		support, err := client.GetSupport(ctx)
		exitOn(err)
		fmt.Printf("channels=%d audioTalk=%t externStream=%t\n", support.ChannelNum, support.AudioTalk, support.ExternStream)
		for _, chn := range support.Channels {
			fmt.Printf("  ch%d: ptz=%t (pan=%t tilt=%t zoom=%t) battery=%t doorbell=%t siren=%t floodlight=%t motion=%t ai=%v\n",
				chn.Channel, chn.PTZ, chn.Pan, chn.Tilt, chn.Zoom, chn.Battery, chn.Doorbell, chn.Siren, chn.Floodlight, chn.Motion, chn.AITypes)
		}
		profiles, err := client.StreamProfiles(ctx, ch)
		exitOn(err)
		for _, p := range profiles {
			fmt.Printf("  stream %-6s %dx%d @%dfps\n", p.Name, p.Width, p.Height, p.Framerate)
		}

	case "battery":
		info, err := client.GetBattery(ctx, ch)
		exitOn(err)
		fmt.Printf("%+v\n", *info)

	case "snap":
		fs := flag.NewFlagSet("snap", flag.ExitOnError)
		out := fs.String("o", "snapshot.jpg", "output file")
		_ = fs.Parse(args)
		start := time.Now()
		snapCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		img, err := client.Snap(snapCtx, ch)
		exitOn(err)
		exitOn(os.WriteFile(*out, img, 0o600))
		fmt.Printf("wrote %s (%d bytes) in %v\n", *out, len(img), time.Since(start).Round(time.Millisecond))

	case "listen":
		fs := flag.NewFlagSet("listen", flag.ExitOnError)
		duration := fs.Duration("d", 60*time.Second, "listen duration")
		_ = fs.Parse(args)
		runListen(ctx, client, ch, *duration)

	case "preview":
		fs := flag.NewFlagSet("preview", flag.ExitOnError)
		stream := fs.String("stream", "main", "stream profile: main, sub, extern")
		duration := fs.Duration("d", 10*time.Second, "preview duration")
		_ = fs.Parse(args)
		runPreview(ctx, client, ch, *stream, *duration)

	case "siren":
		exitOn(client.Siren(ctx, ch, onOff(args)))
		fmt.Println("ok")

	case "led":
		exitOn(client.SetWhiteLed(ctx, ch, onOff(args)))
		fmt.Println("ok")

	case "ptz":
		fs := flag.NewFlagSet("ptz", flag.ExitOnError)
		speed := fs.Int("speed", 16, "speed 1-64")
		duration := fs.Duration("d", time.Second, "move duration before stop")
		if len(args) == 0 {
			exitOn(fmt.Errorf("ptz needs a command (left/right/up/down/zoomInc/zoomDec/stop)"))
		}
		cmd := args[0]
		_ = fs.Parse(args[1:])
		exitOn(client.PTZControl(ctx, ch, cmd, *speed))
		if cmd != "stop" {
			time.Sleep(*duration)
			exitOn(client.PTZControl(ctx, ch, "stop", 0))
		}
		fmt.Println("ok")

	case "raw":
		fs := flag.NewFlagSet("raw", flag.ExitOnError)
		msgID := fs.Uint("msg", 0, "baichuan message ID")
		body := fs.String("body", "", "XML body (without header)")
		_ = fs.Parse(args)
		if *msgID == 0 {
			exitOn(fmt.Errorf("raw needs -msg <id>"))
		}
		xmlText, err := client.RawCommand(ctx, uint32(*msgID), ch, *body)
		exitOn(err)
		fmt.Println(xmlText)
	}
}

type credsEntry struct {
	Host     string `json:"host"`
	UID      string `json:"uid"`
	Username string `json:"username"`
	Password string `json:"password"`
	Port     int    `json:"port"`
	Channel  int    `json:"channel"`
}

type credsFile struct {
	Cameras map[string]credsEntry `json:"cameras"`
}

func defaultCredsPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".reolinkcli.json"
	}
	return filepath.Join(home, ".reolinkcli.json")
}

func loadCredentials(path string, alias string) (credsEntry, error) {
	data, err := os.ReadFile(path) //#nosec G304 -- user-supplied credentials path
	if err != nil {
		return credsEntry{}, fmt.Errorf("read credentials file: %w", err)
	}

	var file credsFile
	if err := json.Unmarshal(data, &file); err != nil {
		return credsEntry{}, fmt.Errorf("parse credentials file: %w", err)
	}

	entry, ok := file.Cameras[alias]
	if !ok {
		aliases := make([]string, 0, len(file.Cameras))
		for name := range file.Cameras {
			aliases = append(aliases, name)
		}
		return credsEntry{}, fmt.Errorf("camera %q not in credentials file (have: %v)", alias, aliases)
	}
	return entry, nil
}

func dial(ctx context.Context, cfg baichuan.Config) *baichuan.Client {
	client, err := baichuan.Dial(ctx, cfg)
	exitOn(err)
	exitOn(client.Login(ctx))
	fmt.Fprintln(os.Stderr, "# connected + logged in")
	return client
}

func runDiscover(ctx context.Context, timeout time.Duration) {
	fmt.Fprintf(os.Stderr, "# broadcasting for %v...\n", timeout)
	devices, err := baichuan.Discover(ctx, timeout)
	exitOn(err)
	if len(devices) == 0 {
		fmt.Println("no devices found")
		return
	}
	for _, d := range devices {
		fmt.Printf("ip=%-15s mac=%-17s name=%-20q ident=%-6q uid=%s\n", d.IP, d.MAC, d.Name, d.Ident, d.UID)
	}
}

func runListen(ctx context.Context, client *baichuan.Client, channel uint8, duration time.Duration) {
	listenCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	cancelAlarms, err := client.ListenForAlarms(listenCtx, channel, func(state baichuan.AlarmState) {
		fmt.Printf("[%s] alarm: motion=%t aiTypes=%v\n", time.Now().Format("15:04:05.000"), state.MotionDetected, state.AITypes)
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "# alarm listener failed: %v\n", err)
	} else {
		defer cancelAlarms()
	}

	cancelBattery := client.ListenForBattery(listenCtx, channel, func(info baichuan.BatteryInfo) {
		fmt.Printf("[%s] battery: %+v\n", time.Now().Format("15:04:05.000"), info)
	})
	defer cancelBattery()

	fmt.Fprintf(os.Stderr, "# listening for %v (trigger motion / press doorbell now)...\n", duration)
	select {
	case <-listenCtx.Done():
	case <-client.Done():
		fmt.Fprintf(os.Stderr, "# connection closed: %v\n", client.Err())
	}
}

func runPreview(ctx context.Context, client *baichuan.Client, channel uint8, stream string, duration time.Duration) {
	profiles := map[string]baichuan.Stream{
		"main":   baichuan.StreamMain,
		"sub":    baichuan.StreamSub,
		"extern": baichuan.StreamExtern,
	}
	profile, ok := profiles[stream]
	if !ok {
		exitOn(fmt.Errorf("unknown stream %q", stream))
	}

	reader, err := client.StartPreview(ctx, channel, profile)
	exitOn(err)

	fmt.Fprintf(os.Stderr, "# previewing %s for %v...\n", stream, duration)
	deadline := time.After(duration)
	var video, audio, other int
	var videoBytes int
	var codec string
	var width, height uint32
	var fps uint8

	for {
		select {
		case <-deadline:
			fmt.Printf("stream=%s codec=%s size=%dx%d fps=%d video_packets=%d video_bytes=%d audio_packets=%d other=%d\n",
				stream, codec, width, height, fps, video, videoBytes, audio, other)
			_ = client.StopPreview(ctx, channel, profile)
			return
		case pkt, ok := <-reader.Packets:
			if !ok {
				fmt.Fprintln(os.Stderr, "# stream ended early")
				return
			}
			switch pkt.Kind {
			case baichuan.MediaPacketInfoV1, baichuan.MediaPacketInfoV2:
				width, height, fps = pkt.Width, pkt.Height, pkt.FPS
			case baichuan.MediaPacketIFrame, baichuan.MediaPacketPFrame:
				video++
				videoBytes += len(pkt.Data)
				if pkt.Codec != "" {
					codec = pkt.Codec
				}
			case baichuan.MediaPacketAAC, baichuan.MediaPacketADPCM:
				audio++
			default:
				other++
			}
		}
	}
}

func onOff(args []string) int {
	if len(args) == 0 {
		exitOn(fmt.Errorf("expected on or off"))
	}
	switch args[0] {
	case "on", "1", "true":
		return 1
	case "off", "0", "false":
		return 0
	default:
		v, err := strconv.Atoi(args[0])
		exitOn(err)
		return v
	}
}

func exitOn(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
