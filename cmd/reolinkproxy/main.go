// Package main provides the standalone reolinkproxy binary: an env-configured
// wrapper around pkg/bridge that restreams Reolink cameras as RTSP.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/urfave/cli/v3"

	"github.com/shareed2k/reolinkproxy/pkg/bridge"
)

var (
	Version = "dev"
	Commit  = "none"
)

type serverFlags struct {
	RTSPAddress             string
	RTPAddress              string
	RTCPAddress             string
	PprofAddress            string
	LogLevel                string
	LogPackets              bool
	AudioPacerInitialLatMs  int
	AudioPacerMaxLeadMs     int
	VideoPacerInitialLatMs  int
	VideoPacerMaxLeadMs     int
	EnableRTCPSenderReports bool
}

var flags = serverFlags{
	RTSPAddress:            ":8554",
	RTPAddress:             ":8000",
	RTCPAddress:            ":8001",
	LogLevel:               "info",
	AudioPacerInitialLatMs: 500,
	AudioPacerMaxLeadMs:    2000,
	VideoPacerInitialLatMs: 1500,
	VideoPacerMaxLeadMs:    3000,
}

func envVars(names ...string) cli.ValueSourceChain {
	prefixed := make([]string, len(names))
	for i, name := range names {
		prefixed[i] = "REOLINK_" + name
	}
	return cli.EnvVars(prefixed...)
}

func main() {
	log := newAppLogger()

	cmd := &cli.Command{
		Name:                      "reolinkproxy",
		Usage:                     "restream reolink camera feeds as RTSP",
		UsageText:                 "reolinkproxy [options]\n\nExample camera env:\n  REOLINK_CAMERA_0_NAME=front \n  REOLINK_CAMERA_0_UID=123456 \n  REOLINK_CAMERA_0_HOST=192.168.1.10 \n  REOLINK_CAMERA_0_USERNAME=admin \n  REOLINK_CAMERA_0_PASSWORD=secret",
		Version:                   fmt.Sprintf("%s (commit: %s)", Version, Commit),
		DisableSliceFlagSeparator: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "server-rtsp-address",
				Usage:       "rtsp server listen address",
				Sources:     envVars("SERVER_RTSP_ADDRESS"),
				Value:       flags.RTSPAddress,
				Destination: &flags.RTSPAddress,
			},
			&cli.StringFlag{
				Name:        "server-rtp-address",
				Usage:       "rtp server listen address",
				Sources:     envVars("SERVER_RTP_ADDRESS"),
				Value:       flags.RTPAddress,
				Destination: &flags.RTPAddress,
			},
			&cli.StringFlag{
				Name:        "server-rtcp-address",
				Usage:       "rtcp server listen address",
				Sources:     envVars("SERVER_RTCP_ADDRESS"),
				Value:       flags.RTCPAddress,
				Destination: &flags.RTCPAddress,
			},
			&cli.StringFlag{
				Name:        "server-pprof-address",
				Usage:       "pprof server listen address (e.g. :6060)",
				Sources:     envVars("SERVER_PPROF_ADDRESS"),
				Value:       flags.PprofAddress,
				Destination: &flags.PprofAddress,
			},
			&cli.StringFlag{
				Name:        "server-log-level",
				Usage:       "log level (debug, info, warn, error)",
				Sources:     envVars("SERVER_LOG_LEVEL"),
				Value:       flags.LogLevel,
				Destination: &flags.LogLevel,
			},
			&cli.BoolFlag{
				Name:        "server-log-packets",
				Usage:       "enable packet logging",
				Sources:     envVars("SERVER_LOG_PACKETS"),
				Value:       flags.LogPackets,
				Destination: &flags.LogPackets,
			},
			&cli.IntFlag{
				Name:        "server-audio-pacer-initial-latency-ms",
				Usage:       "RTSP audio pacer startup delay in ms (smooths bursts; default 500)",
				Sources:     envVars("SERVER_AUDIO_PACER_INITIAL_LATENCY_MS"),
				Value:       flags.AudioPacerInitialLatMs,
				Destination: &flags.AudioPacerInitialLatMs,
			},
			&cli.IntFlag{
				Name:        "server-audio-pacer-max-lead-ms",
				Usage:       "max audio pacer cursor lead over wall clock in ms before snapping (default 2000)",
				Sources:     envVars("SERVER_AUDIO_PACER_MAX_LEAD_MS"),
				Value:       flags.AudioPacerMaxLeadMs,
				Destination: &flags.AudioPacerMaxLeadMs,
			},
			&cli.IntFlag{
				Name:        "server-video-pacer-initial-latency-ms",
				Usage:       "RTSP video pacer startup delay in ms (default 1500)",
				Sources:     envVars("SERVER_VIDEO_PACER_INITIAL_LATENCY_MS"),
				Value:       flags.VideoPacerInitialLatMs,
				Destination: &flags.VideoPacerInitialLatMs,
			},
			&cli.IntFlag{
				Name:        "server-video-pacer-max-lead-ms",
				Usage:       "max video pacer cursor lead over wall clock in ms before snapping (default 3000)",
				Sources:     envVars("SERVER_VIDEO_PACER_MAX_LEAD_MS"),
				Value:       flags.VideoPacerMaxLeadMs,
				Destination: &flags.VideoPacerMaxLeadMs,
			},
			&cli.BoolFlag{
				Name:        "server-enable-rtcp-sender-reports",
				Usage:       "emit periodic RTCP Sender Reports (default off; enable for legacy clients that require SR)",
				Sources:     envVars("SERVER_ENABLE_RTCP_SENDER_REPORTS"),
				Value:       flags.EnableRTCPSenderReports,
				Destination: &flags.EnableRTCPSenderReports,
			},
		},
		Action: func(ctx context.Context, _ *cli.Command) error {
			if err := log.Configure(flags.LogLevel); err != nil {
				return err
			}

			cameras, err := loadCamerasFromEnv()
			if err != nil {
				return fmt.Errorf("load cameras from environment: %w", err)
			}
			if len(cameras) == 0 {
				return fmt.Errorf("no cameras defined in environment")
			}

			return runApp(ctx, log, cameras)
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		log.Errorf("%v", err)
		os.Exit(1)
	}
}

func runApp(ctx context.Context, log *appLogger, cameras []bridge.CameraConfig) error {
	ctx, cancel := signalContext(ctx, log)
	defer cancel()
	defer log.Infof("application stopped")

	if flags.PprofAddress != "" {
		go func() {
			log.Infof("starting pprof server on %s", flags.PprofAddress)
			server := &http.Server{Addr: flags.PprofAddress, ReadHeaderTimeout: 5 * time.Second}
			if err := server.ListenAndServe(); err != nil {
				log.Warnf("pprof server error: %v", err)
			}
		}()
	}

	b := bridge.New(bridge.Options{
		RTSPAddress:              flags.RTSPAddress,
		RTPAddress:               flags.RTPAddress,
		RTCPAddress:              flags.RTCPAddress,
		EnableRTCPSenderReports:  flags.EnableRTCPSenderReports,
		LogPackets:               flags.LogPackets,
		Logger:                   log,
		AudioPacerInitialLatency: time.Duration(flags.AudioPacerInitialLatMs) * time.Millisecond,
		AudioPacerMaxLead:        time.Duration(flags.AudioPacerMaxLeadMs) * time.Millisecond,
		VideoPacerInitialLatency: time.Duration(flags.VideoPacerInitialLatMs) * time.Millisecond,
		VideoPacerMaxLead:        time.Duration(flags.VideoPacerMaxLeadMs) * time.Millisecond,
	})
	if err := b.Start(); err != nil {
		return err
	}
	defer b.Close()

	for _, cfg := range cameras {
		cam, err := b.AddCamera(cfg)
		if err != nil {
			return fmt.Errorf("add camera %s: %w", cfg.Name, err)
		}
		log.Infof("camera %s ready at %s", cam.Name(), cam.StreamURL(""))
	}

	<-ctx.Done()
	log.Infof("application shutdown started: %v", ctx.Err())
	return nil
}

func signalContext(ctx context.Context, log *appLogger) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		select {
		case sig := <-sigCh:
			log.Infof("shutdown signal received signal=%s", sig)
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx, func() {
		signal.Stop(sigCh)
		cancel()
	}
}
