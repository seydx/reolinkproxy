package bridge

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/bluenviron/gortsplib/v5/pkg/description"
	gformat "github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/pion/rtp"
	"github.com/shareed2k/reolinkproxy/pkg/codec"
)

type rtspTalkInput struct {
	media      *description.Media
	g711       *gformat.G711
	lpcm       *gformat.LPCM
	codecName  string
	sampleRate int
}

func selectTalkInput(desc *description.Session) (*rtspTalkInput, error) {
	if desc == nil {
		return nil, fmt.Errorf("missing announced session description")
	}

	for _, media := range desc.Medias {
		if media.Type != description.MediaTypeAudio {
			continue
		}

		for _, forma := range media.Formats {
			g711, ok := forma.(*gformat.G711)
			if ok {
				if g711.ChannelCount != 1 {
					return nil, fmt.Errorf("talkback only supports mono G711, got %d channels", g711.ChannelCount)
				}

				codecName := "PCMA"
				if g711.MULaw {
					codecName = "PCMU"
				}

				return &rtspTalkInput{
					media:      media,
					g711:       g711,
					codecName:  codecName,
					sampleRate: g711.SampleRate,
				}, nil
			}

			lpcm, ok := forma.(*gformat.LPCM)
			if !ok {
				continue
			}
			if lpcm.BitDepth != 16 {
				return nil, fmt.Errorf("talkback only supports 16-bit LPCM, got %d-bit", lpcm.BitDepth)
			}
			if lpcm.ChannelCount != 1 {
				return nil, fmt.Errorf("talkback only supports mono LPCM, got %d channels", lpcm.ChannelCount)
			}

			return &rtspTalkInput{
				media:      media,
				lpcm:       lpcm,
				codecName:  "L16",
				sampleRate: lpcm.SampleRate,
			}, nil
		}
	}

	return nil, fmt.Errorf("talkback requires mono G711 or 16-bit mono LPCM audio")
}

func selectBackChannelInputs(medias []*description.Media) ([]*rtspTalkInput, error) {
	var inputs []*rtspTalkInput

	for _, media := range medias {
		if media == nil || media.Type != description.MediaTypeAudio || !media.IsBackChannel {
			continue
		}

		for _, forma := range media.Formats {
			g711, ok := forma.(*gformat.G711)
			if !ok {
				continue
			}
			if g711.ChannelCount != 1 {
				return nil, fmt.Errorf("talkback only supports mono G711, got %d channels", g711.ChannelCount)
			}

			codecName := "PCMA"
			if g711.MULaw {
				codecName = "PCMU"
			}

			inputs = append(inputs, &rtspTalkInput{
				media:      media,
				g711:       g711,
				codecName:  codecName,
				sampleRate: g711.SampleRate,
			})
		}
	}

	if len(inputs) == 0 {
		return nil, fmt.Errorf("backchannel requires a sendonly mono G711 audio media")
	}

	return inputs, nil
}

func (i *rtspTalkInput) decode(pkt *rtp.Packet) ([]int16, error) {
	if pkt == nil {
		return nil, nil
	}
	if i == nil || (i.g711 == nil && i.lpcm == nil) {
		return nil, fmt.Errorf("talkback input is not configured")
	}

	if i.g711 != nil && i.g711.MULaw {
		return codec.DecodePCMU(pkt.Payload), nil
	}
	if i.g711 != nil {
		return codec.DecodePCMA(pkt.Payload), nil
	}

	if len(pkt.Payload)%2 != 0 {
		return nil, fmt.Errorf("invalid lpcm payload size %d", len(pkt.Payload))
	}

	out := make([]int16, len(pkt.Payload)/2)
	for j := 0; j < len(out); j++ {
		out[j] = int16(binary.BigEndian.Uint16(pkt.Payload[j*2 : j*2+2])) //#nosec G115
	}
	return out, nil
}

type pcmResampler struct {
	from   int64
	to     int64
	pos    int64 // next output position in 1/to input samples, counted from prev
	prev   int16
	primed bool
}

func newPCMResampler(fromRate int, toRate int) *pcmResampler {
	return &pcmResampler{from: int64(fromRate), to: int64(toRate)}
}

func (r *pcmResampler) resample(in []int16) []int16 {
	if !r.primed {
		if len(in) == 0 {
			return nil
		}
		r.prev, in, r.primed = in[0], in[1:], true
	}

	limit := int64(len(in)) * r.to
	out := make([]int16, 0, (limit-r.pos)/r.from+1)
	for ; r.pos < limit; r.pos += r.from {
		base := r.pos / r.to
		a := int64(r.prev)
		if base > 0 {
			a = int64(in[base-1])
		}
		b := int64(in[base])
		out = append(out, int16(a+(b-a)*(r.pos%r.to)/r.to)) //#nosec G115
	}
	r.pos -= limit
	if len(in) > 0 {
		r.prev = in[len(in)-1]
	}
	return out
}

func applyTalkVolume(pcm []int16, percent int) {
	if percent == 100 {
		return
	}
	if percent < 0 {
		percent = 0
	}

	for i, sample := range pcm {
		scaled := int64(sample) * int64(percent) / 100
		if scaled > 32767 {
			scaled = 32767
		}
		if scaled < -32768 {
			scaled = -32768
		}
		pcm[i] = int16(scaled)
	}
}

func isSilence(pcm []int16) bool {
	for _, sample := range pcm {
		if sample > 25 || sample < -25 {
			return false
		}
	}
	return true
}

func enqueueTalkPCM(ctx context.Context, pcmCh chan []int16, pcm []int16) {
	for {
		select {
		case <-ctx.Done():
			return
		case pcmCh <- pcm:
			return
		default:
		}

		select {
		case <-ctx.Done():
			return
		case <-pcmCh:
			// Drop the oldest buffered audio to keep latency bounded for live talk.
		default:
		}
	}
}

type talkbackPipeline struct {
	cameraName string
	channel    uint8
	device     *cameraDevice
	talkVolume int
	log        Logger
}

func newTalkbackPipeline(
	cameraName string,
	channel uint8,
	device *cameraDevice,
	talkVolume int,
	log Logger,
) *talkbackPipeline {
	return &talkbackPipeline{
		cameraName: cameraName,
		channel:    channel,
		device:     device,
		talkVolume: talkVolume,
		log:        log,
	}
}

func (p *talkbackPipeline) run(ctx context.Context, pcmCh <-chan []int16, input *rtspTalkInput, path string) {
	for {
		select {
		case <-ctx.Done():
			return
		case firstPcm := <-pcmCh:
			if len(firstPcm) == 0 || isSilence(firstPcm) {
				continue
			}

			connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			talkSession, err := p.device.StartTalk(connectCtx, p.channel)
			cancel()
			if err != nil {
				p.log.Warnf("talk start error: %v", err)
				continue
			}

			p.log.Debugf(
				"talk session activated path=%s input=%s/%d target=ADPCM/%d volume=%d%%",
				path,
				input.codecName,
				input.sampleRate,
				talkSession.SampleRate(),
				p.talkVolume,
			)

			p.runBridge(ctx, path, input, talkSession, firstPcm, pcmCh)

			closeCtx, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
			if err := talkSession.Close(closeCtx); err != nil {
				p.log.Warnf("talk close error: %v", err)
			}
			cancelClose()
		}
	}
}

func (p *talkbackPipeline) runBridge(
	ctx context.Context,
	path string,
	input *rtspTalkInput,
	talkSession *resilientTalkSession,
	firstPcm []int16,
	pcmCh <-chan []int16,
) {
	startedAt := time.Now()
	result := "completed (idle)"
	bridgeCtx, bridgeCancel := context.WithCancel(ctx)
	defer bridgeCancel()
	defer func() {
		if ctx.Err() != nil {
			result = ctx.Err().Error()
		}
		p.log.Debugf("talk bridge stopped path=%s duration=%v result=%s", path, time.Since(startedAt).Round(time.Millisecond), result)
	}()

	if err := p.runBridgeInternal(bridgeCtx, path, input, talkSession, firstPcm, pcmCh); err != nil {
		result = err.Error()
	}
}

func (p *talkbackPipeline) runBridgeInternal(
	ctx context.Context,
	path string,
	input *rtspTalkInput,
	talkSession *resilientTalkSession,
	firstPcm []int16,
	pcmCh <-chan []int16,
) error {
	encoder := &codec.ADPCMEncoder{}
	targetSampleRate := talkSession.SampleRate()
	blockSamples := talkSession.SamplesPerBlock()
	pcmBuffer := make([]int16, 0, blockSamples*2)
	startedAt := time.Now()
	pcmPackets := 0
	pcmSamples := 0
	blocksWritten := 0
	defer func() {
		p.log.Debugf("talk internal bridge stopped path=%s duration=%v pcm_packets=%d pcm_samples=%d blocks=%d queued=%d", path, time.Since(startedAt).Round(time.Millisecond), pcmPackets, pcmSamples, blocksWritten, len(pcmCh))
	}()

	idleTimer := time.NewTimer(5 * time.Second)
	defer idleTimer.Stop()

	resampler := newPCMResampler(input.sampleRate, targetSampleRate)
	writeParent := ctx

	writeBlocks := func() error {
		for len(pcmBuffer) >= blockSamples {
			block, err := encoder.EncodeBlock(pcmBuffer[:blockSamples])
			if err != nil {
				p.log.Warnf("talk adpcm encode error: %v", err)
				return err
			}

			writeCtx, cancel := context.WithTimeout(writeParent, 5*time.Second)
			err = talkSession.WriteADPCMBlock(writeCtx, block)
			cancel()
			if err != nil {
				p.log.Warnf("talk write error: %v", err)
				return err
			}
			blocksWritten++

			pcmBuffer = pcmBuffer[blockSamples:]
		}
		return nil
	}

	processPCM := func(pcm []int16) error {
		pcmPackets++
		pcmSamples += len(pcm)
		if input.sampleRate != targetSampleRate {
			pcm = resampler.resample(pcm)
		}
		pcmBuffer = append(pcmBuffer, pcm...)
		return writeBlocks()
	}

	// the queue and the partial block are the end of the last phrase, the
	// caller's ctx is already gone when the client hangs up
	flush := func() {
		tailCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		writeParent = tailCtx
		for len(pcmCh) > 0 {
			if processPCM(<-pcmCh) != nil {
				return
			}
		}
		if len(pcmBuffer) > 0 {
			pcmBuffer = append(pcmBuffer, make([]int16, blockSamples-len(pcmBuffer))...)
			_ = writeBlocks()
		}
	}

	// Opening the talk session takes a moment, and the audio that piled up
	// meanwhile would be pushed as one burst the camera then plays back late
	// for the rest of the session. Keep the newest, drop the rest.
	dropped := 0
	for {
		select {
		case pcm := <-pcmCh:
			dropped++
			firstPcm = pcm
			continue
		default:
		}
		break
	}
	if dropped > 0 {
		p.log.Debugf("talk dropped %d packets buffered while the session opened", dropped)
	}

	if err := processPCM(firstPcm); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			p.log.Debugf("talk internal bridge context done path=%s err=%v", path, ctx.Err())
			flush()
			return nil

		case <-idleTimer.C:
			flush()
			return nil

		case pcm := <-pcmCh:
			if !isSilence(pcm) {
				idleTimer.Reset(5 * time.Second)
			}
			if err := processPCM(pcm); err != nil {
				return err
			}
		}
	}
}
