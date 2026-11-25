package baichuan

import (
	"bytes"
	"encoding/binary"
	"slices"
	"testing"
	"time"
)

func TestMediaParserIFrame(t *testing.T) {
	t.Parallel()

	raw := new(bytes.Buffer)
	writeU32 := func(v uint32) {
		if err := binary.Write(raw, binary.LittleEndian, v); err != nil {
			t.Fatalf("binary.Write(): %v", err)
		}
	}

	writeU32(bcmediaIFrameMin)
	raw.WriteString("H264")
	writeU32(3)
	writeU32(4)
	writeU32(123)
	writeU32(0)
	writeU32(1700000000)
	raw.Write([]byte{0x01, 0x02, 0x03})
	raw.Write(make([]byte, 5))

	var parser MediaParser

	if packets, err := parser.Append(raw.Bytes()[:10]); err != nil || len(packets) != 0 {
		t.Fatalf("partial append = (%d packets, %v), want (0, nil)", len(packets), err)
	}

	packets, err := parser.Append(raw.Bytes()[10:])
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if len(packets) != 1 {
		t.Fatalf("Append() packets = %d, want 1", len(packets))
	}

	packet := packets[0]
	if packet.Kind != MediaPacketIFrame {
		t.Fatalf("packet.Kind = %v, want %v", packet.Kind, MediaPacketIFrame)
	}
	if packet.Codec != "H264" {
		t.Fatalf("packet.Codec = %q, want %q", packet.Codec, "H264")
	}
	if packet.TimestampMicrosecs != 123 {
		t.Fatalf("packet.TimestampMicrosecs = %d, want 123", packet.TimestampMicrosecs)
	}
	if !packet.HasTimestamp {
		t.Fatalf("packet.HasTimestamp = false, want true")
	}
	if !bytes.Equal(packet.Data, []byte{0x01, 0x02, 0x03}) {
		t.Fatalf("packet.Data = %v, want %v", packet.Data, []byte{0x01, 0x02, 0x03})
	}
	if packet.UnixTime == nil {
		t.Fatalf("packet.UnixTime is nil")
	}
	if got, want := packet.UnixTime.UTC(), time.Unix(1700000000, 0).UTC(); !got.Equal(want) {
		t.Fatalf("packet.UnixTime = %v, want %v", got, want)
	}
}

func TestMediaParserAACVariants(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		magic uint32
	}{
		{name: "bw50", magic: bcmediaAAC},
		{name: "bw51", magic: bcmediaAACV2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			payload := []byte{0xff, 0xf9, 0x60, 0x40, 0x01}
			raw := new(bytes.Buffer)
			writeU32 := func(v uint32) {
				if err := binary.Write(raw, binary.LittleEndian, v); err != nil {
					t.Fatalf("binary.Write(): %v", err)
				}
			}
			writeU16 := func(v uint16) {
				if err := binary.Write(raw, binary.LittleEndian, v); err != nil {
					t.Fatalf("binary.Write(): %v", err)
				}
			}

			writeU32(tc.magic)
			writeU16(uint16(len(payload)))
			writeU16(uint16(len(payload)))
			raw.Write(payload)
			raw.Write(make([]byte, padLen(len(payload))))

			var parser MediaParser
			packets, err := parser.Append(raw.Bytes())
			if err != nil {
				t.Fatalf("Append() error = %v", err)
			}
			if len(packets) != 1 {
				t.Fatalf("Append() packets = %d, want 1", len(packets))
			}
			if packets[0].Kind != MediaPacketAAC {
				t.Fatalf("packet.Kind = %v, want %v", packets[0].Kind, MediaPacketAAC)
			}
			if packets[0].HasTimestamp {
				t.Fatalf("packet.HasTimestamp = true, want false")
			}
			if !bytes.Equal(packets[0].Data, payload) {
				t.Fatalf("packet.Data = %v, want %v", packets[0].Data, payload)
			}
		})
	}
}

func TestMediaParserADPCMNoTimestamp(t *testing.T) {
	t.Parallel()

	payload := []byte{0x01, 0x02, 0x03, 0x04}
	payloadSize := 4 + len(payload)
	raw := new(bytes.Buffer)
	writeU32 := func(v uint32) {
		if err := binary.Write(raw, binary.LittleEndian, v); err != nil {
			t.Fatalf("binary.Write(): %v", err)
		}
	}
	writeU16 := func(v uint16) {
		if err := binary.Write(raw, binary.LittleEndian, v); err != nil {
			t.Fatalf("binary.Write(): %v", err)
		}
	}

	writeU32(bcmediaADPCM)
	writeU16(uint16(payloadSize))
	writeU16(uint16(payloadSize))
	writeU16(bcmediaADPCMHeader)
	writeU16(2)
	raw.Write(payload)

	var parser MediaParser
	packets, err := parser.Append(raw.Bytes())
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if len(packets) != 1 {
		t.Fatalf("Append() packets = %d, want 1", len(packets))
	}
	if packets[0].Kind != MediaPacketADPCM {
		t.Fatalf("packet.Kind = %v, want %v", packets[0].Kind, MediaPacketADPCM)
	}
	if packets[0].HasTimestamp {
		t.Fatalf("packet.HasTimestamp = true, want false")
	}
	if !bytes.Equal(packets[0].Data, payload) {
		t.Fatalf("packet.Data = %v, want %v", packets[0].Data, payload)
	}
}

func TestMediaParserCodecFromKeyframe(t *testing.T) {
	t.Parallel()

	frame := func(magic uint32, codec string, data []byte) []byte {
		raw := new(bytes.Buffer)
		for _, v := range []any{magic, []byte(codec), uint32(len(data)), uint32(0), uint32(1), uint32(0)} {
			if err := binary.Write(raw, binary.LittleEndian, v); err != nil {
				t.Fatal(err)
			}
		}
		raw.Write(data)
		raw.Write(make([]byte, padLen(len(data))))
		return raw.Bytes()
	}
	h265Key := []byte{0, 0, 0, 1, 0x46, 0x01, 0x50, 0, 0, 0, 1, 0x40, 0x01, 0x0c, 0, 0, 1, 0x42, 0x01}
	h264Key := []byte{0, 0, 0, 1, 0x09, 0xf0, 0, 0, 0, 1, 0x27, 0x64, 0x00, 0x33}
	slice := []byte{0, 0, 0, 1, 0x41, 0x9a}

	tests := []struct {
		name   string
		frames [][]byte
		want   []string
	}{
		{"header says H264, keyframe carries H265", [][]byte{frame(bcmediaIFrameMin, "H264", h265Key), frame(bcmediaPFrameMin, "H264", slice)}, []string{"H265", "H265"}},
		{"header says H265, keyframe carries H264", [][]byte{frame(bcmediaIFrameMin, "H265", h264Key), frame(bcmediaPFrameMin, "H265", slice)}, []string{"H264", "H264"}},
		{"keyframe without parameter sets keeps the header", [][]byte{frame(bcmediaIFrameMin, "H265", slice)}, []string{"H265"}},
		{"codec switch follows the next keyframe", [][]byte{frame(bcmediaIFrameMin, "H264", h264Key), frame(bcmediaIFrameMin, "H265", h265Key), frame(bcmediaPFrameMin, "H265", slice)}, []string{"H264", "H265", "H265"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var parser MediaParser
			var got []string
			for _, f := range tt.frames {
				packets, err := parser.Append(f)
				if err != nil {
					t.Fatal(err)
				}
				for _, p := range packets {
					got = append(got, p.Codec)
				}
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("codecs = %v, want %v", got, tt.want)
			}
		})
	}
}
