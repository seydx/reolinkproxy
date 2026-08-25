package baichuan

import "testing"

func TestXMLElementValueFindsTheTagAnywhere(t *testing.T) {
	t.Parallel()

	doc := []byte("<?xml version=\"1.0\"?>\n<P2P><D2C_S_R><uid> 9527000012345678 </uid><port>2015</port></D2C_S_R></P2P>")
	if got := xmlElementValue(doc, "uid"); got != "9527000012345678" {
		t.Fatalf("uid = %q", got)
	}
	if got := xmlElementValue(doc, "cid"); got != "" {
		t.Fatalf("missing element returned %q", got)
	}
	if got := xmlElementValue([]byte("<uid>broken"), "uid"); got != "" {
		t.Fatalf("unterminated element returned %q", got)
	}
}
