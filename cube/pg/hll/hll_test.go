package hll

import (
	"bytes"
	"math"
	"net"
	"strings"
	"testing"
)

func TestOpenClose(t *testing.T) {
	ca, err := New(DEFAULT_LOG2M, DEFAULT_REGWIDTH, DEFAULT_EXPTHRESH, DEFAULT_SPARSEON)
	if err != nil {
		t.Fatal(err)
	}
	defer ca.Delete()
}

func TestOpenCloseDefault(t *testing.T) {
	ca, err := NewDefault()
	if err != nil {
		t.Fatal(err)
	}
	defer ca.Delete()
}

func TestAdd(t *testing.T) {
	ca, err := NewDefault()
	if err != nil {
		t.Fatal(err)
	}
	defer ca.Delete()
	ca.Add("test")
}

func TestAddInts(t *testing.T) {
	ca, err := NewDefault()
	if err != nil {
		t.Fatal(err)
	}
	defer ca.Delete()
	ca.AddInt32(34)
	ca.AddInt32(1)
	ca.AddInt32(1)
	ca.AddInt64(304)

	if ca.GetCardinality() != 3 {
		t.Errorf("Cardinality failed: got %f, want %f.", ca.GetCardinality(), 3.)
	}

	if err = ca.Add4Bytes(net.ParseIP("153.23.1.4").To4()); err != nil {
		t.Fatal(err)
	}

	if err = ca.Add4Bytes(net.ParseIP("153.23.1.4").To4()); err != nil {
		t.Fatal(err)
	}

	if err = ca.Add4Bytes(net.ParseIP("153.23.1.4").To4()); err != nil {
		t.Fatal(err)
	}

	valb := net.ParseIP("::1").To16()
	if err = ca.Add8Bytes(valb[0:8]); err != nil {
		t.Fatal(err)
	}

	if ca.GetCardinality() != 5 {
		t.Errorf("Cardinality failed: got %f, want %f.", ca.GetCardinality(), 5.)
	}
}

func TestUnion(t *testing.T) {
	ca, err := NewDefault()
	if err != nil {
		t.Fatal(err)
	}
	defer ca.Delete()

	cb, err := NewDefault()
	if err != nil {
		t.Fatal(err)
	}
	defer cb.Delete()

	ca.Add("test1")
	ca.Add("test2")
	cb.Add("test1")
	cb.Add("test3")

	ca.Union(cb)

	eVal := "EXPLICIT, 3 elements, nregs=2048, nbits=5, expthresh=-1(160), sparseon=1:"
	sVal := strings.Split(ca.Print(), "\n")
	if len(sVal) <= 0 {
		t.Errorf("Got invalid respose from Serialize (0 length array after serialize)")
	} else if sVal[0] != eVal {
		t.Errorf("Print failed: got \n%s\n, want \n%s\n.", sVal[0], eVal)
	}
}

func TestSerialize(t *testing.T) {
	ca, err := NewDefault()
	if err != nil {
		t.Fatal(err)
	}
	defer ca.Delete()

	ca.Add("test1")
	ca.Add("test2")
	ca.Add("test3")
	ca.Add("test4")
	ser := ca.Serialize()

	target := []byte{18, 139, 127, 165, 171, 229, 36, 51, 65, 150, 204, 166, 54, 223, 36, 180, 128, 179, 96, 247, 35, 127, 96, 49, 70, 47, 150, 3, 194, 204, 79, 8, 188, 203, 205}

	if !bytes.Equal(ser, target) {
		t.Errorf("Serialize failed: got %v, want %v.", ser, target)
	}
}

func BenchmarkHashBytes(b *testing.B) {
	ca, err := NewDefault()
	if err != nil {
		b.Fatal(err)
	}
	defer ca.Delete()

	for i := 0; i < b.N; i++ {
		if err = ca.Add4Bytes(net.ParseIP("153.23.1.4").To4()); err != nil {
			b.Fatal(err)
		}
	}

	for i := 0; i < b.N; i++ {
		vv := int64(i)
		ca.AddInt64(vv)
	}

	if math.Abs(ca.GetCardinality()-float64(b.N+1)) > float64(b.N/10) {
		b.Errorf("Cardinality failed: got %f, want %d.", ca.GetCardinality(), b.N+1)
	}
}
