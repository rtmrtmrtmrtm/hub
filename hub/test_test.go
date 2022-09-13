package hub

import (
	"testing"
)

func TestOne(t *testing.T) {
	MakeServer(1)
	MakeServer(2)
	MakeServer(3)

	ck := MakeClerk()

	ck.Put("xyz", "123")
	ck.Put("a", "b")

	if ck.Get("xyz") != "123" {
		t.Fatalf("key=xyz mismatch")
	}
	if ck.Get("a") != "b" {
		t.Fatalf("key=a mismatch")
	}
}
