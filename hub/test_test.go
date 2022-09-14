package hub

import (
	"testing"
)

func TestOne(t *testing.T) {
	s1 := MakeServer(1)
	s2 := MakeServer(2)
	s3 := MakeServer(3)

	ck := MakeClerk()

	ck.Put("xyz", "123")
	ck.Put("a", "b")

	if ck.Get("xyz") != "123" {
		t.Fatalf("key=xyz mismatch")
	}
	if ck.Get("a") != "b" {
		t.Fatalf("key=a mismatch")
	}

	ck.Put("a", "bbb")
	if ck.Get("a") != "bbb" {
		t.Fatalf("key=a 2nd mismatch")
	}

	s1.Stop()
	s2.Stop()
	s3.Stop()
}

func TestTwo(t *testing.T) {
	MakeServer(1)
	MakeServer(2)
	MakeServer(3)

	fff := func(k string, v string, ch chan bool) {
		nck := MakeClerk()
		nck.Put(k, v)
		if nck.Get(k) != v {
			t.Fatalf("fff mismatch")
		}
		ch <- true
	}

	ch := make(chan bool)
	go fff("1", "2", ch)
	go fff("2", "3", ch)
	go fff("4", "5", ch)
	<-ch
	<-ch
	<-ch
}
