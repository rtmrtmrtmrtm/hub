package main

import (
	"hub/hub"
	"log"
	"os"
	"strconv"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Usage: server my-id")
	}
	myid, _ := strconv.Atoi(os.Args[1])
	if myid < 1 || myid > 3 {
		log.Fatalf("id must be smallish")
	}

	hub.MakeServer(myid)

	for {
		time.Sleep(time.Second)
	}
}
