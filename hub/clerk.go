package hub

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"time"
)

func MakeClerk() {
	addr := "127.0.0.1:50051"
	cred := grpc.WithTransportCredentials(insecure.NewCredentials())
	conn, err := grpc.Dial(addr, cred)
	if err != nil {
		log.Fatalf("connect failed to %v: %v", addr, err)
	}
	defer conn.Close()
	c := NewSClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.R(ctx, &MQ{X: 99})
	if err != nil {
		log.Fatalf("c.R failed: %v", err)
	}
	log.Printf("reply: %v", r.GetY())
}
