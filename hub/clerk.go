package hub

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
)

type Clerk struct {
	conn *grpc.ClientConn
	c    SClient
}

func MakeClerk() *Clerk {
	ck := &Clerk{}
	return ck
}

func (ck *Clerk) connect() {
	if ck.conn == nil {
		addr := "127.0.0.1:50051"
		cred := grpc.WithTransportCredentials(insecure.NewCredentials())
		conn, err := grpc.Dial(addr, cred)
		if err != nil {
			log.Fatalf("connect failed to %v: %v", addr, err)
		}
		// defer conn.Close()
		c := NewSClient(conn)

		ck.conn = conn
		ck.c = c
	}
}

func (ck *Clerk) Get(key string) string {
	ck.connect()

	ctx := context.TODO()

	r, err := ck.c.Get(ctx, &GetQ{Key: key})
	if err != nil {
		log.Fatalf("Clerk Get failed: %v", err)
	}
	return r.GetValue()
}

func (ck *Clerk) Put(key string, value string) {
	ck.connect()

	ctx := context.TODO()

	_, err := ck.c.Put(ctx, &PutQ{Key: key, Value: value})
	if err != nil {
		log.Fatalf("Clerk Put failed: %v", err)
	}
}
