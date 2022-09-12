package hub

import (
	"bufio"
	"context"
	"fmt"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
	"time"
)

type HubServer struct {
	n       raft.Node
	storage *raft.MemoryStorage
	c       *raft.Config
}

type server struct {
	UnimplementedSServer
}

func (s *server) R(ctx context.Context, in *MQ) (*MR, error) {
	log.Printf("Received: %v", in.GetX())
	return &MR{Y: "yyy"}, nil
}

func (s *HubServer) send_all(mm []raftpb.Message) {
	for _, m := range mm {
		out, err := m.Marshal()
		if err != nil {
			log.Printf("Marshal failed")
			continue
		}
		hostport := fmt.Sprintf("localhost:%d", 7700+m.To)
		conn, err := net.Dial("tcp", hostport)
		if err != nil {
			log.Printf("Dial failed")
			continue
		}
		conn.Write(out)
		conn.Close()
	}
}

func (s *HubServer) peer_listener() {
	hostport := fmt.Sprintf(":%d", 7700+s.c.ID)
	ln, err := net.Listen("tcp", hostport)
	if err != nil {
		log.Fatalf("cannot Listen(%v): %v", hostport, err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatalf("Accept: %v", err)
		}
		go func() {
			rr := bufio.NewReader(conn)
			all, _ := io.ReadAll(rr)
			m := &raftpb.Message{}
			err := m.Unmarshal(all)
			if err != nil {
				log.Printf("Unmarshal failed: %v", err)
			} else {
				log.Printf("!!! %v", m)
				s.n.Step(context.TODO(), *m)
			}
			conn.Close()
		}()
	}
}

func (s *HubServer) ready_loop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.Printf("tick\n")
			s.n.Tick()
		case rd := <-s.n.Ready():
			log.Printf("s.n.Ready")
			log.Printf("  SoftState %v", rd.SoftState)
			log.Printf("  HardState %v", rd.HardState)
			log.Printf("  ReadStates %v", rd.ReadStates)
			log.Printf("  Entries %v", rd.Entries)
			log.Printf("  Snapshot %v", rd.Snapshot)
			log.Printf("  CommittedEntries %v", rd.CommittedEntries)
			log.Printf("  Messages %v", rd.Messages)
			log.Printf("  MustSync %v", rd.MustSync)
			if !raft.IsEmptySnap(rd.Snapshot) {
				log.Printf("****** got a snapshot ******")
				s.storage.ApplySnapshot(rd.Snapshot)
			}
			s.storage.Append(rd.Entries)
			s.send_all(rd.Messages)
			for _, entry := range rd.CommittedEntries {
				// process(entry)
				if entry.Type == raftpb.EntryConfChange {
					var cc raftpb.ConfChange
					cc.Unmarshal(entry.Data)
					s.n.ApplyConfChange(cc)
				}
			}
			s.n.Advance()
		}
	}
}

func MakeServer(myid int) {

	var s HubServer

	s.storage = raft.NewMemoryStorage()
	s.c = &raft.Config{
		ID:              uint64(myid),
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         s.storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}
	// Set peer list to the other nodes in the cluster.
	// Note that they need to be started separately as well.
	peers := []raft.Peer{{ID: 0x01}, {ID: 0x02}, {ID: 0x03}}
	s.n = raft.StartNode(s.c, peers)

	go s.peer_listener()

	go s.ready_loop()

	// receive client RPCs
	hostport := fmt.Sprintf(":%d", 50051+myid)
	lis, err := net.Listen("tcp", hostport)
	if err != nil {
		log.Fatalf("Listen() failed: %v", err)
	}
	gs := grpc.NewServer()
	RegisterSServer(gs, &server{})

	go func() {
		log.Printf("listening at %v", lis.Addr())
		if err := gs.Serve(lis); err != nil {
			log.Fatalf("Serve() failed: %v", err)
		}
	}()

}
