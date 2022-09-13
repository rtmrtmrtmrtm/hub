package hub

import (
	"bufio"
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type CommandType int

const (
	CommandPut CommandType = 1
	CommandGet             = 2
)

// this is what we propose to Raft.
type Command struct {
	ID    uint64 // so proposer can tell when (if ever) it was committed.
	Type  CommandType
	Key   string
	Value string
}

// track the status of a proposed Command.
type CommandStatus struct {
	done  bool
	err   error
	value string
}

type HubServer struct {
	UnimplementedSServer
	mu             sync.Mutex
	n              raft.Node
	storage        *raft.MemoryStorage
	c              *raft.Config
	nextCommandID  uint64
	scoreboard     map[uint64]*CommandStatus
	scoreboardCond *sync.Cond
	db             map[string]string // the data !!!
}

func (s *HubServer) Get(ctx context.Context, in *GetQ) (*GetR, error) {
	log.Printf("Received: Get(%v)", in.GetKey())

	s.mu.Lock()

	cmd := Command{
		ID:   s.nextCommandID,
		Type: CommandGet,
		Key:  in.GetKey(),
	}

	s.nextCommandID += 1

	// XXX duplicate request suppression!

	// XXX need to make sure we delete the scoreboard entry
	// no matter what.

	st := &CommandStatus{
		done:  false,
		err:   nil,
		value: "",
	}

	s.scoreboard[cmd.ID] = st

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(cmd); err != nil {
		log.Fatalf("Get Encode failed: %v", err)
	}

	s.mu.Unlock()

	s.n.Propose(ctx, buf.Bytes())

	// XXX might be more efficient to have a Cond per CommandStatus.

	// wait for process() to see that Raft committed our Command.
	// XXX it might never commit, e.g. if leader crashed.
	s.mu.Lock()
	for {
		if st.done {
			v := st.value
			delete(s.scoreboard, cmd.ID)
			s.mu.Unlock()
			return &GetR{Value: v}, nil
		}
		s.scoreboardCond.Wait()
	}
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
			// log.Printf("Dial failed")
			continue
		}
		conn.Write(out)
		conn.Close()
	}
}

func (s *HubServer) process(e raftpb.Entry) {
	if e.Type == raftpb.EntryNormal && len(e.Data) > 0 {
		var cmd Command
		dec := gob.NewDecoder(bytes.NewBuffer(e.Data))
		if err := dec.Decode(&cmd); err != nil {
			log.Fatalf("process Decode failed: %v", err)
		}
		st, ok := s.scoreboard[cmd.ID]
		if ok == false {
			log.Printf("hmm, process but no scoreboard[%v]", cmd.ID)
		} else if st.done == true {
			log.Printf("hmm, process but scoreboard[%v].done = true", cmd.ID)
		} else if cmd.Type == CommandGet {
			st.value = s.db[cmd.Key]
			st.done = true
			s.scoreboardCond.Broadcast()
		} else if cmd.Type == CommandPut {
			s.db[cmd.Key] = cmd.Value
			st.done = true
			s.scoreboardCond.Broadcast()
		} else {
			log.Fatalf("process: invalid cmd.Type %v", cmd.Type)
		}
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
				s.n.Step(context.TODO(), *m)
			}
			conn.Close()
		}()
	}
}

func (s *HubServer) ready_loop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.n.Tick()
		case rd := <-s.n.Ready():
			//log.Printf("%s", raft.DescribeReady(rd, nil))
			if !raft.IsEmptySnap(rd.Snapshot) {
				log.Printf("****** got a snapshot ******")
				s.storage.ApplySnapshot(rd.Snapshot)
			}
			s.storage.Append(rd.Entries)
			s.send_all(rd.Messages)
			for _, entry := range rd.CommittedEntries {
				s.process(entry)
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

	s.scoreboard = map[uint64]*CommandStatus{}
	s.scoreboardCond = sync.NewCond(&s.mu)
	s.db = map[string]string{}

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
	hostport := fmt.Sprintf(":%d", 50050+myid)
	lis, err := net.Listen("tcp", hostport)
	if err != nil {
		log.Fatalf("Listen() failed: %v", err)
	}
	gs := grpc.NewServer()
	RegisterSServer(gs, &s)

	go func() {
		log.Printf("listening at %v", lis.Addr())
		if err := gs.Serve(lis); err != nil {
			log.Fatalf("Serve() failed: %v", err)
		}
	}()
}
