package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state       string
	currentTerm int
	votedFor    int
	log         []ApplyMsg
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	lastHeartbeat   time.Time
	electionTimeout time.Duration
	votesReceived   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).

	if !rf.killed() {

		rf.mu.Lock()
		isleader = rf.state == "LEADER"
		term = rf.currentTerm
		rf.mu.Unlock()
	} else {
		term = -1
		isleader = false
	}
	log.Printf("Server %d is the leader: %v, term %v", rf.me, isleader, term)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []ApplyMsg
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendHeartbeats() {

	go func() {
		for {
			if rf.dead == 1 {
				return
			}
			if rf.state != "LEADER" {
				break
			}

			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					//log.Printf("Server %d sending AppendEntries to %d", rf.me, i)
					rf.sendAppendEntries(i)
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
}
func (rf *Raft) sendAppendEntries(server int) {

	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	reply := AppendEntriesReply{}
	go func(x int) {
		rf.peers[x].Call("Raft.AppendEntries", &args, &reply)
	}(server)
	// log.Printf("%v", ok)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//log.Printf("Server %d received AppendEntries from %d, term %d, currentTerm %d", rf.me, args.LeaderId, args.Term, rf.currentTerm)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.lastHeartbeat = time.Now()
	rf.state = "FOLLOWER"
	rf.votedFor = -1
	rf.currentTerm = args.Term
	reply.Success = true
	reply.Term = rf.currentTerm
	//log.Printf("AppendEntriesReply: server %d, %v", rf.me, reply)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (3A).
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//	log.Printf("Server %d received RequestVote from %d", rf.me, args.CandidateId)
	//log.Printf("Server %d currentTerm: %d, args.Term: %d", rf.me, rf.currentTerm, args.Term)
	//log.Printf("Server %d votedFor: %d", rf.me, rf.votedFor)
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = "FOLLOWER"
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// something about candidates log being up to date here too
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		return
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//log.Printf("Server %d sending RequestVote to %d", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	if !rf.killed() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != "LEADER" {
			isLeader = false
		}
		term = rf.currentTerm
	}

	log.Printf("command: %v", command)
	log.Printf("Start: Server %d is the leader: %v, term %v", rf.me, isLeader, term)
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	rf.state = "DEAD"
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	log.Printf("Server %d is starting an election", rf.me)
	rf.mu.Lock()
	rf.state = "CANDIDATE"
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votesReceived = 1
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	var wg sync.WaitGroup
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			wg.Add(1)
			go func(x int) {
				defer wg.Done()

				args := RequestVoteArgs{
					Term:        currentTerm,
					CandidateId: rf.me,
					// LastLogIndex and LastLogTerm should be initialized here if you have implemented log replication
				}
				reply := RequestVoteReply{}

				//	log.Printf("Server %d sending RequestVote to %d", rf.me, x)
				rf.sendRequestVote(x, &args, &reply)
				//log.Printf("Server %d received RequestVoteReply from %d", rf.me, x)
				//log.Printf("VoteGranted: %t", reply.VoteGranted)

				rf.mu.Lock()
				if reply.VoteGranted && rf.state == "CANDIDATE" && rf.currentTerm == args.Term {
					rf.votesReceived++
					if rf.votesReceived > len(rf.peers)/2 {
						log.Printf("Server %d is now the leader", rf.me)
						rf.state = "LEADER"
						// Additional code to handle transition to leader, e.g., sending initial empty AppendEntries to all followers
					}
				}
				rf.mu.Unlock()
			}(i)
		}
	}
	wg.Wait() // Correct placement of wg.Wait(), outside the for-loop
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// check if we need to start an election
		if rf.state == "FOLLOWER" || rf.state == "CANDIDATE" {
			// check if we need to start an election
			elapsed := time.Since(rf.lastHeartbeat)
			//log.Printf("Server %d elapsed time: %v", rf.me, elapsed)
			if elapsed >= rf.electionTimeout {
				rf.startElection()
			}
		}
		rf.mu.Lock()
		if rf.state == "LEADER" {
			rf.sendHeartbeats()

		}
		rf.mu.Unlock()
		ms := 300 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = "FOLLOWER"
	rf.currentTerm = 0
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = time.Duration(300+(rand.Int63()%300)) * time.Millisecond
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
