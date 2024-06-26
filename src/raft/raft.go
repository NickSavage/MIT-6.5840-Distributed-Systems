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
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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

type LogEntry struct {
	Command ApplyMsg
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state       string
	currentTerm int
	votedFor    int
	logs        []LogEntry
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
	//	log.Printf("persist: running persist()")
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var logs []LogEntry

	//	var votedFor int
	d.Decode(&term)
	d.Decode(&votedFor)
	d.Decode(&logs)
	log.Printf("persist: term %v", term)
	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.logs = logs
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = "FOLLOWER"
		rf.persist()
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// something about candidates log being up to date here too
		log.Printf("Server %d: len(rf.logs): %d, args.LastLogIndex: %d", rf.me, len(rf.logs), args.LastLogIndex)

		lastLogIndex := len(rf.logs) - 1
		lastLogTerm := rf.logs[lastLogIndex].Term
		isLogUpToDate := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

		if isLogUpToDate {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()
			return
		}
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	log.Printf("Server %d is starting an election", rf.me)
	rf.mu.Lock()
	rf.state = "CANDIDATE"
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()
	rf.votesReceived = 1
	rf.lastHeartbeat = time.Now()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(x int) {
				args := RequestVoteArgs{
					Term:         currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.logs) - 1,
					LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
				}
				reply := RequestVoteReply{}

				rf.sendRequestVote(x, &args, &reply)

				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.persist()
					rf.state = "FOLLOWER"
					rf.votedFor = -1
				}
				if reply.VoteGranted && rf.state == "CANDIDATE" && rf.currentTerm == args.Term {
					rf.votesReceived++
				}
				rf.mu.Unlock()
				if rf.state == "CANDIDATE" && rf.votesReceived > len(rf.peers)/2 {
					log.Printf("Server %d is now the leader", rf.me)
					log.Printf("term: %d", rf.currentTerm)
					rf.mu.Lock()
					rf.state = "LEADER"
					// Additional code to handle transition to leader, e.g., sending initial empty AppendEntries to all followers
					for i := 0; i < len(rf.peers); i++ {
						if i != rf.me {
							rf.nextIndex[i] = len(rf.logs)
							rf.matchIndex[i] = 0
						}
					}
					rf.matchIndex[rf.me] = len(rf.logs) - 1
					rf.mu.Unlock()
					if rf.state == "LEADER" {
						rf.sendHeartbeats()
					}
				}
			}(i)
		}
	}
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
	Term       int
	Success    bool
	NextIndex  int
	MatchIndex int
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.logs)
	term := rf.currentTerm
	isLeader := true

	// Your code here (3B).

	if rf.killed() {
		return -1, -1, false
	}
	if rf.state != "LEADER" {
		isLeader = false
		return index, term, isLeader
	}

	log.Printf("server %d: command: %v", rf.me, command)

	newMessage := ApplyMsg{
		Command:      command,
		CommandIndex: index,
		CommandValid: true,
	}

	logEntry := LogEntry{
		Command: newMessage,
		Term:    term,
	}
	rf.logs = append(rf.logs, logEntry)
	rf.lastApplied++

	log.Printf("server %d: logs %v", rf.me, rf.logs)
	go func() {
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(x int) {
					//log.Printf("server %d: sent append entries to %d", rf.me, x)
					ok := rf.sendAppendEntries(x, index)
					log.Printf("server %d: response from %d: %v", rf.me, x, ok)
				}(i)
			}
		}
	}()
	return index, term, isLeader
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
					//					log.Printf("server %d sent heartbeat to %d", rf.me, i)
					go rf.sendAppendEntries(i, -1)
				}
			}
			time.Sleep(150 * time.Millisecond)
		}
	}()
}
func (rf *Raft) sendAppendEntries(server int, index int) bool {

	var heartbeat bool
	var lastIndex int

	if index != -1 {
		heartbeat = false
	} else {
		heartbeat = true
		index = len(rf.logs) - 1
	}

	// make an empty array to store entries to send
	var result bool

	for {
		rf.mu.Lock()
		lastIndex = rf.nextIndex[server] - 1
		entries := make([]ApplyMsg, 0)
		if !heartbeat {
			if index >= rf.nextIndex[server] {
				for i := rf.nextIndex[server]; i <= index; i++ {
					entries = append(entries, rf.logs[i].Command)
				}
				rf.nextIndex[server] = index + 1
			}

		}
		if lastIndex < 0 {
			lastIndex = 0
		}
		log.Printf("server %d: index %v, heartbeat %v, entries %v", rf.me, index, heartbeat, entries)
		log.Printf("server %d: sending entries to %d: %v", rf.me, server, entries)
		log.Printf("server %d: lastIndex: %d, server %d nextIndex: %d", rf.me, lastIndex, server, rf.nextIndex[server])
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: lastIndex,
			PrevLogTerm:  rf.logs[lastIndex].Term,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()
		reply := AppendEntriesReply{}

		rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
		if !heartbeat {
			log.Printf("server %d: response from %d: %v", rf.me, server, reply.Success)
		}
		if !reply.Success {
			if reply.Term > rf.currentTerm {
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				rf.state = "FOLLOWER"
				rf.votedFor = -1
				rf.persist()
				rf.mu.Unlock()
				break
			}
			if reply.Term == 0 {
				// I think the call has failed here
				log.Printf("Server %d hasn't responded", server)
				break
			}
			// if lastIndex >= nextIndex, we have an inconsistency, otherwise we're dealing with heartbeats
			rf.mu.Lock()
			rf.nextIndex[server]--
			rf.mu.Unlock()
			//	log.Printf("Log inconsistency with server %d, we're going to try again", server)
		}
		if reply.Success {

			// TODO this is wrong! we're setting nextIndex when we absolutely should not be
			log.Printf("server %d: setting nextIndex for %d to %d", rf.me, server, reply.NextIndex)
			log.Printf("server %d: heartbeat: %v", rf.me, heartbeat)
			rf.mu.Lock()
			rf.matchIndex[server] = reply.MatchIndex
			rf.nextIndex[server] = reply.NextIndex
			log.Printf("server %d: server %d matchIndex %d nextIndex %d", rf.me, server, reply.MatchIndex, reply.NextIndex)
			rf.mu.Unlock()
			result = reply.Success
			break
		}
	}
	return result
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//log.Printf("is this thing on?")
	reply.Term = rf.currentTerm
	reply.NextIndex = len(rf.logs)
	reply.MatchIndex = rf.lastApplied
	if rf.state == "LEADER" {
		if args.Term > rf.currentTerm {
			rf.state = "FOLLOWER"
			rf.votedFor = -1
		}
	}
	log.Printf("server %d: received append entries from %d", rf.me, args.LeaderId)
	log.Printf("Server %d: args.Term: %d, rf.currentTerm: %d", rf.me, args.Term, rf.currentTerm)
	log.Printf("server %d: len(rf.logs) %d, args.PrevLogIndex %d", rf.me, len(rf.logs), args.PrevLogIndex)
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if args.PrevLogIndex >= len(rf.logs) || args.PrevLogIndex < 0 {
		reply.Success = false
		return
	}
	log.Printf("server %d: rf.logs[args.PrevLogIndex].Term %d, args.PrevLogTerm %d", rf.me, rf.logs[args.PrevLogIndex].Term, args.PrevLogTerm)

	// Check if log term at PrevLogIndex matches args.PrevLogTerm
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	reply.Success = true
	rf.lastHeartbeat = time.Now()
	rf.currentTerm = args.Term
	rf.persist()

	//	if len(args.Entries) > 0 {
	log.Printf("server %d: received entries from %d: %v", rf.me, args.LeaderId, args.Entries)
	log.Printf("server %d: PrevLogIndex: %d, len(rf.logs): %d", rf.me, args.PrevLogIndex, len(rf.logs))

	//	}
	// delete conflicting entries
	log.Printf("server %d: entries before %v", rf.me, rf.logs)
	nextIndex := args.PrevLogIndex + 1
	if nextIndex < len(rf.logs) {
		if rf.logs[nextIndex].Term != args.Term {
			rf.logs = rf.logs[:nextIndex] // Remove conflicting entries
			rf.persist()
			rf.lastApplied = len(rf.logs) - 1
		}
	}
	log.Printf("server %d: entries after %v", rf.me, rf.logs)
	// Append new entries not already in the log
	for _, entry := range args.Entries {
		logEntry := LogEntry{
			Command: entry,
			Term:    args.Term,
		}

		if nextIndex >= len(rf.logs) {
			rf.logs = append(rf.logs, logEntry)
			rf.persist()
			rf.lastApplied = len(rf.logs) - 1
			log.Printf("server %d: tocommit, appended %v", rf.me, logEntry)
			log.Printf("server %d: log status: %v", rf.me, rf.logs)
		} else {
			// If already the same log exists, no need to append
			if rf.logs[nextIndex].Term != args.Term {
				rf.logs[nextIndex] = logEntry
			}
		}
		nextIndex++
	}
	reply.NextIndex = len(rf.logs)
	reply.MatchIndex = rf.lastApplied
	//log.Printf("args.LeaderCommit: %d, rf.commitIndex: %d", args.LeaderCommit, rf.commitIndex)
	log.Printf("server %d: looking at committing", rf.me)
	if args.LeaderCommit > rf.commitIndex {
		toCommit := args.LeaderCommit
		if toCommit > rf.lastApplied {
			toCommit = rf.lastApplied
		}
		log.Printf("server %d: toCommit %v, args.LeaderCommit %v, rf.commitIndex %v", rf.me, toCommit, args.LeaderCommit, rf.commitIndex)
		log.Printf("server %d: logs: %v", rf.me, rf.logs)
		for i := rf.commitIndex; i <= toCommit; i++ {
			log.Printf("server %d: committed %v", rf.me, rf.logs[i].Command)

			rf.applyCh <- rf.logs[i].Command
			rf.persist()
			rf.commitIndex = i
		}
	}
	log.Printf("server %d: reply to %d: %v", rf.me, args.LeaderId, reply.Success)
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

func (rf *Raft) ticker() {
	for rf.killed() == false {

		if rf.state == "FOLLOWER" || rf.state == "CANDIDATE" {
			elapsed := time.Since(rf.lastHeartbeat)
			//log.Printf("Server %d elapsed time: %v", rf.me, elapsed)
			if elapsed >= rf.electionTimeout {
				// TODO: the paper says something about not calling an election if we've already voted in this term
				rf.startElection()
			}
		}
		if rf.state == "LEADER" {
			// committing

			log.Printf("server %d: checking on leader commits", rf.me)
			rf.mu.Lock()
			log.Printf("server %d: commitIndex %v, lastApplied %v", rf.me, rf.commitIndex, rf.lastApplied)
			log.Printf("server %d: commit matchIndex %v", rf.me, rf.matchIndex)
			for i := rf.commitIndex + 1; i <= rf.lastApplied; i++ {
				commits := 1
				for peer := range rf.peers {
					if rf.matchIndex[peer] >= i {
						commits++
					}
				}
				if commits > len(rf.peers)/2 {

					//TODO this issue with this is that it treats commitIndex like it is 0 indexed, when it should be 1 indexed. FailNoAgree isn't working becuase of that
					// need to figure out how to convert this to a 1 indexed, it wasn't obvious when I tried it
					message := rf.logs[i].Command
					log.Printf("server %d: leader committed %v", rf.me, message)
					log.Printf("server %d: logs: %v", rf.me, rf.logs)
					rf.applyCh <- message
					rf.persist()
					if !(i == 0) {
						rf.commitIndex++

					}
				}
			}
			rf.mu.Unlock()

		}
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
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.state = "FOLLOWER"
	rf.currentTerm = 0
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = time.Duration(300+(rand.Int63()%300)) * time.Millisecond
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 0
	}
	rf.matchIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 0
	}

	rf.logs = make([]LogEntry, 0)
	rf.logs = append(rf.logs, LogEntry{Command: ApplyMsg{CommandValid: false}, Term: 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = len(rf.logs) - 1
	rf.lastApplied = len(rf.logs) - 1

	log.Printf("persist: server %d, term %d, votedFor %d, logs %v", rf.me, rf.currentTerm, rf.votedFor, rf.logs)
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
