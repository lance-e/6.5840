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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	Follower int32 = iota
	Candidate
	Leader
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
	Index   int
	Term    int
	Command interface{}
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
	state            int32
	currentTerm      int
	votedFor         int
	timer            time.Time
	heartbeatTimeout chan struct{}
	electionTimeout  chan struct{}
	log              []LogEntry
	commitIndex      int
	lastApplied      int
	nextIndex        []int //for leader: all server's next index
	matchIndex       []int //for leader: the lastest repliacted entry index in all server
	applyCh          chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	/*  rf.mu.Lock() */
	/* defer rf.mu.Unlock() */
	/*  */
	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	}

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
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	//
	if rf.currentTerm > args.Term {
		return
	}
	//
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1 // clear votedFor
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {

		//election limit
		lastIndex := len(rf.log) - 1
		if len(rf.log) > 0 && (rf.log[lastIndex].Term > args.LastLogTerm || (rf.log[lastIndex].Term == args.LastLogTerm && rf.log[lastIndex].Index > args.LastLogIndex)) {
			return
		}
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.timer = time.Now()
	}
	return
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int //leader committed log index
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("AppendEntries: before append--->raft[%d]'s entries [%v]", rf.me, rf.log)

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
	}

	//refresh heartbeat
	rf.SetState(Follower)

	//Consistency Check(TODO:more graceful)
	lastEntry := len(rf.log) - 1
	if len(rf.log) == 0 && args.PreLogIndex != 0 {
		reply.ConflictIndex = 1
		return
	}
	if len(rf.log) > 0 && args.PreLogIndex > rf.log[lastEntry].Index {
		reply.ConflictIndex = rf.log[lastEntry].Index + 1
		return
	}
	if len(rf.log) > 0 && args.PreLogIndex > 0 && args.PreLogTerm != rf.log[args.PreLogIndex-1].Term {
		DPrintf("!!!!!!raft[%d] %d,%d,%d,%d\n", rf.me, args.PreLogIndex, rf.log[lastEntry].Index, args.PreLogTerm, rf.log[lastEntry].Term)
		reply.ConflictTerm = rf.log[args.PreLogIndex-1].Term
		for i := 1; i <= args.PreLogIndex; i++ {
			if rf.log[i-1].Term == reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}
		DPrintf("Consistency Check: ConflictIndex-[%d] , ConflictTerm-[%d]\n", reply.ConflictIndex, reply.ConflictTerm)
		return
	}
	reply.Success = true

	//handle log replication
	index := args.PreLogIndex
	for i, entry := range args.Entries {
		index++
		if index <= len(rf.log) {
			if rf.log[index-1].Term == entry.Term {
				continue
			}
			rf.log = rf.log[:index-1]
		}
		rf.log = append(rf.log, args.Entries[i:]...)
		// DPrintf("AppendEntries: after append--->raft[%d]'s entries [%v]", rf.me, rf.log)

		break
	}

	//commit follower's log entries
	// DPrintf("AppendEntries:rf.commitIndex[%d] -- args.LeaderCommit[%d]\n", rf.commitIndex, args.LeaderCommit)
	if rf.commitIndex < args.LeaderCommit {
		lastIndex := rf.log[len(rf.log)-1].Index
		if args.LeaderCommit > lastIndex {
			//mean follower doesn't have complete entries,so only commit exist entries
			rf.commitIndex = lastIndex
		} else {
			//mean follower have complete entries , so directly commit
			rf.commitIndex = args.LeaderCommit
		}

		rf.Apply()
	}

	//refresh hearbeat
	rf.SetState(Follower)
	return
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
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	if rf.killed() {
		return index, term, isLeader
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term, isLeader = rf.GetState()
	if isLeader {
		index = len(rf.log) + 1
		entry := LogEntry{
			Term:    term,
			Index:   index,
			Command: command,
		}
		rf.log = append(rf.log, entry)
		rf.nextIndex[rf.me] = rf.log[len(rf.log)-1].Index + 1
		rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].Index
		DPrintf("<<<<<<<<<<<< a log entry to raft--[%d],index[%d] , entry--[%d,%d]\n", rf.me, index, entry.Index, entry.Term)
	}
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) SetState(newState int32) {
	atomic.StoreInt32(&rf.state, newState)
	rf.timer = time.Now()
	switch newState {
	case Follower:
	case Candidate:
		rf.currentTerm++
		rf.votedFor = rf.me
	case Leader:
		rf.BroadcastHeartBeat()
		for i, _ := range rf.peers {
			if len(rf.log) > 0 {
				rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
			} else {
				rf.nextIndex[i] = 1
			}
			rf.matchIndex[i] = 0
		}
	}
}

func (rf *Raft) Election() {

	rf.SetState(Candidate)
	var count int32 = 1

	for i, _ := range rf.peers {
		if i != rf.me {
			go rf.BroadcastRequestVote(i, &count)
		}
	}

}

func (rf *Raft) BroadcastRequestVote(target int, count *int32) {
	lastLogIndex := 0
	lastLogTerm := 0
	if len(rf.log) > 0 {
		lastLogIndex = rf.log[len(rf.log)-1].Index
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	reply := &RequestVoteReply{}
	if rf.sendRequestVote(target, args, reply) {
		if reply.Term > rf.currentTerm {
			rf.SetState(Follower)
			rf.currentTerm = reply.Term
			return
		}
		if reply.VoteGranted {
			// DPrintf("server[%d] voted for server[%d] success\n", target, rf.me)
			atomic.AddInt32(count, 1)
		} else {
			// DPrintf("server[%d] vote server[%d] failed\n", target, rf.me)
		}
	} else {
		// DPrintf("RequestVote can't send, raft[%d--%d]\n", target, rf.me)
	}
	if atomic.LoadInt32(count) >= int32(len(rf.peers)/2+1) {
		rf.SetState(Leader)
		DPrintf("raft[%d] become leader, count = %d\n", rf.me, *count)
		// rf.BroadcastHeartBeat()
	}
	return
}

func (rf *Raft) BroadcastHeartBeat() {
	rf.timer = time.Now()

	DPrintf("BroadcastHeartBeat: now leader[%d]'s entries :[%v]\n", rf.me, rf.log)
	for i, _ := range rf.peers {
		if i != rf.me {
			go rf.BuildAppendEntries(i)
		}
	}
}

func (rf *Raft) BuildAppendEntries(i int) {
	entries := make([]LogEntry, 0)
	preLogIndex := 0
	preLogTerm := rf.currentTerm
	if rf.nextIndex[i]-1 >= 0 {
		preLogIndex = rf.nextIndex[i] - 1
	}
	DPrintf("raft[%d]'s state: PreLogIndex-[%d] , nextIndex-[%d]\n", i, preLogIndex, rf.nextIndex[i])
	if len(rf.log) > 0 && preLogIndex > 0 {
		preLogTerm = rf.log[preLogIndex-1].Term
	}
	// ???
	if preLogIndex < len(rf.log) {
		entries = rf.log[preLogIndex:]
	}

	DPrintf("args's entries to server[%d]:[%v] \n", i, entries)

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PreLogIndex:  preLogIndex,
		PreLogTerm:   preLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	reply := &AppendEntriesReply{}
	if rf.sendAppendEntries(i, args, reply) {
		if reply.Term > rf.currentTerm {
			rf.SetState(Follower)
			rf.currentTerm = reply.Term
			return
		}
		if reply.Success {
			DPrintf("raft[%d]'s appendEntries to raft[%d] successful\n", rf.me, i)
			if len(entries) == 0 {
				return
			}
			rf.nextIndex[i] = preLogIndex + len(entries) + 1
			DPrintf("UUUUpdate: raft[%d]'s nextIndex is [%d] ,entries' length is [%d]\n", i, rf.nextIndex[i], len(entries))
			rf.matchIndex[i] = preLogIndex + len(entries)

			if atomic.LoadInt32(&rf.state) == Leader {
				rf.updateCommitIndex()
			}
		} else {
			DPrintf("raft[%d]'s appendEntries to raft[%d] not success\n", rf.me, i)
			if reply.ConflictTerm == -1 {
				rf.nextIndex[i] = reply.ConflictIndex
			} else {
				tempIndex := -1
				for _, entry := range rf.log {
					if entry.Term == reply.ConflictTerm {
						tempIndex = entry.Index
					}
				}
				if tempIndex != -1 {
					rf.nextIndex[i] = tempIndex + 1
				} else {
					rf.nextIndex[i] = reply.ConflictIndex
				}
			}
			DPrintf("Consistency Check: CIndex-[%d] , CTerm-[%d] , nextIndex----[%d]\n", reply.ConflictIndex, reply.ConflictTerm, rf.nextIndex[i])
		}
	} else {
		// DPrintf("appendEntries can't sent, raft[%d--%d]", rf.me, i)
		return
	}

}
func (rf *Raft) updateCommitIndex() {
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term == rf.currentTerm && rf.countVotes(i+1) > len(rf.peers)/2 {
			if rf.commitIndex == rf.log[i].Index {
				break
			}
			rf.commitIndex = rf.log[i].Index
			DPrintf("??????Leader[%d] update commitIndex--[%d]\n", rf.me, rf.commitIndex)
			rf.Apply()
			break
		}
	}
}

func (rf *Raft) countVotes(index int) int {
	count := 0
	for i := 0; i < len(rf.peers); i++ {
		if rf.matchIndex[i] >= index {
			count++
		}
	}
	return count
}

func (rf *Raft) Apply() {
	if rf.lastApplied >= rf.commitIndex {
		DPrintf("Apply: wrong !!! raft[%d]\n", rf.me)
		return
	}
	// DPrintf("Apply: raft[%d] lastApplied--[%d] , commitIndex--[%d]\n", rf.me, rf.lastApplied, rf.commitIndex)
	entries := rf.log[rf.lastApplied:rf.commitIndex]
	for _, entry := range entries {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.Index,
		}
		if atomic.LoadInt32(&rf.state) == Leader {
			DPrintf("Apply: leader  [%d] apply command-- :lastApplied--[%d], commandIndex--[%d], commandTerm--[%d]\n", rf.me, rf.lastApplied, entry.Index, entry.Term)
		} else {
			DPrintf("Apply: follower[%d] apply command-- :lastApplied--[%d], commandIndex--[%d], commandTerm--[%d]\n", rf.me, rf.lastApplied, entry.Index, entry.Term)
		}
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		select {
		case <-rf.electionTimeout:
			rf.Election()
		case <-rf.heartbeatTimeout:
			rf.BroadcastHeartBeat()

			// default:
			// pause for a random amount of time between 50 and 350
			// milliseconds.
			/* ms := 50 + (rand.Int63() % 300) */
			/* time.Sleep(time.Duration(ms) * time.Millisecond) */

		}
	}
	DPrintf("raft[%d] was stop!!!!\n", rf.me)
}

func (rf *Raft) Timer() {
	for !rf.killed() {

		switch atomic.LoadInt32(&rf.state) {
		case Follower, Candidate:
			if time.Now().After(rf.timer.Add(time.Duration(500+rand.Intn(501)) * time.Millisecond)) {
				rf.timer = time.Now()
				rf.electionTimeout <- struct{}{}
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		case Leader:
			if time.Now().After(rf.timer.Add(100 * time.Millisecond)) {
				rf.timer = time.Now()
				rf.heartbeatTimeout <- struct{}{}
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}

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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	/* rf.state = Follower */
	/* rf.timer = time.Now() */
	rf.SetState(Follower)
	rf.currentTerm = 1
	rf.votedFor = -1
	rf.heartbeatTimeout = make(chan struct{})
	rf.electionTimeout = make(chan struct{})
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers)+1)
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	//start timer
	go rf.Timer()

	return rf
}
