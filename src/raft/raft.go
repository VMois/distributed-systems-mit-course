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
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type logEntry struct {
	Command interface{}
	Term    int
}

const (
	candidate = "candidate"
	follower  = "follower"
	leader    = "leader"

	logging = false
)

// Raft peer
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// persistent state
	currentTerm int
	votedFor    int // candidate id, -1 indicates null or "No vote"
	log         []logEntry

	// votalite state on all servers
	commitIndex int
	lastApplied int

	// votalite state on leaders
	nextIndex  map[int]int
	matchIndex map[int]int

	// additional variables
	electionTimeout int    // in ms
	role            string // constants candidate, follower, leader
	votesNum        int
	applyCh         chan ApplyMsg
}

// choose randomly new electionTimeout
// not thread safe
func (rf *Raft) resetElectionTimeout() {
	// timeout between 150-300ms, taken from the original Raft extended paper
	rf.electionTimeout = rand.Intn(300-150) + 150
}

// GetState returns currentTerm and whether this server
// believes it is the leader, thread safe
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool

	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.role == leader
	rf.mu.Unlock()

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

// not thread safe
func (rf *Raft) getMajorityServersNumber() int {
	return int(math.Round(float64(len(rf.peers)) / 2.0))
}

func (rf *Raft) applyNewEntriesProcess() {
	const defaultPeriod = 10

	rf.mu.Lock()
	prevCommitIndex := rf.commitIndex
	rf.mu.Unlock()

	for !rf.killed() {
		rf.mu.Lock()
		if rf.role == leader && ((len(rf.log) - 1) > rf.commitIndex) {

			// Raft (Extended), fig. 2, Rules for Servers/Leades
			n := rf.commitIndex + 1
			for n <= len(rf.log)-1 {
				if rf.log[n].Term != rf.currentTerm {
					break
				}

				count := 1
				for i := range rf.peers {
					if (i != rf.me) && (rf.matchIndex[i] >= n) {
						count++
					}
				}

				// no majority
				if count < rf.getMajorityServersNumber() {
					break
				}
				n++
			}
			rf.commitIndex = n - 1
		}

		if rf.commitIndex > prevCommitIndex {
			if logging {
				fmt.Println(rf.me, " applies new entries. Term: ", rf.currentTerm)
			}
			for i, entry := range rf.log[prevCommitIndex+1 : rf.commitIndex+1] {
				rf.applyCh <- ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: prevCommitIndex + 1 + i}
			}
			prevCommitIndex = rf.commitIndex
		}
		rf.mu.Unlock()

		time.Sleep(time.Duration(defaultPeriod) * time.Millisecond)
	}
}

// AppendEntriesArgs RPC argument struct. Fields must be in caps
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

// AppendEntriesReply RPC reply struct. Fields must be in caps
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries handles hearbeats and new log entries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.checkTerm(args.Term)

	reply.Term = rf.currentTerm

	// request comes from the "past" term, ignore request
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.resetElectionTimeout()

	if (len(rf.log) - 1) < args.PrevLogIndex {
		reply.Success = false
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	if (len(rf.log) > args.PrevLogIndex) &&
		(rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false
		return
	}

	if logging {
		if len(args.Entries) > 0 {
			fmt.Println(rf.me, " received new entries from ", args.LeaderID, "Term:", rf.currentTerm, "Len: ", len(args.Entries))
		} else {
			fmt.Println(rf.me, " received heartbeat from ", args.LeaderID, "Term: ", rf.currentTerm)
		}
	}

	for i, newEntry := range args.Entries {
		newIndex := args.PrevLogIndex + i + 1

		// some entries exist after newIndex
		if len(rf.log) > newIndex {
			if newEntry.Term != rf.log[newIndex].Term {
				rf.log = rf.log[:newIndex-1]
				rf.log = append(rf.log, newEntry)
			}
		} else {
			rf.log = append(rf.log, newEntry)
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		// choose min, code here needs improvement
		if args.LeaderCommit < len(rf.log)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log) - 1
		}
	}

	reply.Success = true
}

func (rf *Raft) sendNewLogEntries(serverID int, nextIndexForServer int) {
	args := AppendEntriesArgs{}

	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.LeaderID = rf.me

	prevLogIndex := nextIndexForServer - 1
	lastLogIndex := len(rf.log) - 1
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = rf.log[prevLogIndex].Term

	args.LeaderCommit = rf.commitIndex
	args.Entries = rf.log[nextIndexForServer : lastLogIndex+1]
	rf.mu.Unlock()

	if logging {
		if len(args.Entries) > 0 {
			fmt.Println(rf.me, " is sending new logs for ", serverID, " starting ", nextIndexForServer)
		}
	}

	reply := AppendEntriesReply{}
	ok := rf.peers[serverID].Call("Raft.AppendEntries", &args, &reply)
	rf.mu.Lock()
	if ok {
		if logging {
			if len(args.Entries) > 0 {
				fmt.Println(rf.me, " receives append response from ", serverID, "Status:", reply.Success, "Start:", nextIndexForServer, "Term: ", rf.currentTerm)
			}
		}
		rf.checkTerm(reply.Term)

		if reply.Success {
			rf.nextIndex[serverID] = lastLogIndex + 1
			rf.matchIndex[serverID] = lastLogIndex
		} else {
			rf.nextIndex[serverID]--
		}
	}
	rf.mu.Unlock()
}

// check incoming term, convert to follower if needed
// not thread safe
func (rf *Raft) checkTerm(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.role = follower
		rf.votedFor = -1
	}
}

// not thread safe
func (rf *Raft) getNewEntries(prevLogIndex int) []logEntry {
	return rf.log[prevLogIndex+1 : len(rf.log)]
}

// not thread safe
func (rf *Raft) getLastLogEntry() (lastLogEntry logEntry, lastLogIndex int) {
	lastIndex := len(rf.log) - 1
	if lastIndex >= 0 {
		return rf.log[lastIndex], lastIndex
	}
	return logEntry{Command: nil, Term: 0}, -1
}

func (rf *Raft) appendNewLogEntriesProcess() {
	const defaultPeriod = 20

	for !rf.killed() {
		rf.mu.Lock()
		if rf.role == leader {
			lastLogIndex := len(rf.log) - 1
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				if lastLogIndex >= rf.nextIndex[i] {
					go rf.sendNewLogEntries(i, rf.nextIndex[i])
				} else {
					// heartbeat
					go rf.sendNewLogEntries(i, lastLogIndex+1)
				}
			}
		}
		rf.mu.Unlock()

		time.Sleep(time.Duration(defaultPeriod) * time.Millisecond)
	}
}

// not thread safe
func (rf *Raft) initIndexMaps() {
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
}

// RequestVoteArgs RPC argument struct. Fields must be in caps
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply RPC reply structure. Fields must be in caps
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote receives a vote request
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.checkTerm(args.Term)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if (args.Term == rf.currentTerm) && ((rf.votedFor == -1) || (rf.votedFor == args.CandidateID)) {
		lastLogEntry, lastLogIndex := rf.getLastLogEntry()
		if (lastLogEntry.Term < args.LastLogTerm) ||
			((lastLogEntry.Term == args.LastLogTerm) && (lastLogIndex <= args.LastLogIndex)) {
			rf.votedFor = args.CandidateID
			rf.resetElectionTimeout()
			reply.VoteGranted = true
		}
	}
}

func (rf *Raft) sendRequestVote(args RequestVoteArgs, server int) {
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)

	rf.mu.Lock()
	if ok {
		rf.checkTerm(reply.Term)

		if reply.VoteGranted && rf.role == candidate {
			rf.votesNum++

			// if majority votes, become leader
			if rf.votesNum >= rf.getMajorityServersNumber() {
				if logging {
					fmt.Println("Newly elected leader: ", rf.me, " on term ", rf.currentTerm)
				}
				rf.role = leader
				rf.initIndexMaps()
				rf.resetElectionTimeout()
			}
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) electionProcess() {
	const defaultPeriod = 50
	period := defaultPeriod

	for !rf.killed() {
		rf.mu.Lock()

		if rf.role != leader {
			rf.electionTimeout -= period
		}

		if rf.electionTimeout <= 0 {
			rf.startElection()
		}

		if rf.electionTimeout > defaultPeriod {
			period = defaultPeriod
		} else {
			period = rf.electionTimeout
		}
		rf.mu.Unlock()

		time.Sleep(time.Duration(period) * time.Millisecond)
	}
}

// not thread safe
func (rf *Raft) startElection() {
	if logging {
		fmt.Println(rf.me, " starts election process on term: ", rf.currentTerm+1)
	}

	rf.role = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votesNum = 1
	rf.resetElectionTimeout()

	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateID = rf.me

	lastLogEntry, lastLogIndex := rf.getLastLogEntry()
	args.LastLogIndex = lastLogIndex
	args.LastLogTerm = lastLogEntry.Term

	// send vote requests to all peers
	for i := range rf.peers {
		if i != rf.me {
			go func(serverId int) {
				rf.sendRequestVote(args, serverId)
			}(i)
		}
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	term, isLeader := rf.GetState()
	rf.mu.Lock()
	index := len(rf.log)
	if isLeader {
		newEntry := logEntry{Command: command, Term: term}
		rf.log = append(rf.log, newEntry)
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

// Main starts main server services
func (rf *Raft) Main() {
	go rf.electionProcess()
	go rf.appendNewLogEntriesProcess()
	go rf.applyNewEntriesProcess()
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, logEntry{Command: nil, Term: 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)

	rf.role = follower
	rf.votesNum = 0
	rf.resetElectionTimeout()

	go rf.Main()

	return rf
}
