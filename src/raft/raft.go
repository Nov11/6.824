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

import "sync"
import (
	"labrpc"
	"math/rand"
	"time"
	"fmt"
	"bytes"
	"encoding/gob"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain
	//should be used through accessor
	currentTerm int
	votedFor    int //use -1 for nil
	log         []Entry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	role                    int //0:follower 1:candidate 2:leader
	delay                   time.Time
	commitIndexChanged      *sync.Cond //use this to issue append entry rpcs
	applyCh                 chan ApplyMsg
	highestReplicatingIndex []int //highest index that this server is replicating to server[i], it's to reduce duplicate RPC
}

type Entry struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == 2
	rf.mu.Unlock()
	return term, isleader
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
	//these are talking about committed logs, see 5.4.1. but not related to commitIndex.
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) String() string               { return fmt.Sprintf("ME: %v<role:%v curTerm:%v vote:%v log:%v commitIndex:%v>", rf.me, rf.role, rf.currentTerm, rf.votedFor, rf.log, rf.commitIndex) }
func (ra *RequestVoteArgs) String() string    { return fmt.Sprintf("RequestVoteArgs: <ra.term:%v, ra.cadi:%v>", ra.Term, ra.CandidateId); }
func (rp *RequestVoteReply) String() string   { return fmt.Sprintf("RequestVoteReply: <rp.term:%v, rp.granted:%v>", rp.Term, rp.VoteGranted); }
func (aa *AppendEntriesArgs) String() string  { return fmt.Sprintf("AppendEntriesArgs[heartBeat? %v]: <aa.Term: %v, aa.LeaderId:%v,  aa.PrevLogIndex:%v, aa.PrevLogTerm:%v, aa.Entries:%v aa.LeaderCommit:%v>", len(aa.Entries) == 0, aa.Term, aa.LeaderId, aa.PrevLogIndex, aa.PrevLogTerm, aa.Entries, aa.LeaderCommit) }
func (ap *AppendEntriesReply) String() string { return fmt.Sprintf("AppendEntriesReply: <ap.Term:%v, ap.Suc:%v>", ap.Term, ap.Success); }
func (rf *Raft) saveContent() Raft {
	ret := Raft{me: rf.me, role: rf.role, currentTerm: rf.currentTerm, votedFor: rf.votedFor, commitIndex: rf.commitIndex, log: make([]Entry, len(rf.log))}
	copy(ret.log, rf.log)
	return ret
}
func (msg *ApplyMsg) String() string { return fmt.Sprintf("ApplyMsg< msg.Command:%v, msg.Index:%v>", msg.Command, msg.Index) }

func (rf *Raft) setVotedFor(newVal int) {
	rf.votedFor = newVal
	rf.persist()
}

func (rf *Raft) setLog(newLog []Entry) {
	rf.log = newLog
	rf.persist()
}

func (rf *Raft) setCommitIndex(newIdx int) {
	rf.commitIndex = newIdx
	rf.persist()
}

//
// example RequestVote RPC handler.
//
// raft may be in 3 state
// and
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	ori := rf.saveContent()

	defer rf.mu.Unlock()
	defer DPrintf("RequestVote ori rf : %v  rf : %v args : %v reply : %v\n", &ori, rf, args, reply)

	//take care of stale request which term < current term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	//rf is in old term, so turns to follower and take args.CandidateId as new leader
	if args.Term > rf.currentTerm {
		rf.switchToFollower(args.Term, -1)
		//if the leader is not update-to-date than current raft instance, reject this
		if !isPeerNotLessUpdateThanCurrentRaft(rf, args) {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}

		//rf.votedFor = args.CandidateId 2C
		rf.setVotedFor(args.CandidateId)
		//rf.delay = rf.generateDelay()

		reply.Term = rf.currentTerm
		reply.VoteGranted = true //rf vote the candidate in newer term
		return
	}

	//----rf is in the same term as args.term
	//three possible states, right?
	if rf.role == 2 {
		//I'm already the leader of this term.
		//the role may be changed only receiving RPC of larger term
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

		return
	}
	//as long as follower gets RPC from leader (append entry) or candidate(request vote)
	//it stays in follower state
	//I think the RPC that make sense here should be in curTerm
	//as the paper listed what should be done when encounter newer or older term in request
	//
	//take care of election restriction(5.4.1) vote if candidate is more up to date.
	if rf.role == 0 {
		//reset time out delay  NO
		//rf.delay = rf.generateDelay()

		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		//check log is up to date
		if rf.votedFor == args.CandidateId {
			if isPeerNotLessUpdateThanCurrentRaft(rf, args) {
				reply.VoteGranted = true
			} else {
				DPrintf("voting [deny with equal candidate]cur rf:%v args:%v", rf, args)
			}

			return
		}

		if rf.votedFor == -1 {
			//vote for this candidate if last entries of this raft is not later than the request
			//and if the term is the same, candidate's log should not be smaller than the current raft
			//or deny this request
			ret := isPeerNotLessUpdateThanCurrentRaft(rf, args)
			if ret {
				//rf.votedFor = args.CandidateId 2C
				rf.setVotedFor(args.CandidateId)
				reply.VoteGranted = true
			}
			return
		}

		return
	}

	//role == 1
	//there is no way that I receive a request from myself of the same term
	//so reject any request vote request
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return
}
func isPeerNotLessUpdateThanCurrentRaft(rf *Raft, args *RequestVoteArgs) bool {
	size := len(rf.log)
	if size == 0 {
		return true
	} else if rf.log[size-1].Term < args.LastLogTerm {
		return true
	} else if rf.log[size-1].Term == args.LastLogTerm && size <= args.LastLogIndex {
		return true
	}
	return false
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	ori := rf.saveContent()

	defer rf.mu.Unlock()
	s := "heartbeat"
	if len(args.Entries) != 0 {
		s = "replicate"
	}
	defer DPrintf("AppendEntries[%s] ori rf : %v  rf : %v args : %v reply : %v\n", s, &ori, rf, args, reply)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.switchToFollower(args.Term, args.LeaderId)
		//rf.votedFor = args.LeaderId 2C
		//rf.setVotedFor(args.LeaderId) combined into new switchToFollower
		//rf.delay = rf.generateDelay()

		fillReply(reply, rf, args)
		if reply.Success == false {
			return
		}
		if len(args.Entries) == 0 {
			updateCommitIndex(rf, args)
			return
		}

		//apply entries later
	}

	//as a leader, drop this
	if rf.role == 2 {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("[ERROR] leader receive appendentry RPC of same term !!! term: %v ", rf.currentTerm)
		return
	}

	//if current raft is in candidate state and receive a AppendEntry RPC that has equal term
	//it means that a leader has been elected
	//so return to follower state and set leader to args.LeaderId
	if rf.role == 1 {
		rf.switchToFollower(args.Term, args.LeaderId)
		//rf.votedFor = args.LeaderId 2C
		//rf.delay = rf.generateDelay()

		fillReply(reply, rf, args)
		if reply.Success == false {
			return
		}
		if len(args.Entries) == 0 {
			updateCommitIndex(rf, args)
			return
		}

		//apply entries later
	}

	//same term, as follower
	rf.delay = rf.generateDelay() //stay in follower state

	if rf.votedFor != -1 && rf.votedFor != args.LeaderId {
		DPrintf("[ERROR]follower of term: %v voted leader: %v but receive appRPC from %v", rf.currentTerm, rf.votedFor, args.LeaderId)
	}
	//rf.votedFor = args.LeaderId 2C
	rf.setVotedFor(args.LeaderId)

	fillReply(reply, rf, args)
	if reply.Success == false {
		return
	}
	if len(args.Entries) == 0 {
		updateCommitIndex(rf, args)
		return
	}
	////if the new entry conflicts with existing ones, delete existing entries and all that follows it
	////curLogSize := len(rf.log)
	//newEntryIndex := args.PrevLogIndex + 1
	////if curLogSize >= newEntryIndex && rf.log[newEntryIndex-1].Term != args.Entries[0].Term {
	//rf.log = rf.log[:newEntryIndex-1]
	////}
	////append new entry
	//rf.log = append(rf.log, args.Entries...)
	//
	//updateCommitIndex(rf, args)

	//[3--10]maybe here before [4-7] arrive, you don't wanna remove 8-10, right?
	for i := 0; i < len(args.Entries); i++ {
		iter := args.PrevLogIndex + i
		if iter >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[i])
		} else {
			if rf.log[iter].Term == args.Entries[i].Term {
				//nothing
			} else {
				rf.log = rf.log[:iter]
				rf.log = append(rf.log, args.Entries[i])
			}
		}
		rf.persist()
	}
	return
}

func updateCommitIndex(rf *Raft, args *AppendEntriesArgs) {
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log))
		DPrintf("updateCommitIndex:args:%v rf:%v result:%v", args, rf, rf.commitIndex)
		rf.commitIndexChanged.Broadcast()
	}
}

func (rf *Raft) applyCommand() bool {
	rf.mu.Lock()
	ret := false
	if rf.commitIndex > rf.lastApplied {
		ret = true
		applyThis := rf.lastApplied + 1
		msg := ApplyMsg{Index: applyThis, Command: rf.log[applyThis-1].Command}
		rf.applyCh <- msg
		rf.lastApplied = applyThis
		DPrintf("[Apply] rf:%v applied: %v", rf.me, &msg)
	}
	rf.mu.Unlock()
	return ret
}

func fillReply(reply *AppendEntriesReply, rf *Raft, args *AppendEntriesArgs) {
	//this is a heartbeat msg
	reply.Term = rf.currentTerm
	//if len(args.Entries) == 0 {
	//	reply.Success = true
	//} else {
	//this is meant to append new log entry
	myLogSize := len(rf.log)
	//there exits one entry at prevlogindex with prevlogterm
	if myLogSize == 0 {
		reply.Success = args.PrevLogIndex == 0 //&& args.PrevLogTerm == 0
	} else if myLogSize >= args.PrevLogIndex && (args.PrevLogIndex == 0 || rf.log[args.PrevLogIndex-1].Term == args.PrevLogTerm) {
		reply.Success = true
	} else {
		reply.Success = false
	}
	//}
}

//
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
//

//yes, I'm using time out here.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendRequestVoteWithTimeOut(server int, args *RequestVoteArgs, reply *RequestVoteReply, timeOut int) bool {
	c := make(chan bool, 1)
	go func(rf *Raft) {
		c <- rf.sendRequestVote(server, args, reply)
	}(rf)

	var ret bool
	select {
	case ret = <-c:
	case <-time.After(time.Millisecond * time.Duration(timeOut)):
	}

	return ret
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = rf.role == 2

	if isLeader {
		size := len(rf.log)
		index = size + 1
		term = rf.currentTerm
		cmd := Entry{Term: rf.currentTerm, Command: command}
		rf.log = append(rf.log, cmd)
		rf.persist()
		DPrintf("[REPLICATE] [Start] leader:%v replicate: command:%v", rf.me, cmd)
		go rf.replicateLog(index)
	}

	return index, term, isLeader
}

func (rf *Raft) replicateLog(index int) {
	//1.append command to rf's own log<this has already been done before calling this function>
	//2.issue append entry rpc
	//3.if it's safe to apply this command, apply it to state machine
	//4.return result to client
	rf.mu.Lock()
	previousIndex := index - 1
	previousTerm := 0
	if previousIndex != 0 {
		previousTerm = rf.log[previousIndex-1].Term
	}
	rf.mu.Unlock()
	//issue rpc only if previous command is committed or this command will not be accepted by followers as
	//they can't find log entry matching previous index and term

	rf.mu.Lock()
	//for rf.role == 2 && rf.commitIndex < previousIndex {
	//	rf.cond.Wait()
	//}
	if rf.role != 2 {
		rf.mu.Unlock()
		return
	}

	request := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, LeaderCommit: rf.commitIndex, PrevLogIndex: previousIndex, PrevLogTerm: previousTerm}
	size := len(rf.peers)
	rf.mu.Unlock()
	accepted := make(chan int, 100)
	abort := make(chan int, 100)
	duplicate := make(chan int, 100)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.replicateUpThroughThisLogRPC(i, request, accepted, index, abort, duplicate)
	}

	cnt := 1
	updated := []int{}
	for cnt < size/2+1 {
		select {
		case idx := <-accepted:
			cnt = cnt + 1
			updated = append(updated, idx)
		case exit := <-abort:
			//DPrintf("one [%v]replicateUpThroughThisLogRPC return abort, so stop replicating entries up through %v[entry is not committed]", exit, index)
			exit = exit + 1
			return
		case d := <-duplicate:
			//DPrintf("one [%v]replicateUpThroughThisLogRPC return duplicate. entry %v is being replicating. quit current process.[entry is not committed]", d, index)
			d = d + 1
			return
		}
	}

	rf.mu.Lock()
	if rf.commitIndex < index {
		rf.commitIndex = index
		rf.commitIndexChanged.Broadcast()
	}

	for _, server := range updated {
		rf.matchIndex[server] = max(rf.matchIndex[server], rf.commitIndex)
	}
	rf.mu.Unlock()
	//rf.applyCommand()
	//msg := ApplyMsg{Index: request.PrevLogIndex + 1, Command: command}
	//rf.applyCh <- msg

	rf.mu.Lock()
	DPrintf("[REPLICATE] [Done] log replicated to majority: command:%v", rf.log[index-1].Command)
	rf.mu.Unlock()
	//make sure this log entry is append to all followers
	//this is not necessary as channel accepted is with buffer of large enough size
	for cnt < size {
		select {
		case idx := <-accepted:
			cnt = cnt + 1
			rf.mu.Lock()
			if rf.matchIndex[idx] < rf.commitIndex {
				rf.matchIndex[idx] = rf.commitIndex
			}
			rf.mu.Unlock()
		case exit := <-abort:
			//DPrintf("one [%v]replicateUpThroughThisLogRPC return [abort], so stop replicating entries up through %v[entry is committed]", exit, index)
			exit = exit + 1
			return
		case d := <-duplicate:
			//DPrintf("one [%v]replicateUpThroughThisLogRPC return [duplicate]. entry %v is being replicating. quit current process.[entry is committed]", d, index)
			d = d + 1
			return
		}
	}
}

func (rf *Raft) replicateUpThroughThisLogRPC(i int, args AppendEntriesArgs, acc chan int, cmdIndex int, abort chan int, dup chan int) {
	updateReplictingIndex := false
	for true {
		//set entries to this args
		rf.mu.Lock()
		if rf.role != 2 {
			//DPrintf("rf:%v is not a leader any more. current role: %v. stop replicating with last entry:%v", rf.me, rf.role, cmdIndex)
			rf.mu.Unlock()
			abort <- i
			return
		}
		if cmdIndex < rf.nextIndex[i] {
			//DPrintf("rf:%v stop replicating with last entry:%v to %v. since nextIndex[i] : %v > cmdIndex:%v", rf.me, rf.log[cmdIndex-1], i, rf.nextIndex[i], cmdIndex)
			rf.mu.Unlock()
			dup <- i
			return
		}

		if (!updateReplictingIndex && cmdIndex <= rf.highestReplicatingIndex[i]) || (updateReplictingIndex && cmdIndex != rf.highestReplicatingIndex[i]) {
			//DPrintf("rf:%v stop replicating with last entry:%v to %v. since a higher index cmd[index:%v] > [cur:%v] is being replicating.", rf.me, rf.log[cmdIndex-1], i, rf.highestReplicatingIndex[i], cmdIndex)
			rf.mu.Unlock()
			dup <- i
			return
		}

		updateReplictingIndex = true
		rf.highestReplicatingIndex[i] = cmdIndex
		next := rf.nextIndex[i]
		args.Entries = rf.log[next-1:cmdIndex]
		args.PrevLogIndex = next - 1
		args.PrevLogTerm = 0
		if args.PrevLogIndex > 0 {
			args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
		}
		rf.mu.Unlock()

		if args.PrevLogIndex < 0 {
			DPrintf("[ERROR] appendentriesargs prevlogindex < 0")
		}

		//send rpc
		ret, reply := sendAppendEntriesRPC(i, rf, args)
		//ret == false means connection failed. try later.
		if ret == false {
			time.Sleep(20 * time.Millisecond)
			DPrintf("rf.me:%v replicate log to %v: req:%v ret:%v rsp:%v [connection failed, retry later]", rf.me, i, args, ret, reply)
			continue
		}

		//connection is ok.
		if reply.Success == false {
			rf.mu.Lock()
			if rf.role != 2 {
				//this leader's term is stale
				//and state has already fall back to follower on receiving reply
				//no need to replicate command any more
				DPrintf("rf.me:%v replicate log to %v: req:%v ret:%v rsp:%v [fall back to follower]", rf.me, i, args, ret, reply)
				rf.mu.Unlock()
				abort <- i
				return
			}
			rf.mu.Unlock()

			//peer doesn't contain entry at prevLogIndex with term == prevLogTerm
			//so here decrease corresponding nextIndex and try again
			//whenever nextIndex == 0, the reply must be true. so this loop is guaranteed to be terminated
			rf.mu.Lock()
			if rf.nextIndex[i] == args.PrevLogIndex+1 {
				rf.nextIndex[i] = rf.nextIndex[i] - 1
				if rf.nextIndex[i] < 1 {
					DPrintf("[ERROR]rf.nextIndex[%v] < 1", i)
				}
			}
			DPrintf("rf.me:%v replicate log to %v: req:%v ret:%v rsp:%v [decrease nextIndex to %v]", rf.me, i, args, ret, reply, rf.nextIndex[i])
			rf.mu.Unlock()
			continue
		}

		//reply.Success == true
		//log is replicated to peer[i], report this acceptance
		acc <- i
		rf.mu.Lock()
		if rf.nextIndex[i] < args.PrevLogIndex+1+len(args.Entries) {
			rf.nextIndex[i] = args.PrevLogIndex + 1 + len(args.Entries)
		}
		rf.mu.Unlock()
		DPrintf("rf.me:%v replicate log to %v: req:%v ret:%v rsp:%v [success]", rf.me, i, args, ret, reply)
		return
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) generateDelay() time.Time {
	v := rand.Intn(300) + 1000
	//DPrintf("rf.me: %v rand: %v", rf.me, v)
	return time.Now().Add(time.Duration(time.Duration(v) * time.Millisecond))
}
func (rf *Raft) getDelayDiffFromNow() time.Duration {
	return rf.delay.Sub(time.Now())
}

//lock before use this function
func (rf *Raft) switchToFollower(term int, newLeader int) {
	rf.role = 0
	rf.votedFor = newLeader
	rf.currentTerm = term
	rf.persist()
	rf.delay = rf.generateDelay()
	rf.commitIndexChanged.Broadcast()
	DPrintf("[ROLE] [change to follower]Raft %v change to follower", rf.me)
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

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.role = 0
	rf.delay = rf.generateDelay()
	rf.commitIndexChanged = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func(rf *Raft) {
		for true {
			rf.mu.Lock()
			//shouldApply := rf.commitIndex > rf.lastApplied
			for rf.commitIndex == rf.lastApplied {
				rf.commitIndexChanged.Wait()
			}
			rf.mu.Unlock()

			for rf.applyCommand() {
			}
		}
	}(rf)

	go func(rf *Raft) {
		for true {
			rf.mu.Lock()
			d := rf.getDelayDiffFromNow()
			rf.mu.Unlock()
			//better be sleep until, i think
			time.Sleep(time.Duration(d))

			rf.mu.Lock()
			if rf.delay.After(time.Now()) {
				rf.mu.Unlock()
				continue
			}
			if rf.role == 2 {
				rf.delay = rf.generateDelay()
				rf.mu.Unlock()
				continue
			}
			//begin election
			//1.update server's role
			//2.update term
			//3.vote for itself
			//4.reset next time out delay
			//5.send requestvote rpc

			if rf.role == 0 {
				rf.role = 1
			}

			rf.currentTerm++
			rf.votedFor = rf.me
			rf.persist()
			rf.delay = rf.generateDelay()
			cacheMe := rf.me
			cacheTerm := rf.currentTerm
			cacheLastIndex := len(rf.log)
			cacheLastTerm := 0
			if cacheLastIndex != 0 {
				cacheLastTerm = rf.log[cacheLastIndex-1].Term
			}
			timeOut := rf.delay
			rf.mu.Unlock()

			req := &RequestVoteArgs{Term: cacheTerm, CandidateId: cacheMe, LastLogTerm: cacheLastTerm, LastLogIndex: cacheLastIndex}
			//send req
			majority := len(rf.peers) / 2
			start := time.Now()
			DPrintf("rf.me:%v<rf.term:%v> request for election:", rf.me, rf.currentTerm)

			voteChan := make(chan int, 100) //limit connected hosts to 100
			for i := 0; i < len(rf.peers); i++ {
				if i == cacheMe {
					continue
				}

				go func(i int, req RequestVoteArgs, rf *Raft) {
					rsp := &RequestVoteReply{}
					ret := rf.sendRequestVoteWithTimeOut(i, &req, rsp, 50)
					DPrintf("rf.me:%v request for election: req:%v ret:%v rsp:%v", cacheMe, i, ret, rsp)

					if ret && rsp.VoteGranted {
						voteChan <- 1
						return
					}
					if ret && rsp.Term > req.Term {
						rf.mu.Lock()
						if rsp.Term > rf.currentTerm {
							rf.switchToFollower(rsp.Term, -1)
							DPrintf("rf.me:%v request for election: req:%v Switch Role to follower", rf.me, i)
						}
						rf.mu.Unlock()

						return
					}
				}(i, *req, rf)
			}

			//loop until the rf win this election or time out
			cnt := 1
			for cnt <= majority {
				mills := timeOut.Sub(time.Now())
				DPrintf("%v wake after : %v", rf.me, mills)
				if mills < 0 {
					break
				}
				select {
				case in := <-voteChan:
					cnt = cnt + in
				case <-time.After(mills):
					break
				}
			}

			elapse := time.Since(start)
			beLeader := false
			timeCost := int(elapse.Nanoseconds() / int64(1000*1000))
			rf.mu.Lock()
			//rf.delay -= timeCost

			DPrintf("[rf.me:%v request for election]: return cnt:%v timeCost:%v", rf.me, cnt, timeCost)
			if cnt > majority && cacheTerm == rf.currentTerm {
				DPrintf("[LEADER]!!!!rf.me:%v become leader, curTerm:%v", rf.me, rf.currentTerm)
				rf.role = 2
				beLeader = true

				rf.nextIndex = make([]int, len(rf.peers))
				nextIndexInit := len(rf.log) + 1
				for index := range rf.nextIndex {
					rf.nextIndex[index] = nextIndexInit
				}
				rf.matchIndex = make([]int, len(rf.peers))
				rf.highestReplicatingIndex = make([]int, len(rf.peers))
			}
			rf.mu.Unlock()

			if beLeader {
				//send initial empty AppendEntry rpc
				go heartBeat(rf)
			}
		}
	}(rf)
	return rf
}

func heartBeat(rf *Raft) {
	//send heartBeat every 100ms
	for true {
		rf.mu.Lock()
		if rf.role != 2 {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			rf.mu.Lock()
			if rf.nextIndex[i] > len(rf.log) {
				go sendHeartBeat(i, rf)
			} else {
				request := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, LeaderCommit: rf.commitIndex}
				acc := make(chan int, 1)
				abort := make(chan int, 1)
				dup := make(chan int, 1)
				go rf.replicateUpThroughThisLogRPC(i, request, acc, len(rf.log), abort, dup)
			}
			rf.mu.Unlock()

		}

		time.Sleep(100 * time.Millisecond)
	}
}

func sendHeartBeat(i int, rf *Raft) {
	rf.mu.Lock()
	appArg := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, LeaderCommit: rf.commitIndex}
	if len(rf.log) == 0 {
		appArg.PrevLogIndex = 0
	} else {
		if rf.nextIndex[i]-1 > 0 {
			appArg.PrevLogIndex = rf.nextIndex[i] - 1
			appArg.PrevLogTerm = rf.log[appArg.PrevLogIndex-1].Term
		} else {
			appArg.PrevLogIndex = 0
		}
	}
	rf.mu.Unlock()
	ret, reply := sendAppendEntriesRPC(i, rf, appArg)
	if ret == false {
		return
	}

	if reply.Success {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != 2 {
		return
	}

	if rf.nextIndex[i] == appArg.PrevLogIndex+1 {
		rf.nextIndex[i] = rf.nextIndex[i] - 1
	}
	if rf.nextIndex[i] < 1 {
		DPrintf("[ERROR]rf.nextIndex[%v] < 1", i)
	}
}

func sendAppendEntriesRPC(i int, rf *Raft, args AppendEntriesArgs) (bool, AppendEntriesReply) {
	appRsp := &AppendEntriesReply{}

	ret := rf.sendAppendEntries(i, &args, appRsp)
	rf.mu.Lock()
	if ret && appRsp.Term > rf.currentTerm {
		rf.switchToFollower(appRsp.Term, -1)
	}
	rf.mu.Unlock()
	return ret, *appRsp
}
