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

	role  int //0:follower 1:candidate 2:leader
	delay time.Time
}

type Entry struct {
	Term    int
	Command string
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
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	//these are talking about committed logs, see 5.4.1
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

func (rf *Raft) String() string               { return fmt.Sprintf("ME: %v<role:%v curTerm:%v vote:%v>", rf.me, rf.role, rf.currentTerm, rf.votedFor); }
func (ra *RequestVoteArgs) String() string    { return fmt.Sprintf("RequestVoteArgs: <ra.term:%v, ra.cadi:%v>", ra.Term, ra.CandidateId); }
func (rp *RequestVoteReply) String() string   { return fmt.Sprintf("RequestVoteReply: <rp.term:%v, rp.granted:%v>", rp.Term, rp.VoteGranted); }
func (aa *AppendEntriesArgs) String() string  { return fmt.Sprintf("AppendEntriesArgs: <aa.Term: %v, aa.LeaderId:%v>", aa.Term, aa.LeaderId); }
func (ap *AppendEntriesReply) String() string { return fmt.Sprintf("AppendEntriesReply: <ap.Term:%v, ap.Suc:%v>", ap.Term, ap.Success); }

//
// example RequestVote RPC handler.
//
// raft may be in 3 state
// and
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	ori := *rf

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
		rf.switchToFollower(args.Term)
		rf.votedFor = args.CandidateId
		rf.delay = rf.generateDelay()

		reply.Term = rf.currentTerm
		reply.VoteGranted = true//rf vote the candidate in newer term
		return
	}

	//----rf is in the same term as args.term
	//three possible states, right?
	if rf.role == 2{
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
	//so the delay should be reset
	//take care of election restriction(5.4.1) vote if candidate is more up to date.
	if rf.role == 0 {
		//reset time out delay
		rf.delay = rf.generateDelay()

		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		//check log is up to date
		if rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			return
		}

		if rf.votedFor == -1{
			//vote for this candidate if last entries of this raft is not later than the request
			//and if the term is the same, candidate's log should not be smaller than the current raft
			//or deny this requeset
			if rf.log[rf.commitIndex].Term <= args.LastLogTerm && rf.commitIndex <= args.LastLogIndex {
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
				return
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

//todo: append logs in 2B
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	ori := *rf

	defer rf.mu.Unlock()
	defer DPrintf("AppendEntries ori rf : %v  rf : %v args : %v reply : %v\n", &ori, rf, args, reply)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	//todo: some checks on prevLogIndex etc
	if args.Term > rf.currentTerm {
		rf.switchToFollower(args.Term)
		rf.votedFor = args.LeaderId
		rf.delay = rf.generateDelay()

		reply.Term = rf.currentTerm
		reply.Success = true
		return
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
		rf.switchToFollower(args.Term)
		rf.votedFor = args.LeaderId
		rf.delay = rf.generateDelay()

		reply.Term = rf.currentTerm
		reply.Success = true
		return
	}

	//same term, as follower
	rf.delay = rf.generateDelay()//stay in follower state
	//todo:add assert
	if rf.votedFor != -1 && rf.votedFor != args.LeaderId{
		DPrintf("[ERROR]follower of term: %v voted leader: %v but receive appRPC from %v", rf.currentTerm, rf.votedFor, args.LeaderId)
	}
	rf.votedFor = args.LeaderId

	reply.Success = true
	reply.Term = rf.currentTerm
	return
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
	case <- time.After(time.Millisecond * time.Duration(timeOut)):
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

	return index, term, isLeader
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
func (rf *Raft) switchToFollower(term int) {
	rf.role = 0
	rf.votedFor = -1
	rf.currentTerm = term
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
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

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
			rf.delay = rf.generateDelay()
			cacheMe := rf.me
			cacheTerm := rf.currentTerm
			timeOut := rf.delay
			rf.mu.Unlock()

			req := &RequestVoteArgs{Term: cacheTerm, CandidateId: cacheMe}
			//send req
			majority := len(rf.peers) / 2
			start := time.Now()
			DPrintf("rf.me:%v<rf.term:%v> request for election:", rf.me, rf.currentTerm)

			voteChan := make(chan int, 100)//limit connected hosts to 100
			for i := 0; i < len(rf.peers); i++ {
				if i == cacheMe {
					continue
				}

				go func(i int, req RequestVoteArgs, rf*Raft) {
					rsp := &RequestVoteReply{}
					ret := rf.sendRequestVoteWithTimeOut(i, &req, rsp, 50)
					DPrintf("rf.me:%v request for election: req:%v ret:%v rsp:%v", cacheMe, i, ret, rsp)

					if ret && rsp.VoteGranted {
						voteChan <- 1
						return
					}
					if ret && rsp.Term > req.Term {
						rf.mu.Lock()
						if rsp.Term > rf.currentTerm{
							rf.switchToFollower(rsp.Term)
							DPrintf("rf.me:%v request for election: req:%v Switch Role to follower", rf.me, i)
						}
						rf.mu.Unlock()

						return
					}
				}(i, *req, rf)
			}

			//loop until the rf win this election or time out
			cnt := 1
			for cnt <= majority{
				mills := timeOut.Sub(time.Now())
				DPrintf("wake after : %v", mills)
				if mills < 0{
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
				DPrintf("!!!!rf.me:%v become leader, curTerm:%v", rf.me, rf.currentTerm)
				rf.role = 2
				beLeader = true
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
		appArg := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go sendHeartBeat(i, rf, *appArg)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func sendHeartBeat(i int, rf *Raft, args AppendEntriesArgs) {
	appRsp := &AppendEntriesReply{}

	ret := rf.sendAppendEntries(i, &args, appRsp)
	if ret && appRsp.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.switchToFollower(appRsp.Term)
		rf.mu.Unlock()
	}

}
