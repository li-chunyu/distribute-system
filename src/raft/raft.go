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
import "sync/atomic"
import "labrpc"
import "time"
import "math/rand"

// import "bytes"
// import "labgob"

const FOLLOWER = 1
const CANDIDATE = 2
const LEADER = 3

const HBPERIODIC = 100

type LogEntry struct {
	Command interface{}
	Term    int
}


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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// State: follower : 1 , candidate: 2 leader: 3
	state         int

	// Persistent state on all servers
	currentTerm   int
	votedFor      int
	logs          []LogEntry

	// Volatile state on all servers.
	commitIndex   int
	lastApplied   int

	// Volatile state on leader.
	nextIndex     []int
	matchIndex    []int

	// Leader election.
	lastTimeRecHeartBeat int64
	voteCount     int

	leaderElectionCond *sync.Cond
	heartBeatCond      *sync.Cond
}

func (rf *Raft) back2Follower(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
	rf.state = FOLLOWER
	rf.leaderElectionCond.Signal()
}

func (rf *Raft) Vote() {
	rf.mu.Lock()
	rf.state = CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.mu.Unlock()
	args := RequestVoteArgs {
		Term: rf.currentTerm,
		CandidateId: rf.me,
		// TODO, 不考虑 safety 的时候暂时用不上。
		LastLogIndex: 0,
		LastLogTerm: 0,
	}

	requestVoteReplyCh := make(chan RequestVoteReply, len(rf.peers))
	var wg sync.WaitGroup
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(serverId int) {
			defer wg.Done()
			var reply RequestVoteReply
			respCh := make(chan struct{})
			go func() {
				rf.sendRequestVote(serverId, &args, &reply)
				respCh <- struct{}{}
			}()		
			select {
			case <-time.After(time.Second):
				return
			case <-respCh:
				requestVoteReplyCh <- reply
			}
		}(i)
	}

	go func() {
		// 等待所有 requestVote 请求发送完毕（成功，或者超时）
		// 然后关闭通道。
		wg.Wait()
		close(requestVoteReplyCh)
	}()
	for rply := range requestVoteReplyCh {
		if rply.VoteGranted {
			rf.voteCount += 1
		}
		if rf.voteCount > (len(rf.peers) / 2) {
			DPrintf("<vote>: [ID:%d, TERM: %d] becomes leader", rf.me, rf.currentTerm)
			rf.state = LEADER
			rf.voteCount = 0
			rf.heartBeatCond.Signal()
			return
		}
	}
	DPrintf("<vote>: [ID:%d, TERM:%d] election fail, #VOTE is %d", rf.me, rf.currentTerm, rf.voteCount)
	// vote split 或者 没有当选，再次重置时钟	
	// 因为等待 request vote 返回的时间可能大于 leader election timeout，
	// 当 vote（）返回时，该 server 会立即发起一次投票（如果其他人当选那么就是多余的）
	rf.lastTimeRecHeartBeat = GetNowTime()
	rf.back2Follower(rf.currentTerm)
	return
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
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




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int  // candidate's term.
	CandidateId  int  // candidate requesting vote.
	LastLogIndex int  // index of candidate's last log entry.
	LastLogTerm  int  // term of candidate's last log entry.
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself.
	VoteGranted bool // true means candidate reveived vote.
}

func (rf *Raft) isMoreUpToDate(args *RequestVoteArgs) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.logs) == 0 {
		return true
	}
	if args.LastLogTerm > rf.logs[len(rf.logs)-1].Term {
		return true
	} else if args.LastLogTerm == rf.logs[len(rf.logs)-1].Term {

		if args.LastLogIndex > (len(rf.logs) - 1) {
			return true
		} else {
			return false
		}
	} else {
		return false
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("<RequestVote>: FROM [ID:%d, TERM:%d] TO [ID:%d, TERM:%d]",
			args.CandidateId, args.Term, rf.me, rf.currentTerm)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	rf.lastTimeRecHeartBeat = GetNowTime()
	if args.Term < rf.currentTerm {
		return;
	}

	if !rf.isMoreUpToDate(args) {
		return
	}

	if args.Term > rf.currentTerm {
		reply.VoteGranted = true

		rf.mu.Lock()
		rf.votedFor = args.CandidateId
		rf.mu.Unlock()

		rf.back2Follower(args.Term)
		DPrintf("<RequestVote>: [ID:%d, TERM:%d] votes to [ID:%d, TERM:%d]\n",
				rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}
	// now rf.currentTerm == args.Term
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.back2Follower(args.Term)
		DPrintf("<RequestVote>: [ID:%d, TERM:%d] votes to [ID:%d, TERM:%d]\n",
			rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	DPrintf("<AppendEntries>: Heart Beat. FROM [ID:%d, TERM:%d] TO [ID:%d, TERM:%d]",
			args.LeaderId, args.Term, rf.me, rf.currentTerm)
	rf.lastTimeRecHeartBeat = GetNowTime()
	if rf.currentTerm < args.Term {
		rf.back2Follower(args.Term)
		rf.mu.Lock()
		rf.votedFor = -1
		rf.mu.Unlock()
	}
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
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
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
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


func (rf *Raft) startLeaderElectionDaemon() {
	go rf.leaderElectionDaemon(400, 800)
}

func (rf *Raft) leaderElectionDaemon(left, right int64) {
	for {

		for rf.state != LEADER {
			timeout := time.Duration(randInterval(left, right))
			time.Sleep(timeout * time.Millisecond)
			dur := GetNowTime() - rf.lastTimeRecHeartBeat
			if dur > int64(timeout) {
				DPrintf("<ElectionTimeoutDaemon>: [%d] timeout, start electing leader", rf.me)
				rf.lastTimeRecHeartBeat = GetNowTime()
				rf.Vote()
			}
		}

		rf.leaderElectionCond.L.Lock()
		rf.leaderElectionCond.Wait()
	}
}

func randInterval(left, right int64) int64 {
	return rand.Int63n(right-left) + left
}

func (rf *Raft) startHeartBeatDaemon() {
	go rf.heartBeatDaemon()
}

func (rf *Raft) heartBeatDaemon() {
	for {

		for rf.state == LEADER {
			args := AppendEntriesArgs {
				Term: rf.currentTerm,
				LeaderId: rf.me,
				PrevLogIndex: -1,
				PrevLogTerm: -1,
				Entries: make([]LogEntry, 0),
				LeaderCommit: -1,
			}
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				var reply AppendEntriesReply
				go func(serverId int) {
					rf.sendAppendEntries(serverId, &args, &reply)
				}(i)
			}
			time.Sleep(HBPERIODIC * time.Millisecond)
		}

		rf.heartBeatCond.L.Lock()
		rf.heartBeatCond.Wait()
	}
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
	// start of Raft initialization
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)
	rf.lastTimeRecHeartBeat = GetNowTime()
	rf.leaderElectionCond = sync.NewCond(new(sync.Mutex))
	rf.heartBeatCond = sync.NewCond(new(sync.Mutex))
	rf.startLeaderElectionDaemon()
	rf.startHeartBeatDaemon()
	// end of Raft initialization
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
