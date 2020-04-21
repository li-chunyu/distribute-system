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
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

const (
	LEADER    = 1
	FOLLOWER  = 2
	CANDIDATE = 3

	// LE for leader election
	LEMIN = 150
	LEMAX = 300

	// HB for heart beat
	HBPERIOD = 100
)

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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// persistent state on all server
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state on all server
	commitIndex int
	lastApplied int
	state       int
	leaderId    int

	// volatile state on leader
	nextIndex  []int
	matchIndex []int

	// state added for implementation.
	heartBeatPeriod         time.Duration
	electionTimeout         time.Duration
	latestSendHeartBeatTime time.Time
	latestGotHeartBeatTime  time.Time

	electionEventCh  chan bool
	heartBeatEventCh chan bool

	heartBeatCond *sync.Cond
	electionCond  *sync.Cond
}

func (rf *Raft) String() string {
	var role string
	if rf.state == FOLLOWER {
		role = "follower"
	} else if rf.state == CANDIDATE {
		role = "candidate"
	} else {
		role = "leader"
	}
	return fmt.Sprintf("(%d, %d, %v)", rf.me, rf.currentTerm, role)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
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

type LogEntry struct {
	Command interface{}
	Term    int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm {

		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.switchState2(FOLLOWER)
		}
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			lastLogIndex := len(rf.log) - 1
			lastLogTerm := rf.log[lastLogIndex].Term

			if lastLogTerm < args.LastLogTerm ||
				(lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
				DPrintf("%v give vote to %d", rf, args.CandidateId)
				rf.votedFor = args.CandidateId
				rf.setElectionTimeout()
				rf.switchState2(FOLLOWER)

				reply.Term = rf.currentTerm
				reply.VoteGranted = true
				return
			}
		}
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	// 这是一个合法的 AppendEntries
	if rf.currentTerm <= args.Term {
		// 当前 term 小于参数中的 term，先更新 term
		if rf.currentTerm < args.Term {
			DPrintf("<AppendEntries>, %v has older term than (%d, %d), update it", rf, args.LeaderId, args.Term)
			rf.currentTerm = args.Term
			rf.setElectionTimeout()
			// 重置 votedfor，以便于下次再投票
			rf.votedFor = -1
			rf.switchState2(FOLLOWER)
		}
		// now rf.currentTerm == args.Term
		// 一致性检查通过
		if len(rf.log) > args.PrevLogIndex &&
			rf.log[args.PrevLogIndex].Term == args.Term {

			rf.switchState2(FOLLOWER)

			isMatch := true
			nextIndex := args.PrevLogIndex + 1
			end := len(rf.log) - 1
			for i := 0; isMatch && i < len(args.Entries); i++ {
				// args.Entries 比 rf.log 还长了不 match
				if end < nextIndex+i {
					isMatch = false
				} else if args.Entries[i].Term != rf.log[nextIndex+i].Term {
					isMatch = false
				}
			}

			// 存在冲突才复制，不冲突不复制，防止截断 peer 的日志
			if isMatch == false {
				entries := make([]LogEntry, len(args.Entries))
				copy(entries, args.Entries)
				rf.log = append(rf.log[nextIndex:], entries)
			}
			DPrintf("<AppendEntries>, %v commitIndex %d, leader %d commitIndex %d",
				rf, rf.commitIndex, args.LeaderId, args.LeaderCommit)
			// TODO update commit index
			// TODO applycond 状态机执行已经 commit 的状态
		}
	} // END rf.currentTerm <= args.Term

	reply.Term = rf.currentTerm
	reply.Success = false
	rf.mu.Unlock()
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
	rf.mu.Lock()
	if rf.state != LEADER {
		idx := len(rf.log)
		term := rf.currentTerm
		isLeader = false
		rf.mu.Unlock()
		return idx, term, isLeader
	}

	entry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, entry)
	index = len(rf.log) - 1 // 该 log 被插入的 index
	term = rf.currentTerm
	DPrintf("<Start> Leader: %v Append new entry to log, index: %d, term: %d",
		rf, index, term)
	rf.latestSendHeartBeatTime = time.Now()
	go rf.sendAppendEntriesToAll(term, index, rf.commitIndex, "New Entry")
	rf.mu.Unlock()
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

func (rf *Raft) setElectionTimeout() {
	rdst := rand.Int63n(LEMAX - LEMIN)
	rf.electionTimeout = time.Millisecond * time.Duration(LEMIN+rdst)
	rf.latestGotHeartBeatTime = time.Now()
}

func (rf *Raft) electionTimeoutChecker() {
	for {
		if _, isleader := rf.GetState(); isleader {
			// if rf.state == LEADER {
			rf.mu.Lock()
			rf.electionCond.Wait()
			rf.mu.Unlock()
		} else {
			rf.mu.Lock()
			dur := time.Since(rf.latestGotHeartBeatTime)
			if int64(dur) >= int64(rf.electionTimeout) {
				DPrintf("%v leader election timeout.", rf)
				rf.electionEventCh <- true
			}
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (rf *Raft) heartBeatPeriodChecker() {
	for {
		if _, isleader := rf.GetState(); isleader == false {
			// if rf.state != LEADER {
			rf.mu.Lock()
			rf.heartBeatCond.Wait()
			rf.mu.Unlock()
		} else {
			rf.mu.Lock()
			dur := time.Since(rf.latestSendHeartBeatTime)
			if int64(dur) >= int64(rf.heartBeatPeriod) {
				rf.heartBeatEventCh <- true
			}
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (rf *Raft) eventLoop() {
	for {
		select {
		case <-rf.electionEventCh:
			rf.mu.Lock()
			// 目前正在进行一次选举，等选举完再进行下一次
			go rf.startLeaderElection()
			DPrintf("%v start leader election process", rf)
			rf.mu.Unlock()
		case <-rf.heartBeatEventCh:
			rf.mu.Lock()
			DPrintf("%v send heart beat to peers", rf)
			rf.mu.Unlock()
			// 在一个 gorotine 当中进行，因为我不想阻塞 evetloop
			go rf.sendHeartBeatToAll()
		}
	}
}

func (rf *Raft) switchState2(to int) {
	from := rf.state
	if from == CANDIDATE && to == LEADER {
		rf.state = to
		rf.heartBeatCond.Signal()
	} else if from == LEADER && to == FOLLOWER {
		rf.state = to
		rf.electionCond.Signal()
	} else {
		rf.state = to
	}
}

func (rf *Raft) startLeaderElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.switchState2(CANDIDATE)
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.setElectionTimeout()

	lastidx := len(rf.log) - 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastidx,
		LastLogTerm:  rf.log[lastidx].Term,
	}
	go rf.sendRequestVote2peers(args)
}

func (rf *Raft) sendRequestVote2peers(args RequestVoteArgs) {
	votecount := 1
	threshold := len(rf.peers)/2 + 1
	var wg sync.WaitGroup
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			DPrintf("%v send request vote to %d", rf, id)
			var reply RequestVoteReply
			// sendRequestVote在发送时不会占用锁, 所以超时没关系
			ok := rf.sendRequestVote(id, &args, &reply)
			if ok == false {
				DPrintf("%v send request vote to %d **fail**", rf, id)
				return
			}
			rf.mu.Lock()
			// 发送参数中的 term 和当前 term 不相同，这说明收到了一个旧的回复
			// 所以直接丢弃该回复。
			if rf.currentTerm != args.Term {
				rf.mu.Unlock()
				return
			}
			// 防止在此期间 current term 发生改变，所以继续加锁
			if reply.VoteGranted == false {
				DPrintf("%v failed to grant vote from %d", rf, id)
				if reply.Term > rf.currentTerm {
					DPrintf("%v term is out-of-date from %d's term %d, back to follower",
						rf, id, reply.Term)
					rf.currentTerm = reply.Term
					rf.switchState2(FOLLOWER)
					rf.votedFor = -1
				}
			} else {
				votecount += 1
				if rf.state == CANDIDATE && votecount >= threshold {
					DPrintf("%v grant most votes, become leader", rf)
					rf.switchState2(LEADER)
					rf.leaderId = rf.me
					for i := range rf.peers {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}
					// 立即发送一次心跳，用于防止其他 server 超时(比如 heartbeat checker 占不上锁)
					go rf.sendHeartBeatToAll()
				}
			}
			rf.mu.Unlock()
		}(server)
	}
	wg.Wait()
}

func (rf *Raft) sendHeartBeatToAll() {
	rf.mu.Lock()
	// 发送 heartbeat 时的term
	term := rf.currentTerm
	// 构造 Append entry 参数时，args.log = rf.log[nextIndex[id] : index+1]
	// 如果没有新 entry，那么等价于args.log = rf. log[len(rf.log), len(rf.log)]
	// 即 args.log 长度为 0。
	index := len(rf.log) - 1
	commitIndex := rf.commitIndex
	rf.latestSendHeartBeatTime = time.Now()
	rf.mu.Unlock()
	rf.sendAppendEntriesToAll(term, index, commitIndex, "heartbeat")
}

// term: 发送该RPC 时 leader 的 term
// index：要 append 的 log entry 的 index
// reason：是 heartbeat 还是 append entry（为了日志更容易分析）
func (rf *Raft) sendAppendEntriesToAll(term int, index int, commitIndex int, reason string) {
	rf.mu.Lock()
	// defer rf.mu.Unlock()
	// 避免发送老的 rpc
	if term != rf.currentTerm {
		DPrintf("<sendAppendEntriesToAll> %v send append entries, reason is [%s], send stopped, because term miss match ,call term %d, now term %d",
			rf, reason, term, rf.currentTerm)
		return
	}
	rf.mu.Unlock()
	threshold := (len(rf.peers) / 2) + 1
	nreplicated := 1 // leader 中已经复制了一份
	isAgree := false
	var wg sync.WaitGroup
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		wg.Add(1)
		go func(id int) {
			rf.mu.Lock()
			nextIndex := rf.nextIndex[id]
			prevLogIndex := nextIndex - 1
			prevLogTerm := rf.log[prevLogIndex].Term
			// TODO go 的切片是浅拷贝， 所以 entries 可能需要 copy 一下,
			// entries := make([]LogEntries, len(rf.log[nextIndex : index+1]))
			// copy(entries, rf.log[nextIndex : index+1])
			entries := rf.log[nextIndex : index+1]

			if nextIndex > (index + 1) {
				// 不应该发生。
				DPrintf("<sendAppendEntriesToAll>, fatal, leader %v,server %d nextIndex %d bigger than (index+1) %d",
					rf, id, nextIndex, index+1)
			}
			args := AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commitIndex,
			}
			rf.mu.Unlock()

			var reply AppendEntriesReply
			reply.Success = false
			ok := rf.sendAppendEntries(id, &args, &reply)
			if !ok {
				DPrintf("<sendAppendEntriesToAll>, network failed, %v send AppendEntries to %d",
					rf, id)
				return
			}
			for reply.Success == false {
				rf.mu.Lock()
				// 不处理过期的回复
				if rf.currentTerm != args.Term {
					rf.mu.Unlock()
					return
				}
				// 接收方具有更高的 term, back to follower
				if reply.Term > args.Term {
					DPrintf("<sendAppendEntriesToAll>, %v find a bigger term server %d, term: %d, back to follower",
						rf, id, reply.Term)
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.switchState2(FOLLOWER)
					rf.mu.Unlock()
					return
				} else { // 一致性检查出错 nextIndex 回退
					nextIndex = nextIndex - 1
					if nextIndex < 0 {
						DPrintf("<sendAppendEntriesToAll>, %v，***fatal***, %d nextIndex < 0",
							rf, id)
					}
					args.PrevLogIndex = nextIndex
					args.PrevLogTerm = rf.log[nextIndex].Term
					entries = rf.log[nextIndex : index+1]
					args.Entries = entries
					rf.mu.Unlock()
					ok := rf.sendAppendEntries(id, &args, &reply)
					if !ok {
						DPrintf("<sendAppendEntriesToAll>, network failed, %v send AppendEntries to %d",
							rf, id)
						return
					}
				}
			}
			// reply.Success must be true.
			// AppendEntries RPC 发送成功.
			// 更新 index
			rf.mu.Lock()
			if rf.nextIndex[id] < nextIndex+1 {
				rf.nextIndex[id] = nextIndex + 1
				rf.matchIndex[id] = index
			}
			nreplicated += 1
			DPrintf("<sendAppendEntriesToAll>, %v replicate log[%d] to %d success",
				rf, index, id)
			if isAgree == false && rf.state == LEADER && nreplicated >= threshold {
				isAgree = true
				DPrintf("<sendAppendEntriesToAll>, %v replicate log[%d] to majorities",
					rf, index)
				if rf.commitIndex < index && rf.log[index].Term == rf.currentTerm {
					rf.commitIndex = index
					DPrintf("<sendAppendEntriesToAll>, %v moves commitIndex from %d to %d",
						rf, rf.commitIndex, index)
					rf.commitIndex = index
					go rf.sendHeartBeatToAll()
					// TODO applycond
					// rf.apply
				}
			}
			rf.mu.Unlock()
		}(server)
	}
	// 等待所有发送 goroutine 返回。
	wg.Wait()
}

// callTerm: 该 AppendEntries 调用时的参数， 如果该 term 和当前 term 不相同，
// 	说明这是一个过期的 reply, 不应该再处理了。
// 在处理 AppendEntriesReply 时占有 lock，而在等待 reply 时释放lock，这么做的原因是
// sendAppendEntriesToAll
// 函数中全程占有锁，如果一个 RPC 因为网络故障返回特别慢（长达1s）， 由于我的 heartbeat 发送时间
// 轮询函数也需要占有锁，所以这会导致延迟发送 heartbeat，最终导致其他 follower 开始发起选举。
func (rf *Raft) processAppendEntriesReply(callTerm int, replyCh chan AppendEntriesReply) {
	for r := range replyCh {
		rf.mu.Lock()
		if callTerm != rf.currentTerm {
			DPrintf("%v get a out-of-date AppendEntries reply, call term is %d, now term is %d", rf, callTerm, rf.currentTerm)
			// 这是一个过期的 reply, 不应该被处理
			// 在 continue 之前释放锁， 否则 continue 就不会释放所
			rf.mu.Unlock()
			continue
		}
		if r.Term > rf.currentTerm {
			DPrintf("%v find a peer with bigger term %d, back to follower", rf, r.Term)
			rf.votedFor = -1
			rf.currentTerm = r.Term
			rf.setElectionTimeout()
			rf.switchState2(FOLLOWER)
		}
		rf.mu.Unlock()
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
	rf.electionEventCh = make(chan bool)
	rf.heartBeatEventCh = make(chan bool)
	rf.heartBeatCond = sync.NewCond(&rf.mu)
	rf.electionCond = sync.NewCond(&rf.mu)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = FOLLOWER
	rf.leaderId = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.latestGotHeartBeatTime = time.Now()
	rf.heartBeatPeriod = time.Millisecond * HBPERIOD

	rf.setElectionTimeout()
	go rf.electionTimeoutChecker()
	go rf.heartBeatPeriodChecker()
	go rf.eventLoop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
