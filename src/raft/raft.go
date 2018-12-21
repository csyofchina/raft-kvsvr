package raft

//
// this is an outline of the API that Raft must expose to
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
import "labrpc"
import "math/rand"
import "time"
// import "bytes"
// import "labgob"



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

const (
	Stopped      = "stopped"
	Initialized  = "initialized"
	Follower     = "follower"
	Candidate    = "candidate"
	Leader       = "leader"
	Snapshotting = "snapshotting"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Cmd		interface{}	
	Index 	int
	Term 	int
}

type Log struct {
	 entries	[]LogEntry   
     mu 	    sync.Mutex
     startIndex int
     startTerm  int
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
	currTerm  int
	votedFor  int
	commitIndex  int
	lastApplied  int	
	nextIndex  []int
	matchIndex []int
	log        	 Log
	leaderId     int
	state        string   //0:follower  1:candidate  2:leader
	heartBreakInterval    time.Duration
	heartBreakTimer *time.Timer
	lastAppendTime   time.Time
	recvVoteChan	 chan int 
	recvAppendChan   chan int
	stopHeartBreakChan chan int
	stopRaft 		 chan int
	reqWg sync.WaitGroup
}
//  All of My Universe Is You
//func (rf *Raft) Term() int {
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	return rf.currTerm
//}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currTerm
	if rf.state == "leader" {
	    isleader = true	
	}else{
		isleader = false
	}
    rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) State() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}


func (rf *Raft) SetState(state string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Temporarily store previous values.
	//prevState := rf.state
	//prevLeader := rf.leaderId

	// Update state and leader.
	rf.state = state
	if state == Leader {
		rf.leaderId = rf.me
	}

	// Dispatch state and leader change events.
	//s.DispatchEvent(newEvent(StateChangeEventType, s.state, prevState))
	/*
	if prevLeader != s.leader {
		s.DispatchEvent(newEvent(LeaderChangeEventType, s.leader, prevLeader))
	}*/
}

func (rf *Raft) CountPeer() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.peers)
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

func (l *Log) lastInfo() (index int, term int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// If we don't have any entries then just return zeros.
	if len(l.entries) == 0 {
		return l.startIndex, l.startTerm
	}

	// Return the last index & term
	entry := l.entries[len(l.entries)-1]
	return entry.Index, entry.Term
}

//func (rf *Raft) Setvotedfor(index int) {
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	rf.votedFor = index
//}
//
//func (rf *Raft) VotedFor() int {
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	return rf.votedFor	
//}
//
//func (rf *Raft) LeaderId() int {
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	return rf.leaderId
//}
//
//func (rf *Raft) SetLeaderId(index int) {
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	rf.leaderId = index
//}
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
	Term		 int
	CandidateId   int
	LastLogIndex int
	LastLogTerm  int
}

func NewRequestVoteArgs(term int,candidateId int,lastLogIndex int,lastLogTerm int) *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:          term,
		CandidateId: candidateId,
		LastLogIndex:  lastLogIndex,
		LastLogTerm:   lastLogTerm,
	}
} 
//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	VoteId      int
	Term		int
	VoteGranted bool
}

func NewRequestVoteReply() *RequestVoteReply {
	return &RequestVoteReply {
		VoteId:  -1,
		Term:  0,
		VoteGranted: false,
	}
}

type AppendEntriesArgs struct {
	Term		int
	LeaderId	int
	PreLogIndex int
	PreLogTerm  int
	Entries 	[]LogEntry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term		int
	Success		bool
}

func NewAppendEntriesArgs(term int,leaderId int, preLogIndex int, preLogTerm int, entries []LogEntry, commitIndex int) *AppendEntriesArgs{
	return &AppendEntriesArgs{
		Term:  term,
		LeaderId:   leaderId,
		PreLogIndex:		preLogIndex,
		PreLogTerm:		preLogTerm,
		Entries:       	entries, 
		LeaderCommitIndex: commitIndex, 
	}
}


func NewAppendEntriesReply(term int, success bool) *AppendEntriesReply{
	return &AppendEntriesReply {
	Term:				term,			
	Success:			success,
	}		
}
/*Raft determines which of two logs is more up-to-date
by comparing the index and term of the last entries in the
logs. If the logs have last entries with different terms, then
the log with the later term is more up-to-date. If the logs
end with the same term, then whichever log is longer is
more up-to-date*/
func (rf *Raft) AppendEntries(args *AppendEntriesArgs,reply *AppendEntriesReply) {
	lastIndex, _:= rf.log.lastInfo()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d recv Append from %d,args:term=%d,local:term=%d,leaderId=%d,state=%s\n",
	rf.me,args.LeaderId,args.Term,rf.currTerm,rf.leaderId,rf.state)
	if args.Term < rf.currTerm {
		reply.Term = rf.currTerm
		reply.Success = false
		return 
	}else if args.Term > rf.currTerm {
		rf.currTerm = args.Term
		if rf.state != Follower {
			DPrintf("Candidate %d convert to follower\n",rf.me)	
			rf.state = Follower			
			rf.leaderId = args.LeaderId
			if rf.state == Leader {
				rf.stopHeartBreakChan <- 1
			}		
		}
	}
	reply.Term = rf.currTerm
	// log doesn't contain an entry at prevLogIndex whose term matches preLogTerm
	if args.Entries == nil {
		
	}else {
		if lastIndex <= args.PreLogIndex {
			DPrintf("Append Req %d: ret = -1",rf.me)
			reply.Success = false
			return 
		}
		if rf.log.entries[args.PreLogIndex].Term != args.PreLogTerm {
			DPrintf("Append Req %d: ret = -2",rf.me)
			reply.Success = false
			return 
		}
	}
	if rf.state == Candidate && rf.leaderId != args.LeaderId {
		rf.state = Follower			
		rf.leaderId = args.LeaderId
	}
	rf.recvAppendChan <- args.LeaderId
	reply.Success = true
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	lastIndex,lastTerm := rf.log.lastInfo()
	rf.mu.Lock()
    defer rf.mu.Unlock()
	if args.Term < rf.currTerm {
		DPrintf("RequestVote: candidate = %d: voteId = %d ret = -1",args.CandidateId,rf.me)
		reply.Term = rf.currTerm
		reply.VoteGranted = false
	}else{
		 if args.Term > rf.currTerm {
			DPrintf("RequestVote: candidate = %d: voteId = %d ret = -2",args.CandidateId,rf.me)
			rf.currTerm = args.Term
			if rf.state != Follower{
				DPrintf("RequestVote:  %s:%d convert to follower ",rf.state,rf.me)
				rf.state = Follower
				rf.votedFor = -1	
				if rf.state == Leader {
					rf.stopHeartBreakChan <- 1
				}
			}	
		 }

		if (args.LastLogIndex < lastIndex || args.LastLogTerm < lastTerm) {
			DPrintf("RequestVote: candidate = %d: voteId = %d ret = -3",args.CandidateId,rf.me)
			reply.VoteGranted = false
		}else if rf.votedFor >= 0 && rf.votedFor != args.CandidateId {
			DPrintf("RequestVote: candidate = %d: voteId = %d ret = -4",args.CandidateId,rf.me)
			reply.VoteGranted = false
		}else{
			reply.VoteGranted = true 
			reply.VoteId = rf.me
			reply.Term = rf.currTerm
			DPrintf("RequestVote: candidate = %d: voteId = %d,origin votedfor = %d",args.CandidateId,reply.VoteId,rf.votedFor)
			if rf.votedFor < 0 {
				rf.recvVoteChan <- args.CandidateId
				rf.votedFor = args.CandidateId	
			}
		}
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
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.stopRaft <- 1
	rf.mu.Lock()
	rf.state = Stopped
	rf.mu.Unlock()
	DPrintf("kill %d\n",rf.me)
	//mrf.stopHeartBreakChan <- 1
	// Your code here, if desired.
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
	rf.SetState(Follower)
	rf.leaderId = -1;
	rf.votedFor = -1;
	rf.commitIndex, rf.lastApplied, rf.currTerm = 0,0,0;
	rf.log.startIndex, rf.log.startTerm = 0,0;
	rf.log.entries = make([]LogEntry,1)
	rf.log.entries[0].Cmd = "" 
	rf.log.entries[0].Index = 0 
	rf.log.entries[0].Term = 0 
	rf.nextIndex = make([]int,len(peers))
	rf.matchIndex = make([]int,len(peers))
	rf.recvAppendChan = make(chan int,len(peers))
	rf.recvVoteChan =  make(chan int,len(peers))
	rf.heartBreakInterval = time.Duration(150*time.Millisecond)
	rf.stopRaft = make(chan int)
	rf.stopHeartBreakChan = make(chan int)
	// start leader election
	go func() {
	for rf.State() != Stopped {
		DPrintf("server.loop.run state:%s,%d", rf.State(), rf.me)
		switch rf.State() {
		case Stopped:
			return	
		case Follower:
			rf.followerLoop(applyCh)
		case Candidate:
			rf.candidateLoop(applyCh)
		case Leader:
			rf.leaderLoop(applyCh)
		}
	 }
	}()

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

func (rf *Raft) candidateLoop(applyCh chan ApplyMsg) {
	voted := false
	sumVote := 0
	var timeWaitChan <- chan time.Time
	voteRspChan := make(chan int,1)
	lastindex,lastterm := rf.log.lastInfo()
	for rf.state == "candidate" {
		if voted == false {
			rf.mu.Lock()
			rf.currTerm++
			rf.votedFor = rf.me
			rf.mu.Unlock()
			voteRspChan = make(chan int, len(rf.peers))
			for index, _ := range rf.peers {
				if index == rf.me {
					continue
				}
				rf.reqWg.Add(1)
				DPrintf("candinate loop %d: send Reqvote to %d",rf.me,index)
				go func(index int) {
					defer rf.reqWg.Done()
					rf.mu.Lock()
					req := NewRequestVoteArgs(rf.currTerm,rf.me,lastindex,lastterm)
					rf.mu.Unlock()
					rsp := NewRequestVoteReply()
					if rf.sendRequestVote(index,req,rsp) {
						if rf.State() == Candidate && rsp.VoteGranted == true {
							 DPrintf("candinate loop %d: Send %d to voteRspchan,index = %d \n",rf.me,rsp.VoteId,index)
							 voteRspChan <- rsp.VoteId
						}
					}				 
				}(index)
			}
			voted = true
			sumVote = 1
			rand.Seed(time.Now().Unix())
			waitperiod := rand.Intn(300)+500
			timeWaitChan = time.After(time.Duration(waitperiod)*time.Millisecond)
		}	
		if sumVote >= rf.CountPeer()/2+1 {
			rf.SetState(Leader)
			return
		}
		select {
			case <- rf.stopRaft:
			DPrintf("leader loop %d: stop",rf.me)
				return
			case votedId := <-voteRspChan:
				 DPrintf("candinate loop %d: Get VoteRsp From %d\n",rf.me,votedId)
				 sumVote++
			case timeout := <- timeWaitChan:
				 voted = false
				 DPrintf("candinate loop %d: timeout %v\n",rf.me,timeout)
			case leaderId := <- rf.recvAppendChan:
				 DPrintf("candinate loop %d: recv append %d,state = %s",rf.me,leaderId,rf.state)
		    case msg := <- applyCh:
			   	 DPrintf("candinate loop: get applych %v\n",msg)
			case votedfor := <- rf.recvVoteChan:
			     DPrintf("candinate loop %d: vote %d,state = %s\n",rf.me,votedfor,rf.state)
		}
	}
}

func (rf *Raft) followerLoop(applyCh chan ApplyMsg) {
	 var timeWaitChan <- chan time.Time		
	 rand.Seed(time.Now().Unix())
	 waitperiod := rand.Intn(300)+500
	 timeWaitChan = time.After(time.Duration(waitperiod)*time.Millisecond)
	 appendFromLeader := false
	 for rf.State() == Follower {
	 	DPrintf(" followerloop %d : start\n",rf.me)
		select {
			case <- rf.stopRaft:
			DPrintf("leader loop %d: stop",rf.me)
				return
			case _ = <- timeWaitChan:
			rf.mu.Lock()
			if ( rf.votedFor < 0  && appendFromLeader == false) {
					rf.state = Candidate
					DPrintf("followloop %d: timeout to become candidate\n",rf.me)
			}else{ 
				DPrintf("followerloop %d:timeout to continue election\n",rf.me)
				timeWaitChan = time.After(time.Duration(waitperiod)*time.Millisecond)
				appendFromLeader = false
				rf.votedFor = -1
			}
			rf.mu.Unlock()
		    case msg := <- applyCh:
	    	DPrintf("followerloop %d :get applych %v\n",rf.me,msg)
			case votedfor := <- rf.recvVoteChan:
			DPrintf("followerloop %d : recv Votereq from %d\n",rf.me,votedfor)
			case leaderId := <- rf.recvAppendChan:
			rf.mu.Lock()
			if leaderId ==  rf.leaderId || rf.leaderId < 0 {
				rf.leaderId = leaderId
				appendFromLeader = true;
				DPrintf("followerloop %d: recv append from %d\n",leaderId,rf.me)
			}else {
				DPrintf("followerloop %d: recv append from %d,origin leaderId = %d\n",leaderId,rf.me,rf.leaderId)
			}
			rf.mu.Unlock()
		}
	 }
}

func (rf *Raft) StartHeartBreak() {
	for {
		rf.mu.Lock()
		DPrintf("%d start heart break,state = %s",rf.me,rf.state)
		if rf.state != Leader {
			DPrintf("%d start heart break,state = %s return heartbreak",rf.me,rf.state)
			return	
		}
		rf.mu.Unlock()
		rf.heartBreakTimer = time.NewTimer(rf.heartBreakInterval)
		select {
			case <- rf.stopHeartBreakChan:
			DPrintf("%d stop heart break",rf.me)
			return
			case <- rf.heartBreakTimer.C:
			rf.DoOneHeartBreak()	
		}
	}
}

func (rf *Raft) DoOneHeartBreak() {
		lastindex,lastterm := rf.log.lastInfo()
		for index, _ := range rf.peers {
			if index == rf.me {
				continue
			}
			go func(index int) {
				rf.mu.Lock()	
				req := NewAppendEntriesArgs(rf.currTerm,rf.me,lastindex,lastterm,nil,rf.commitIndex)
				rsp := NewAppendEntriesReply(0, false) 
				rf.mu.Unlock()
				DPrintf("leadeloop: %d Append nil entries %d",rf.me,index)
				rf.SendAppendEntries(index,req,rsp)
				//rf.ProcessAppendReply()
	    	}(index)
		}		
}

func (rf *Raft) leaderLoop(applyCh chan ApplyMsg) {
	rf.DoOneHeartBreak()
	go rf.StartHeartBreak()
	for rf.State() == Leader {
   	 	DPrintf(" start leader loop %d\n",rf.me)
		select {
			case <- rf.stopRaft:
				rf.stopHeartBreakChan <- 1
				DPrintf("leader loop %d: stop",rf.me)
				return 
		    case msg := <- applyCh:
	    	DPrintf("leader loop %d: get applych %v\n",rf.me,msg)
			case votedfor := <- rf.recvVoteChan:
			DPrintf("leader loop %d: recv vote %d\n",rf.me,votedfor)
			if rf.State() != Leader {
				DPrintf("after recv vote,stop heart break ")	
				rf.stopHeartBreakChan <- 1
			}
			case leaderId := <- rf.recvAppendChan:
			DPrintf("leader loop %d: recv append from %d\n",rf.me,leaderId)
			if rf.State() != Leader {
				DPrintf("after recv append,stop heart break ")	
				rf.stopHeartBreakChan <- 2
			}
		}
	}
	DPrintf("leaderloop %d end\n",rf.me)
}


