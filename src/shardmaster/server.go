package shardmaster

import "raft"
import "labrpc"
import "sync"
import "labgob"

const (
	Join = "join"
	Leave = "leave"
	Move = "move"
	Query = "query"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	lastSeq map[int64]int
	// Your data here.
	closeCh chan struct{}
	notifyCh	map[int]chan struct{}
	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	ClientId int64  
	SeqNum    int    
	Op       string 
	Servers map[int][]string 
	GIDs    []int            
	Shard   int              
	GID     int              
	Num     int              
}

func (sm *ShardMaster) makeshard(op string, opgid []int, config *Config) {
	maxId := -1
	minId := -1
	var shardcnt map[int]int
	shardcnt = make(map[int]int)
	if len(opgid) == 0 {
		return
	}
	for key, _ := range config.Groups {
		shardcnt[key] = 0
	}
	if op == Join {
		for _, gid := range opgid {
			shardcnt[gid] = 0
		}
		for index, value := range config.Shards {
			if value <= 0 {
				value = opgid[0]
				config.Shards[index] = value 
			}
			shardcnt[value]++
		}
	} else if op == Leave {
		nodelId := -1
		//delete node set 0 in shards
		for  _, gid := range opgid {
			for index, value := range config.Shards {
				if value == gid {
					config.Shards[index] = 0
				} 
			}
		}
		for _ , value := range config.Shards {
			if value > 0 {
				nodelId = value
			}	
		}
		if nodelId < 0 {
			DPrintf("del all shards")
			return
		}else{
			for index , value := range config.Shards {
				if value == 0 {
					value = nodelId
				}	
				config.Shards[index] = value
				shardcnt[value]++
			}
		}
	} else if op == Move {
		for _, value := range config.Shards {
			shardcnt[value]++
		}
	}

	for {
		max := 0
		min := 999
		for gid, cnt := range shardcnt {
			if cnt > max {
				max = cnt
				maxId = gid
			} 
			if cnt < min {
				min = cnt
				minId = gid 
			}
		}
		DPrintf("configshard = %v,shardcnt = %v,max = %d, maxId = %d, min = %d, minId = %d",
			config.Shards,shardcnt,max,maxId,min,minId)
		if max-min > 1 {
			for index, gid := range config.Shards {
				if gid == maxId {
					config.Shards[index] = minId
					shardcnt[maxId]--
					shardcnt[minId]++
					break
				}
			}
			DPrintf("after sort Shards=%v", config.Shards)
		} else {
			DPrintf("Final Shards=%v", config.Shards)
			return
		}
	}

}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	DPrintf("svr %d start to join,args=%+v",sm.me,args)
	sm.mu.Lock()
	ok, _ := sm.rf.IsLeader()
	if !ok {
		sm.mu.Unlock()
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
	}
	if lastSeq, ok := sm.lastSeq[args.ClientId]; ok {
		// filter duplicate
		if args.SeqNum <= lastSeq {
			sm.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = "repeat req"
			return
		}
	}

	cmd := Op{
		ClientId: args.ClientId, 
		SeqNum: args.SeqNum, 
		Op: Join, 
		Servers: args.Servers,
	}
	index, term, _ := sm.rf.Start(cmd)
	ch := make(chan struct{})
	sm.notifyCh[index] = ch
	sm.mu.Unlock()

	// wait for Raft to complete agreement
	select {
	case <-ch:
		// lose leadership?
		isLeader, currTerm := sm.rf.IsLeader()

		// what if still leader, but different term? just let client retry
		if !isLeader || term != currTerm {
			reply.WrongLeader = false
			reply.Err = "repeat req"
			return
		}
	case <-sm.closeCh:
	}
	DPrintf("svr %d after join config = %+v",sm.me,sm.configs)
	
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	DPrintf("svr %d start to Leave,args=%+v",sm.me,args)
	sm.mu.Lock()
	ok, _ := sm.rf.IsLeader()
	if !ok {
		sm.mu.Unlock()
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
	}
	if lastSeq, ok := sm.lastSeq[args.ClientId]; ok {
		// filter duplicate
		if args.SeqNum <= lastSeq {
			sm.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = "repeat req"
			return
		}
	}
	cmd := Op{
		ClientId: args.ClientId, 
		SeqNum: args.SeqNum, 
		Op: Leave, 
		GIDs: args.GIDs,
	}
	index, term, _ := sm.rf.Start(cmd)
	ch := make(chan struct{})
	sm.notifyCh[index] = ch
	sm.mu.Unlock()

	// wait for Raft to complete agreement
	select {
	case <-ch:
		// lose leadership?
		isLeader, currTerm := sm.rf.IsLeader()

		// what if still leader, but different term? just let client retry
		if !isLeader || term != currTerm {
			reply.WrongLeader = false
			reply.Err = "repeat req"
			return
		}
	case <-sm.closeCh:
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	DPrintf("svr %d start to Move,args=%+v",sm.me,args)
	sm.mu.Lock()
	ok, _ := sm.rf.IsLeader()
	if !ok {
		sm.mu.Unlock()
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
	}
	if lastSeq, ok := sm.lastSeq[args.ClientId]; ok {
		// filter duplicate
		if args.SeqNum <= lastSeq {
			sm.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = "repeat req"
			return
		}
	}
	cmd := Op{
		ClientId: args.ClientId, 
		SeqNum: args.SeqNum, 
		Op: Move, 
		Shard: args.Shard,
		GID: args.GID,
	}
	index, term, _ := sm.rf.Start(cmd)
	ch := make(chan struct{})
	sm.notifyCh[index] = ch
	sm.mu.Unlock()

	// wait for Raft to complete agreement
	select {
	case <-ch:
		// lose leadership?
		isLeader, currTerm := sm.rf.IsLeader()

		// what if still leader, but different term? just let client retry
		if !isLeader || term != currTerm {
			reply.WrongLeader = false
			reply.Err = "repeat req"
			return
		}
	case <-sm.closeCh:
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	DPrintf("svr %d start to Query,args=%+v",sm.me,args)
	sm.mu.Lock()
	ok, _ := sm.rf.IsLeader()
	if !ok {
		sm.mu.Unlock()
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
	}
	if lastSeq, ok := sm.lastSeq[args.ClientId]; ok {
		// filter duplicate
		if args.SeqNum <= lastSeq {
			sm.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = "repeat req"
			return
		}
	}
	cmd := Op{
		ClientId: args.ClientId, 
		SeqNum: args.SeqNum, 
		Num: args.Num,
		Op:  Query,
	}
	index, term, _ := sm.rf.Start(cmd)
	ch := make(chan struct{})
	sm.notifyCh[index] = ch
	sm.mu.Unlock()

	// wait for Raft to complete agreement
	select {
	case <-ch:
		// lose leadership?
		isLeader, currTerm := sm.rf.IsLeader()

		// what if still leader, but different term? just let client retry
		if !isLeader || term != currTerm {
			reply.WrongLeader = false
			reply.Err = "repeat req"
			return
		}
		sm.copyConfig(args.Num, &reply.Config) 
	case <-sm.closeCh:
	}
	
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	close(sm.closeCh)
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.configs[0].Num = 0;
	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.notifyCh = make(map[int]chan struct{})
	sm.lastSeq = make(map[int64]int)
	sm.closeCh = make(chan struct{})
	// Your code here.
	go sm.applyConfig()
	return sm
}

func (sm *ShardMaster) applyConfig() {
	for {
		select {
		case <-sm.closeCh:
			return
		case msg, ok := <-sm.applyCh:
			// have client's request? must filter duplicate command
			if ok && msg.Command != nil {
				cmd := msg.Command.(Op)
				sm.mu.Lock()
				if lastSeq, ok := sm.lastSeq[cmd.ClientId]; !ok || lastSeq < cmd.SeqNum {
					// update to latest
					sm.lastSeq[cmd.ClientId] = cmd.SeqNum
					//DPrintf("cmd = %s",cmd.Op)
					switch cmd.Op {
					case Join:
						sm.configJoin(cmd.Servers)
					case Leave:
						sm.configLeave(cmd.GIDs)
					case Move:
						sm.configMove(cmd.Shard, cmd.GID)
					case Query:
						// no need to modify config
					default:
						panic("invalid command operation")
					}
				}
				// notify channel
				if notifyCh, ok := sm.notifyCh[msg.CommandIndex]; ok && notifyCh != nil {
					close(notifyCh)
					delete(sm.notifyCh, msg.CommandIndex)
				}
				sm.mu.Unlock()
			}
		}
	}
}

func (sm *ShardMaster) copyConfig(index int, config *Config) {
	if index == -1 || index >= len(sm.configs) {
		index = len(sm.configs) - 1
	}
	config.Num = sm.configs[index].Num
	config.Shards = sm.configs[index].Shards
	config.Groups = make(map[int][]string)
	for k, v := range sm.configs[index].Groups {
		var servers = make([]string, len(v))
		copy(servers, v)
		config.Groups[k] = servers
	}
}

func (sm *ShardMaster) configJoin(servers map[int][]string) {
	DPrintf("svr %d start to join, servers = %+v",sm.me,servers)
	var config Config
	sm.copyConfig(-1,&config)
	config.Num++
	var opid []int
	for key, val := range servers {
		opid = append(opid,key)
		config.Groups[key] = val
	}
	DPrintf("opid = %+v,config = %+v",opid,config)
	sm.makeshard(Join, opid, &config)
	DPrintf("svr %d after join,config = %+v",sm.me,config)
	sm.configs = append(sm.configs, config)
		//lastSvrNum := len(sm.configs[n-1].Groups)
}

func (sm *ShardMaster) configMove(shard int, GID int) {
	var config Config
	sm.copyConfig(-1,&config)
	config.Num++
	config.Shards[shard]=GID
	DPrintf("svr %d start to move, shard=%d, GID=%d, config=%+v",
		sm.me,shard,GID,config)
	sm.makeshard(Move, nil, &config)
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) configLeave(GIDs []int) {
	var config Config
	sm.copyConfig(-1,&config)
	config.Num++
	DPrintf("svr %d start to Leave, GIDs=%+v,config=%+v",sm.me,GIDs,config)
	for _ , key := range GIDs {
		if _, ok := config.Groups[key]; ok {
			delete(config.Groups,key)
		}
	} 
	sm.makeshard(Leave, GIDs , &config)
	sm.configs = append(sm.configs, config)
}