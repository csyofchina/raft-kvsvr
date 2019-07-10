package shardkv

import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "labgob"
import "time"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	SeqNum    int
	Operation string
	Key       string
	Value     string
}

type ShardChange struct {
	KVMap      map[string]string
	LastSeqMap map[int64]LastSeq
	Shard      int
	Num        int
}

type ConfigChange struct {
	Config shardmaster.Config
}

type ShardKV struct {
	mu               sync.Mutex
	me               int
	rf               *raft.Raft
	applyCh          chan raft.ApplyMsg
	make_end         func(string) *labrpc.ClientEnd
	gid              int
	masters          []*labrpc.ClientEnd
	maxraftstate     int // snapshot if log grows this big
	kvMap            map[string]string
	clientMap        map[int64]LastSeq
	chRecMap         map[int](chan string)
	toDoShardMap     map[int][]ShardChange //key:config.Num
	stcmdCh          chan struct{}
	closeCh          chan struct{}
	mck              *shardmaster.Clerk
	configs          []shardmaster.Config
	checkConfigTimer *time.Timer
	// Your definitions here.
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	clientId := args.Id
	//Is leader?
	ok, preTerm := kv.rf.IsLeader()
	if ok == false {
		reply.WrongLeader = true
		reply.Err = ""
		DPrintf("kv %d proc get:client %d call wrong leader", kv.me, args.Id)
		return
	}

	kv.mu.Lock()
	//check wrong group
	if kv.configs[0].Shards[key2shard(args.Key)] != kv.gid {
		reply.WrongLeader = false
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	lastSeq, ok := kv.clientMap[clientId]
	if ok == true {
		if args.SeqNum == lastSeq.SeqNum {
			kv.mu.Unlock()
			reply.Value = lastSeq.LastReply.Value
			reply.Err = ""
			reply.WrongLeader = false
			DPrintf("Get: %d call %d get before seqNum = %d",
				clientId, kv.me, args.SeqNum)
			return
		}
	}
	kv.mu.Unlock()
	//raft
	cmd := Op{Key: args.Key, Value: "",
		Operation: "Get", ClientId: clientId, SeqNum: args.SeqNum}
	index, _, _ := kv.rf.Start(cmd)
	recCh := make(chan string)
	kv.mu.Lock()
	kv.chRecMap[index] = recCh
	kv.mu.Unlock()
	DPrintf("kv %d, start cmd with index = %d", kv.me, index)
	select {
	case <-recCh:
		ok, nowTerm := kv.rf.IsLeader() //add term information
		if ok && nowTerm == preTerm {
			reply.WrongLeader = false
			kv.mu.Lock()
			if val, ok := kv.kvMap[args.Key]; ok {
				DPrintf("kv %d finish Get,key=%s,value=%s", kv.me, args.Key, reply.Value)
				reply.Value = val
				reply.Err = OK
			} else {
				DPrintf("kv %d  dont have key%s", kv.me, args.Key)
				reply.Err = ErrNoKey
			}
			kv.mu.Unlock()
		} else {
			reply.WrongLeader = true
		}
	case <-kv.stcmdCh:
		DPrintf("Stcmd get wait")
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	clientId := args.Id
	//Is leader?
	ok, preTerm := kv.rf.IsLeader()
	if ok == false {
		reply.WrongLeader = true
		reply.Err = ""
		DPrintf("kv %d proc put : client %d call wrong leader", kv.me, args.Id)
		return
	}
	//New clinet?
	/*
		if args.Id < 0 {
			reply.Id = kv.AllocateId()
			clientId = reply.Id
		}else {
			clientId = args.Id
		}*/

	kv.mu.Lock()
	/*
		for {
			if len(kv.configs) > 0 {
				break
			} else {
				kv.mu.Unlock()
				time.Sleep(100 * time.Millisecond)
				kv.mu.Lock()
			}
		}
	*/
	if kv.configs[0].Shards[key2shard(args.Key)] != kv.gid {
		reply.WrongLeader = false
		reply.Err = ErrWrongGroup
		DPrintf("kv %d,key = %s, shard = %d, config = %+v,gid = %d",
			kv.me, args.Key, key2shard(args.Key), kv.configs[0],kv.gid)
		kv.mu.Unlock()
		return
	}
	lastSeq, ok := kv.clientMap[clientId]
	if ok == true {
		if args.SeqNum == lastSeq.SeqNum {
			kv.mu.Unlock()
			reply.Err = ""
			reply.WrongLeader = false
			DPrintf("kv %d Put: %d call %d get before seqNum = %d",
				kv.me,clientId, kv.me, args.SeqNum)
			return
		}
	}
	kv.mu.Unlock()

	//raft
	cmd := Op{Key: args.Key, Value: args.Value,
		Operation: args.Op, ClientId: clientId, SeqNum: args.SeqNum}
	index, _, _ := kv.rf.Start(cmd)
	recCh := make(chan string)
	kv.mu.Lock()
	kv.chRecMap[index] = recCh
	kv.mu.Unlock()
	DPrintf("kv %d ,start cmd with index = %d,args=%+v", kv.me, index, args)
	select {
	case <-recCh:
		ok, nowTerm := kv.rf.IsLeader() //add term information
		if ok && nowTerm == preTerm {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
			DPrintf("kv %d, term change",kv.me)
		}
		DPrintf("kv %d %d finish PutAppend ,index = %d,clientId = %d,seqNum = %d,key =%s,value=%s",
			kv.gid, kv.me, index, clientId, args.SeqNum, args.Key, args.Value)
	case <-kv.closeCh:
		DPrintf("stop put wait")
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.configs = make([]shardmaster.Config, 2)
	kv.configs[0].Num = -1
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.clientMap = make(map[int64]LastSeq)
	kv.kvMap = make(map[string]string)
	kv.closeCh = make(chan struct{})
	kv.chRecMap = make(map[int]chan string)
	kv.toDoShardMap = make(map[int][]ShardChange)
	go kv.ApplyKVMap()
	go kv.CheckConfig()

	return kv
}

func (kv *ShardKV) ApplyKVMap() {
	for {
		select {
		case msg, ok := <-kv.applyCh:
			if ok {
				if msg.Command != nil && msg.CommandValid {
					value := ""
					kv.mu.Lock()
					switch cmd := msg.Command.(type) {
					case Op:
						if cmd.Operation == "Get" {
							value = kv.kvMap[cmd.Key]
							lastReply := GetReply{WrongLeader: false, Value: value}
							kv.clientMap[cmd.ClientId] = LastSeq{SeqNum: cmd.SeqNum, LastReply: lastReply}
						} else if cmd.Operation == "Append" {
							preSeq, ok := kv.clientMap[cmd.ClientId]
							if ok {
								if preSeq.SeqNum == cmd.SeqNum {
									DPrintf("kv %d index = %d,client %d append key=%s value=%s again",
										kv.me, msg.CommandIndex, cmd.ClientId, cmd.Key, cmd.Value)
									kv.mu.Unlock()
									break
								}
							}
							kv.kvMap[cmd.Key] += cmd.Value
							value = kv.kvMap[cmd.Key]
							kv.clientMap[cmd.ClientId] = LastSeq{SeqNum: cmd.SeqNum}
							DPrintf("kv %d index = %d,client %d append key=%s,value=%s",
								kv.me, msg.CommandIndex, cmd.ClientId, cmd.Key, cmd.Value)
						} else if cmd.Operation == "Put" {
							preSeq, ok := kv.clientMap[cmd.ClientId]
							if ok {
								if preSeq.SeqNum == cmd.SeqNum {
									DPrintf("kv %d index = %d,client %d put key=%s value=%s again",
										kv.me, msg.CommandIndex, cmd.ClientId, cmd.Key, cmd.Value)
									kv.mu.Unlock()
									break
								}
							}
							kv.kvMap[cmd.Key] = cmd.Value
							value = cmd.Value
							kv.clientMap[cmd.ClientId] = LastSeq{SeqNum: cmd.SeqNum}
							DPrintf("kv %d index = %d,client %d put key=%s,value=%s",
								kv.me, msg.CommandIndex, cmd.ClientId, cmd.Key, value)
						} else {
							DPrintf("Unclear cmd %s", cmd.Operation)
						}
						chRec, ok := kv.chRecMap[msg.CommandIndex]
						if ok && chRec != nil {
							close(chRec)
							delete(kv.chRecMap, msg.CommandIndex)
						}
					case ShardChange:
						if cmd.Num == kv.configs[0].Num {
							kv.ApplyShardChange(&cmd)
						} else if cmd.Num > kv.configs[0].Num {
							if _, ok := kv.toDoShardMap[cmd.Num]; ok {
								kv.toDoShardMap[cmd.Num] = append(kv.toDoShardMap[cmd.Num], cmd)
							} else {
								var shardChangeSlice = []ShardChange{cmd}
								kv.toDoShardMap[cmd.Num] = shardChangeSlice
							}
						}
					case ConfigChange:
						if cmd.Config.Num == kv.configs[0].Num+1 {
							newconfig := make([]shardmaster.Config, len(kv.configs)+1)
							newconfig[0] = cmd.Config
							copy(newconfig[1:], kv.configs)
							kv.configs = newconfig
							DPrintf("kv %d %d update config update to %d", kv.gid,kv.me, cmd.Config.Num)
							if shardChangeSlice, ok := kv.toDoShardMap[cmd.Config.Num]; ok {
								if len(shardChangeSlice) != 0 {
									for _, shardChange := range shardChangeSlice {
										kv.ApplyShardChange(&shardChange)
									}
								}
							}
						}
					}//swtich
					kv.mu.Unlock()
				} //if
			} //if
		case <-kv.closeCh:
			return
		} //select
	} //for
}

func (kv *ShardKV) ProcPullShard(args PullShardArgs, reply PullShardReply) {
	ok, _ := kv.rf.IsLeader()
	if ok == false {
		return
	}
	shardChange := ShardChange{
		KVMap:      reply.KVMap,
		LastSeqMap: reply.LastSeqMap,
		Shard:      reply.Shard,
		Num:        args.Num}
	DPrintf("kv %d %d start to ProcPullShard,reply=%+v,,cmd=%+v",
		kv.gid, kv.me, reply,shardChange)
	index ,_,_ := kv.rf.Start(shardChange)
	DPrintf("kv %d %d,cmd index=%d",kv.gid,kv.me,index)
}

func (kv *ShardKV) PullShard(args *PullShardArgs, reply *PullShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ok, _ := kv.rf.IsLeader()
	if ok == false {
		reply.WrongLeader = true
		reply.Err = ""
		DPrintf("kv%d: ConfigChange wrong leader", kv.me)
		return
	}
	if args.Num > kv.configs[0].Num {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}
	reply.KVMap = make(map[string]string)
	reply.LastSeqMap = make(map[int64]LastSeq)
	for key, val := range kv.kvMap {
		if key2shard(key) == args.Shard {
			reply.KVMap[key] = val
		}
	}
	reply.LastSeqMap = kv.clientMap
	reply.WrongLeader = false
	reply.Err = ""
}

func (kv *ShardKV) SendPullShard(old *shardmaster.Config, new *shardmaster.Config) {
	if old.Num < new.Num {
		shardgidMap := make(map[int]int)
		for shard, gid := range new.Shards {
			if gid == kv.gid {
				if old.Shards[shard] != gid {
					DPrintf("kv %d change config, new shard %d in %d",
						kv.me, shard, old.Shards[shard])
					shardgidMap[shard] = old.Shards[shard]
				}
			}
		}
		//Send ChangeConfig RPC
		for shard, gid := range shardgidMap {
			go func(shard int, gid int) {
				if svrs, ok := old.Groups[gid]; ok {
					args := PullShardArgs{
						Shard: shard,
						Gid:   gid,
						Num:   old.Num,
					}
					for _, svr := range svrs {
						svrend := kv.make_end(svr)
						var reply PullShardReply
						for {
							ok := svrend.Call("ShardKV.PullShard", &args, &reply)
							if ok {
								DPrintf("kv %d call svr %s success,args=%+v,reply=%+v",
									kv.me, svr, args, reply)
								kv.ProcPullShard(args, reply)
								break
							} else {
								DPrintf("kv %d call svr %s fail", kv.me, svr)
								time.Sleep(200 * time.Millisecond)
							}
						}
					}
				}
			}(shard, gid)
		}
	}
}

func (kv *ShardKV) CheckConfig() {
	checkInterval := time.Duration(100 * time.Millisecond)
	kv.checkConfigTimer = time.NewTimer(checkInterval)
	for {	
		select {
		case <-kv.closeCh:
			return
		default:
			config := kv.mck.Query(-1)
			DPrintf("kv %d %d check config,config = %+v",kv.gid,kv.me,config)
			if config.Num == 0 {
				continue
			}
			kv.mu.Lock()
			if kv.configs[0].Num == -1 {
				kv.configs[0] = config
				DPrintf("kv %d %d update1 config to %d",kv.gid, kv.me, config.Num)
				//if isLeader, _ := kv.rf.IsLeader(); isLeader {
				//	kv.rf.Start(ConfigChange{Config: config})
				//}
			} else if config.Num > kv.configs[0].Num {
				DPrintf("kv %d %d update2 config to %d",kv.gid,kv.me,config.Num)
				kv.SendPullShard(&kv.configs[0], &config)
				kv.configs[1] = kv.configs[0]
				kv.configs[0] = config
				DPrintf("kv %d %d finish SendPullShard",kv.gid, kv.me)
				/*
				if isLeader, _ := kv.rf.IsLeader(); isLeader {
					configChange := ConfigChange{Config: config}
					index,_,_ := kv.rf.Start(configChange)
					DPrintf("kv %d %d start cmd,index=%d,cmd=%+v",
					kv.gid,kv.me,index,configChange)
				}*/
				if shardChangeSlice, ok := kv.toDoShardMap[config.Num]; ok {
					if len(shardChangeSlice) != 0 {
						for _, shardChange := range shardChangeSlice {
							kv.ApplyShardChange(&shardChange)
						}
					}
				}
			}
			kv.mu.Unlock()	
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) ApplyShardChange(shardChange *ShardChange) {
	for key, val := range shardChange.KVMap {
		kv.kvMap[key] = val
	}
	for key, val := range shardChange.LastSeqMap {
		kv.clientMap[key] = LastSeq{SeqNum: val.SeqNum, LastReply: val.LastReply}
	}

}
