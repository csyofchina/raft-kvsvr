package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 1 

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type LastSeq struct {
	SeqNum	int64
	LastReply  GetReply
}

type Op struct {
	Key		string
	Value	string
	Operation	string
	ClientId	int64
	SeqNum		int64
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu      		sync.Mutex
	me      		int
	rf      		*raft.Raft
	kvMap			map[string]string
	applyCh 		chan raft.ApplyMsg
	clientMap 		map[int64]LastSeq
	chRecMap		map[int](chan string)		
	stopCh 			chan struct{}
	maxraftstate 	int // snapshot if log grows this big
	clientNum		int64
	// Your definitions here.
}

func (kv *KVServer) AllocateId() int64 {
	kv.mu.Lock()
	kv.clientNum = kv.clientNum + 1
	newId := kv.clientNum
 	kv.mu.Unlock()
	return newId
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	var clientId  int64
	//Is leader?
	ok,preTerm := kv.rf.IsLeader() 
	if ok == false {	
		reply.WrongLeader = true
		reply.Err = ""
		reply.Id = args.Id
		DPrintf("kv%d:client %d call wrong leader",kv.me,args.Id)
		return
	}
	
	//New clinet?
	if args.Id <= 0 {
		reply.Id = kv.AllocateId()
		clientId = reply.Id 
	}else {
		clientId = args.Id
	}
	kv.mu.Lock()	
	lastSeq, ok := kv.clientMap[clientId]
	kv.mu.Unlock()
	if ok == true {
		if args.SeqNum == lastSeq.SeqNum {
			reply.Value  = lastSeq.LastReply.Value
			reply.Err = ""
			reply.WrongLeader = false
			DPrintf("Get: %d call %d get before seqNum = %d",
				clientId,kv.me,args.SeqNum)
			return
		}
	}
	//raft
	cmd := Op{Key:args.Key,Value:"",
		Operation:"Get",ClientId:clientId,SeqNum:args.SeqNum}
	index,_,_ := kv.rf.Start(cmd)
	recCh := make(chan string)
	kv.mu.Lock()
	kv.chRecMap[index] = recCh
	kv.mu.Unlock()
	DPrintf("kv %d, start cmd with index = %d",kv.me,index)
	select{
		case <-recCh:
			 ok,nowTerm := kv.rf.IsLeader()//add term information 
		     if ok && nowTerm == preTerm {
			    reply.WrongLeader = false 	
		     }else{
				 reply.WrongLeader = true
		     }  
			 reply.Err = ""
			 kv.mu.Lock()
			 reply.Value = kv.kvMap[args.Key] 
			 kv.mu.Unlock()
			 DPrintf("kv %d finish Get,key=%s,value=%s",kv.me,args.Key,reply.Value)
		case  <-kv.stopCh:
			 DPrintf("Stop get wait")
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var clientId  int64
	//Is leader?
	ok,preTerm := kv.rf.IsLeader() 
	if ok == false {	
		reply.WrongLeader = true
		reply.Err = ""
		reply.Id = args.Id
		DPrintf("kv%d:client %d call wrong leader",kv.me,args.Id)
		return
	}
	
	//New clinet?
	if args.Id <= 0 {
		reply.Id = kv.AllocateId()
		clientId = reply.Id 
	}else {
		clientId = args.Id
	}
	
	kv.mu.Lock()	
	lastSeq, ok := kv.clientMap[clientId]
	kv.mu.Unlock()
	if ok == true {
		if args.SeqNum == lastSeq.SeqNum {
			reply.Id = clientId
			reply.Err = ""
			reply.WrongLeader = false
			DPrintf("Put: %d call %d get before seqNum = %d",
				clientId,kv.me,args.SeqNum)
			return
		}
	}
	
	//raft
	cmd := Op{Key:args.Key,Value:args.Value,
		Operation:args.Op,ClientId:clientId,SeqNum:args.SeqNum}
	index,_,_ := kv.rf.Start(cmd)
	recCh := make(chan string)
	kv.mu.Lock()
	kv.chRecMap[index] = recCh
	kv.mu.Unlock()
	DPrintf("kv %d ,start cmd with index = %d,args=%+v",kv.me,index,args)
	select{
		case  <-recCh:
		  	 ok,nowTerm := kv.rf.IsLeader()//add term information 
		     if ok && nowTerm == preTerm {
			    reply.WrongLeader = false 	
		     }else{
				 reply.WrongLeader = true
		     }  
			 reply.Id = clientId
			 reply.Err = ""
			 DPrintf("kv %d finish PutAppend ,index = %d,seqNum = %d,reply%+v",
			 	kv.me,index,args.SeqNum,reply)
		case  <-kv.stopCh:
			DPrintf("stop put wait")
	}
}

func (kv *KVServer) WriteKvMap() {
	for{
		select{
			case msg,ok := <-kv.applyCh:
				if ok {
					if msg.Command != nil && msg.CommandValid {
						op := msg.Command.(Op)
						kv.mu.Lock()
						value := "" 
						if op.Operation == "Put" {
							preSeq,ok := kv.clientMap[op.ClientId] 
							if ok {
								if preSeq.SeqNum == op.SeqNum {
									DPrintf("kv %d index = %d,put key=%s value=%s again",
									kv.me,msg.CommandIndex,op.Key,op.Value)
									break
								}
							}
							kv.kvMap[op.Key] = op.Value
							value = op.Value
							kv.clientMap[op.ClientId] = LastSeq{SeqNum:op.SeqNum}
							DPrintf("kv %d index = %d,put,key=%s,value=%s",
							kv.me,msg.CommandIndex,op.Key,value)
						}else if op.Operation == "Append" {
							preSeq,ok := kv.clientMap[op.ClientId] 
							if ok {
								if preSeq.SeqNum == op.SeqNum {
									DPrintf("kv %d index = %d,append key=%s value=%s again",
									kv.me,msg.CommandIndex,op.Key,op.Value)
									break
								}
							}
							kv.kvMap[op.Key] += op.Value
							value = kv.kvMap[op.Key]
							kv.clientMap[op.ClientId] = LastSeq{SeqNum:op.SeqNum}
							DPrintf("kv %d index = %d,append,key=%s,value=%s",
							kv.me,msg.CommandIndex,op.Key,value)
					    }else if op.Operation == "Get" {
					    	value = kv.kvMap[op.Key]
					    	lastReply := GetReply{WrongLeader:false,Value:value,Id:op.ClientId}
							kv.clientMap[op.ClientId] = LastSeq{SeqNum:op.SeqNum,LastReply: lastReply}
					    }else{
					    	DPrintf("Unclear op %s",op.Operation)
					    }
					    chRec, ok := kv.chRecMap[msg.CommandIndex]
						
					    if ok && chRec != nil {
							close(chRec)
							delete(kv.chRecMap,msg.CommandIndex)   	
					    }
					    kv.mu.Unlock()
					 }else{
					 	DPrintf("Command nil")
					 }
				}else{
					DPrintf("apply ch close?")
				}
			case <-kv.stopCh:
				DPrintf("stop writekvmap %d",kv.me)
				return
		}
	}
	
}
//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	DPrintf("start to kill %d ",kv.me)
	go func() {
		kv.rf.Kill()
	}()
	close(kv.stopCh)
//	DPrintf("%d wait for close raft", kv.me)
	DPrintf("kill %d success",kv.me)
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.clientNum = 0
	kv.stopCh = make(chan struct{})
	kv.chRecMap = make(map[int](chan string))
	kv.clientMap = make(map[int64]LastSeq)
	kv.kvMap = make(map[string]string)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	go kv.WriteKvMap()
	return kv
}
