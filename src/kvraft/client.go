package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "time"
import "sync"

var clerknum int64 = 0 
func (ck *Clerk) GetClientId() (ret int64) {
	ck.mu.Lock()
	clerknum++
	ret = clerknum
	ck.mu.Unlock()
	return
}

type Clerk struct {
	servers    []*labrpc.ClientEnd
	lastLeader int
	Id         int64
	SeqNum     int64
	mu         sync.Mutex
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}


func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.lastLeader = -1
	ck.Id = ck.GetClientId()
	DPrintf("MakeClerk")
	ck.SeqNum = nrand()
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) ProcessGetRsp(index int, reply *GetReply) (bool, string) {
	if reply.WrongLeader == true {
		DPrintf("client %d call get %d ,wrong leader,reply=%+v",
			ck.Id, index, reply)
	} else {
		ck.mu.Lock()
		if ck.lastLeader != index {
			ck.lastLeader = index
		}
		ck.mu.Unlock()
		if reply.Err == "" {
			DPrintf("client %d, get Success,reply:+%+v", ck.Id, reply)
			return true, reply.Value
		} else {
			DPrintf("Err:%s", reply.Err)
		}
	}
	return false, ""
}

func (ck *Clerk) ProcessPutAppendRsp(index int, reply *PutAppendReply) bool {
	if reply.WrongLeader == true {
		DPrintf("client %d call put %d ,wrong leader,reply=%+v",
			ck.Id, index, reply)
	} else {
		ck.mu.Lock()
		if ck.lastLeader != index {
			ck.lastLeader = index
		}
		ck.mu.Unlock()
		if reply.Err == "" {
			//DPrintf("client %d, put Success,reply:+%+v", ck.Id, reply)
			return true
		} else {
			DPrintf("Err:%s", reply.Err)
		}
	}
	return false
}

func (ck *Clerk) Get(key string) string {
	ck.SeqNum = ck.SeqNum + 1
	seqNum := ck.SeqNum
	index := 0
	n := len(ck.servers)
	connectcnt := 0
	DPrintf("client %d start to Get", ck.Id)
	for {
		connectcnt++
		if connectcnt%n == 0 {
			time.Sleep(time.Duration(250)*time.Millisecond)
		}
		DPrintf("client %d start to call get %d,seq=%d", index, index, seqNum)
		done := make(chan bool, 1)
		//value := ""
		if ck.lastLeader > 0 {
			index = ck.lastLeader
		}
		args := NewGetArgs(key, ck.Id, seqNum)
		reply := NewGetReply()
		go func() {
			ok := ck.sendGet(index, args, reply)
			done <- ok
		}()
		select {
		case <-time.After(200 * time.Millisecond):
			DPrintf("client %d get timeout,leader:%d", ck.Id, index)
			index = (index + 1) % n
			ck.lastLeader = -1
		case ok := <-done:
			if !ok {
				DPrintf("client %d call get %d fail", ck.Id, index)
			} else {
				ret, value := ck.ProcessGetRsp(index, reply)
				if ret {
					return value
				} 
			}
			ck.lastLeader = -1
			index = (index + 1) % n
		}

	}
	// You will have to modify this function.
}

func (ck *Clerk) sendGet(index int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[index].Call("KVServer.Get", args, reply)
	//DPrintf("sendGet,args=%+v,reply=%+v",args,reply)
	return ok
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.SeqNum = ck.SeqNum + 1
	seqNum := ck.SeqNum
	n := len(ck.servers)
	connectcnt := 0
	//DPrintf("client %d start to put key = %s,value = %s", ck.Id, key, value)
	index := 0
	for {
		connectcnt++
		if connectcnt%n == 0 {
			time.Sleep(time.Duration(250)*time.Millisecond)
		}
		done := make(chan bool, 1)
		if ck.lastLeader > 0 {
			index = ck.lastLeader
		}
		DPrintf("client %d start to call put %d,seqNum %d", ck.Id, index, seqNum)
		args := NewPutAppendArgs(key, value, op, ck.Id, seqNum)
		reply := NewPutAppendReply()
		go func() {
			ok := ck.sendPutAppend(index, args, reply)
			done <- ok
		}()
		select {
		case <-time.After(500 * time.Millisecond):
			DPrintf("client %d timeout,leader:%d,connect cnt = %d", 
				ck.Id, index,connectcnt)
			index = (index + 1) % n
			ck.lastLeader = -1
			continue
		case ok := <-done:
			if ok {
				ret := ck.ProcessPutAppendRsp(index, reply)
				if ret {
					DPrintf("client %d putappend to %d success,key = %s,value = %s,seq=%d",
					ck.Id,index,key,value,seqNum)
					return
				}
			} else {
				DPrintf("client %d call putappend %d fail", ck.Id, index)
			}
			ck.lastLeader = -1
			index = (index + 1) % n
		}
	}
	//DPrintf("Put End")
	// You will have to modify this function.
}
func (ck *Clerk) sendPutAppend(index int, args *PutAppendArgs,
	reply *PutAppendReply) bool {
	ok := ck.servers[index].Call("KVServer.PutAppend", args, reply)
	return ok
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
