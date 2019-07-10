package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"
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
	servers []*labrpc.ClientEnd
	id 		 int64
	seqNum 	 int
	mu       sync.Mutex
	// Your data here.
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
	ck.id = ck.GetClientId()
	ck.seqNum = 1
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.id
	args.SeqNum = ck.seqNum
	DPrintf("client %d Query args=%+v",ck.id,args)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			DPrintf("client %d Query reply=%+v",ck.id,reply)
			if ok && reply.WrongLeader == false {
				if reply.Err == "" {
					ck.seqNum++
					DPrintf("client %d Query finish",ck.id)
					return reply.Config
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.id
	args.SeqNum = ck.seqNum
	DPrintf("client %d Join args=%+v",ck.id,args)

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			DPrintf("client %d Join reply=%+v",ck.id,reply)
			if ok && reply.WrongLeader == false {
				if reply.Err == "" {
					ck.seqNum++
					return
				}	
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.id
	args.SeqNum = ck.seqNum
	DPrintf("client %d Leave args=%+v",ck.id,args)

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			DPrintf("client %d Leave reply=%+v",ck.id,reply)
			if ok && reply.WrongLeader == false {
				if reply.Err == "" {
					ck.seqNum++
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.id
	args.SeqNum = ck.seqNum
	DPrintf("client %d Move args=%+v",ck.id,args)

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			DPrintf("client %d Move reply=%+v",ck.id,reply)
			if ok && reply.WrongLeader == false {
				if reply.Err == "" {
					ck.seqNum++
					return
				}	
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
