package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Id		int64
	SeqNum	int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

func NewPutAppendArgs(key string, value string, op string, 
	id int64, seqNum int64) *PutAppendArgs {
	return &PutAppendArgs{
		Key:         key,
		Value:  value,
		Op: op,
		Id:  id,
		SeqNum:  seqNum,
	}
}

type PutAppendReply struct {
	WrongLeader bool
	Id			int64
	Err         Err
}

func NewPutAppendReply() *PutAppendReply {
	return &PutAppendReply{}
}

type GetArgs struct {
	Key string
	SeqNum int64
	Id	int64
	// You'll have to add definitions here.
}

func NewGetArgs(key string,id int64, seqNum int64) *GetArgs{
	return &GetArgs{
		Key: 	key,
		Id:	    id,
		SeqNum: seqNum,
	}
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
	Id 			int64
}

func NewGetReply() *GetReply{
	return &GetReply{}
}
