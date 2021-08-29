package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"fmt"
)

const (
    Debug = 0

    GET_OPCODE = 1
    PUT_OPCODE = 2
    APPEND_OPCODE = 3

    OPS_TOPIC = "operations"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
    Code int
    Key string
    Value string
}

type OpResult struct {
    Index int
    Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	store   map[string]string
	pubsub  *Pubsub

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
    fmt.Printf("%d server, received GET \n", kv.me)
    reply.Err = OK
    _, isLeader := kv.rf.GetState()
    if isLeader != true {
        reply.Err = ErrWrongLeader
        reply.Value = ""
        return
    }

    op := Op{Code: GET_OPCODE, Key: args.Key, Value: ""}
    opIndex, _, _ := kv.rf.Start(op)
    fmt.Printf("%d server, Get, waiting for index: %d \n", kv.me, opIndex)

    opsChan := kv.pubsub.Subscribe(OPS_TOPIC)

    for {
        msg := <- opsChan
        if msg.Index == opIndex {
            fmt.Printf("%d server, Get, finished index: %d \n", kv.me, opIndex)
            reply.Value = msg.Value
            kv.pubsub.Unsubscribe(OPS_TOPIC, opsChan)
            return
        }
    }
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    fmt.Printf("%d server, received PutAppend\n", kv.me)
	reply.Err = OK
    _, isLeader := kv.rf.GetState()

    if isLeader != true {
        reply.Err = ErrWrongLeader
        return
    }

    op := Op{}
    op.Key = args.Key
    op.Value = args.Value

    if args.Op == PUT_OPERATION {
        op.Code = PUT_OPCODE
    } else if args.Op == APPEND_OPERATION {
        op.Code = APPEND_OPCODE
    }

    opIndex, _, _ := kv.rf.Start(op)
    fmt.Printf("%d server, PutAppend, waiting for index: %d\n", kv.me, opIndex)

    opsChan := kv.pubsub.Subscribe(OPS_TOPIC)

    for {
        msg := <- opsChan
        fmt.Println(msg)
        if msg.Index == opIndex {
            fmt.Printf("%d server, PutAppend, finished index: %d\n", kv.me, opIndex)
            kv.pubsub.Unsubscribe(OPS_TOPIC, opsChan)
            return
        }
    }
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	kv.pubsub.Close()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyOperations() {
    for !kv.killed() {
        msg := <- kv.applyCh
        opRes := OpResult{}
        opRes.Index = msg.CommandIndex

        op := msg.Command.(Op)

        if op.Code == PUT_OPCODE {
            fmt.Printf("%d server, apply PUT, index: %d \n", kv.me, opRes.Index)
            kv.store[op.Key] = op.Value
            opRes.Value = ""
        } else if op.Code == APPEND_OPCODE {
            fmt.Printf("%d server, apply APPEND, index: %d \n", kv.me, opRes.Index)
            kv.store[op.Key] += op.Value
            opRes.Value = ""
        } else if op.Code == GET_OPCODE {
            fmt.Printf("%d server, apply GET, index: %d \n", kv.me, opRes.Index)
            opRes.Value = kv.store[op.Key]
        }
        kv.pubsub.Publish(OPS_TOPIC, opRes)
    }
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
	kv.pubsub = NewPubsub()

	kv.store = make(map[string]string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyOperations()

	return kv
}
