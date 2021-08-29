package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "fmt"
import "time"

const (
    PUT_OPERATION = "Put"
    APPEND_OPERATION = "Append"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	currentLeader *labrpc.ClientEnd
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
func (ck *Clerk) Get(key string) string {
    args := GetArgs{}
    reply := GetReply{}

    args.Key = key

    // we might know current leader
    if ck.currentLeader != nil {
        ok := ck.currentLeader.Call("KVServer.Get", &args, &reply)
        if ok && reply.Err == OK {
            return reply.Value
        }

        if ok && reply.Err == ErrWrongLeader {
            fmt.Println("Leader changed")
            ck.currentLeader = nil
        }
    }

    for ck.currentLeader == nil {
        for _, server := range ck.servers {
            ok := server.Call("KVServer.Get", &args, &reply)
            if ok && reply.Err == OK {
                ck.currentLeader = server
                return reply.Value
            }
        }
        time.Sleep(200 * time.Millisecond)
    }
    return ""
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
    args := PutAppendArgs{}
    reply := PutAppendReply{}

    args.Key = key
    args.Value = value
    args.Op = op

    // we might know current leader
    if ck.currentLeader != nil {
        ok := ck.currentLeader.Call("KVServer.PutAppend", &args, &reply)
        if ok && reply.Err == OK {
            return
        }

        if ok && reply.Err == ErrWrongLeader {
            fmt.Println("Leader changed")
            ck.currentLeader = nil
        }
    }

    for ck.currentLeader == nil {
        for _, server := range ck.servers {
            ok := server.Call("KVServer.PutAppend", &args, &reply)
            if ok && reply.Err == OK {
                ck.currentLeader = server
                return
            }
        }
        time.Sleep(200 * time.Millisecond)
    }
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT_OPERATION)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND_OPERATION)
}
