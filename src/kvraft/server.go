package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"fmt"
	"sync"
	"sync/atomic"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key, Value string
	OpType     string
	BaseReq
}

func (receiver Op) String() string {
	return fmt.Sprintf("key: %v value: %v opType: %v", receiver.Key, receiver.Value, receiver.OpType)
}

type ResultInfo struct {
	SeqNum int64
	OpReply
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg // 1
	dead    int32              // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	Store                            // 2
	Response  map[int]chan ApplyResp // 3
	ApplyCond *sync.Cond             //4
	// clientId : seq
	IsExec map[int]int64 // 5
	// clientId: result
	Result map[int][]ResultInfo // 6
}

func (kv *KVServer) Name() string {
	return fmt.Sprintf("KV-%d", kv.me)
}

func (kv *KVServer) Lock() {
	Debug(kv, dLock, "Lock")
	kv.mu.Lock()
}
func (kv *KVServer) UnLock() {
	Debug(kv, dLock, "UnLock")
	kv.mu.Unlock()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.

	kv.Store = newStore()
	kv.ApplyCond = sync.NewCond(&kv.mu)
	kv.Response = make(map[int]chan ApplyResp)
	kv.Result = map[int][]ResultInfo{}
	kv.IsExec = map[int]int64{}
	go kv.apply()

	// You may need initialization code here.

	return kv
}
