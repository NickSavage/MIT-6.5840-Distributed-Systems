package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KeyValue struct {
	Key   string
	Value string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvs map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	log.Printf("server get %v", args)
	reply.Value = kv.kvs[args.Key]
	// Your code here.
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	log.Printf("server put %v", args)
	kv.kvs[args.Key] = args.Value
	// Your code here.
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Your code here.
	log.Printf("server append %v", args)

	result := kv.kvs[args.Key]
	kv.kvs[args.Key] = result + args.Value

	reply.Value = result
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	kv.kvs = make(map[string]string)
	// You may need initialization code here.

	return kv
}
