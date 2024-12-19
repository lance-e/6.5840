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

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	database  map[string]string
	clientMap map[int64]map[string]string //the old version k-v per client
	clientId  map[int64]int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.database[args.Key]
}

func (kv *KVServer) CheckId(clientId int64, id int64) bool {
	curId := kv.clientId[clientId]
	if curId >= id {
		return false
	}
	kv.clientId[clientId] = id
	return true
}

func (kv *KVServer) SetHistory(clientId int64, key string, value string) {
	val := kv.clientMap[clientId]
	if val == nil {
		kv.clientMap[clientId] = make(map[string]string)
	}
	kv.clientMap[clientId][key] = value
}

func (kv *KVServer) ClearHistory(clientId int64, key string) {
	delete(kv.clientMap[clientId], key)
	if len(kv.clientMap[clientId]) == 0 {
		kv.clientMap[clientId] = make(map[string]string)
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Over {
		kv.ClearHistory(args.CId, args.Key)
		return
	}
	if kv.CheckId(args.CId, args.Id) {
		kv.database[args.Key] = args.Value
		kv.SetHistory(args.CId, args.Key, args.Value)
	}
	reply.Value = kv.clientMap[args.CId][args.Key]
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Over {
		kv.ClearHistory(args.CId, args.Key)
		return
	}
	if kv.CheckId(args.CId, args.Id) {
		kv.SetHistory(args.CId, args.Key, kv.database[args.Key])
		kv.database[args.Key] += args.Value
	}
	reply.Value = kv.clientMap[args.CId][args.Key]
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.database = make(map[string]string)
	kv.clientId = make(map[int64]int64)
	kv.clientMap = make(map[int64]map[string]string)

	return kv
}
