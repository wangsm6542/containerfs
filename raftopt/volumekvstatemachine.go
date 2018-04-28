// Copyright (c) 2017, TIG All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

package raftopt

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	pbproto "github.com/golang/protobuf/proto"
	"github.com/google/btree"
	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/proto/kvp"
	"github.com/tiglabs/containerfs/raftopt/btreeinstance"
	"github.com/tiglabs/containerfs/raftopt/common"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage/wal"
)

//VolumeKvStateMachine is a wrapper of btree based kv state maachine
type VolumeKvStateMachine struct {
	id      uint64
	applied uint64
	raft    *raft.RaftServer

	dentryItem btreeinstance.DentryKV
	inodeItem  btreeinstance.InodeKV
	bgItem     btreeinstance.BGKV

	dentryDataLocker sync.RWMutex
	dentryData       *btree.BTree

	inodeDataLocker sync.RWMutex
	inodeData       *btree.BTree

	blockGroupDataLocker sync.RWMutex
	blockGroupData       *btree.BTree

	chunkIDLocker sync.Mutex
	chunkID       uint64

	inodeIDLocker sync.Mutex
	inodeID       uint64
}

//newVolumeKvStateMachine creates a new VolumeKvStateMachine instance
func newVolumeKvStateMachine(id uint64, raft *raft.RaftServer) *VolumeKvStateMachine {
	return &VolumeKvStateMachine{
		id:             id,
		raft:           raft,
		dentryData:     btree.New(32),
		inodeData:      btree.New(32),
		blockGroupData: btree.New(8),
	}
}

//CreateVolumeKvStateMachine is exported out for initialization of volume kv statemachine
func CreateVolumeKvStateMachine(rs *raft.RaftServer, peers []proto.Peer, nodeID uint64, dir string, UUID string, raftGroupID uint64) (*VolumeKvStateMachine, *wal.Storage, error) {
	wc := &wal.Config{}
	raftStroage, err := wal.NewStorage(path.Join(dir, UUID, "wal"), wc)
	if err != nil {
		logger.Error("new raft log stroage error: %v", err)
		return nil, nil, err
	}

	// state machine
	kvsm := newVolumeKvStateMachine(nodeID, rs)

	var index uint64

	index, err = LoadVolumeKvSnapShot(kvsm, path.Join(dir, UUID, "wal", "snap"))
	if err != nil {
		return nil, nil, err
	}

	logger.Debug("CreateVolumeKvStateMachine Success index : %v", index)

	rc := &raft.RaftConfig{
		ID:           raftGroupID,
		Peers:        peers,
		Storage:      raftStroage,
		StateMachine: kvsm,
		Applied:      index,
	}
	err = rs.CreateRaft(rc)
	if err != nil {
		logger.Error("creat raft failed. %v", err)
	}

	logger.Debug("CreateRaft Success index : %v", rc.Applied)

	return kvsm, raftStroage, nil

}

//Apply implements raftgroup interafce
func (ms *VolumeKvStateMachine) Apply(data []byte, index uint64) (interface{}, error) {

	kv := &kvp.Kv{}
	err := pbproto.Unmarshal(data, kv)
	if err != nil {
		return nil, err
	}

	switch kv.Opt {
	case OPT_ALLOCATE_INODEID: // allockInodeID
		atomic.AddUint64(&ms.inodeID, 1)

	case OPT_ALLOCATE_CHUNKID: // allockChunkID
		atomic.AddUint64(&ms.chunkID, 1)

	case OPT_SET_DENTRY: // set dentryData
		ms.dentryItem.K = kv.K
		ms.dentryItem.V = kv.V
		ms.dentryDataLocker.Lock()
		ms.dentryData.ReplaceOrInsert(ms.dentryItem)
		ms.dentryDataLocker.Unlock()

	case OPT_DEL_DENTRY: // del dentryData
		ms.dentryItem.K = kv.K
		ms.dentryDataLocker.Lock()
		ms.dentryData.Delete(ms.dentryItem)
		ms.dentryDataLocker.Unlock()

	case OPT_SET_INODE: // set inodeData
		key, _ := strconv.ParseUint(kv.K, 10, 64)
		ms.inodeItem.K = key
		ms.inodeItem.V = kv.V
		ms.inodeDataLocker.Lock()
		ms.inodeData.ReplaceOrInsert(ms.inodeItem)
		ms.inodeDataLocker.Unlock()

	case OPT_DEL_INODE: // del inodeData
		key, _ := strconv.ParseUint(kv.K, 10, 64)
		ms.inodeItem.K = key
		ms.inodeDataLocker.Lock()
		ms.inodeData.Delete(ms.inodeItem)
		ms.inodeDataLocker.Unlock()

	case OPT_SET_BG: // set OPT_SET_BG
		key, _ := strconv.ParseUint(kv.K, 10, 64)
		ms.bgItem.K = key
		ms.bgItem.V = kv.V
		ms.blockGroupDataLocker.Lock()
		ms.blockGroupData.ReplaceOrInsert(ms.bgItem)
		ms.blockGroupDataLocker.Unlock()
	}

	ms.applied = index
	return nil, nil
}

//DentryGet gets dentry item
func (ms *VolumeKvStateMachine) DentryGet(raftGroupID uint64, key string) ([]byte, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return nil, errors.New("not leader")
	}

	var item btreeinstance.DentryKV
	item.K = key
	ms.dentryDataLocker.RLock()
	newItem := ms.dentryData.Get(item)
	ms.dentryDataLocker.RUnlock()

	if newItem != nil {
		return newItem.(btreeinstance.DentryKV).V, nil
	}
	return []byte{}, ErrKeyNotFound

}

//DentryGetRange gets dentry items ranging from minKey to maxKey
func (ms *VolumeKvStateMachine) DentryGetRange(raftGroupID uint64, minKey string, maxKey string) ([]btreeinstance.DentryKV, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return nil, errors.New("not leader")
	}

	var v []btreeinstance.DentryKV

	var itemMin btreeinstance.DentryKV
	itemMin.K = minKey

	var itemMax btreeinstance.DentryKV
	itemMax.K = maxKey

	ms.dentryDataLocker.RLock()
	ms.dentryData.AscendRange(itemMin, itemMax, func(a btree.Item) bool {
		v = append(v, a.(btreeinstance.DentryKV))
		return true
	})
	ms.dentryDataLocker.RUnlock()

	return v, nil
}

//DentrySet sets kv pair into sm
func (ms *VolumeKvStateMachine) DentrySet(raftGroupID uint64, key string, value []byte) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}
	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_SET_DENTRY, K: key, V: value}

	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		return fmt.Errorf("Put error[%v]", err)
	}
	return nil

}

//DentryDel removes kv pair by key
func (ms *VolumeKvStateMachine) DentryDel(raftGroupID uint64, key string) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}
	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_DEL_DENTRY, K: key}

	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		return fmt.Errorf("Del error[%v]", err)
	}
	return nil

}

//InodeGet gets inode item by key
func (ms *VolumeKvStateMachine) InodeGet(raftGroupID uint64, key uint64) ([]byte, error) {

	if !ms.raft.IsLeader(raftGroupID) {
		return nil, errors.New("not leader")
	}

	var item btreeinstance.InodeKV
	item.K = key
	ms.inodeDataLocker.RLock()
	newItem := ms.inodeData.Get(item)
	ms.inodeDataLocker.RUnlock()

	if newItem != nil {
		return newItem.(btreeinstance.InodeKV).V, nil
	}
	return []byte{}, ErrKeyNotFound

}

//InodeSet sets inode kv into sm
func (ms *VolumeKvStateMachine) InodeSet(raftGroupID uint64, key uint64, value []byte) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}

	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_SET_INODE, K: strconv.FormatUint(key, 10), V: value}

	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		return fmt.Errorf("Put error[%v]", err)
	}
	return nil

}

//InodeDel removes kv pair by key
func (ms *VolumeKvStateMachine) InodeDel(raftGroupID uint64, key uint64) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}

	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_DEL_INODE, K: strconv.FormatUint(key, 10)}

	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		return fmt.Errorf("Del error[%v]", err)
	}
	return nil

}

//BGGet gets BG item by key
func (ms *VolumeKvStateMachine) BGGet(raftGroupID uint64, key uint64) ([]byte, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return nil, errors.New("not leader")
	}

	var item btreeinstance.BGKV
	item.K = key
	ms.blockGroupDataLocker.RLock()
	newItem := ms.blockGroupData.Get(item)
	ms.blockGroupDataLocker.RUnlock()

	if newItem != nil {
		return newItem.(btreeinstance.BGKV).V, nil
	}
	return []byte{}, errNotExists

}

//BGSet sets BG kv into sm
func (ms *VolumeKvStateMachine) BGSet(raftGroupID uint64, key uint64, value []byte) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}

	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_SET_BG, K: strconv.FormatUint(key, 10), V: value}

	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		return fmt.Errorf("Put error[%v]", err)
	}
	return nil

}

//BGGetAll gets all BG items in sm
func (ms *VolumeKvStateMachine) BGGetAll(raftGroupID uint64) ([]btreeinstance.BGKV, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return nil, errors.New("not leader")
	}

	var v []btreeinstance.BGKV

	ms.blockGroupData.Ascend(func(a btree.Item) bool {
		v = append(v, a.(btreeinstance.BGKV))
		return true
	})

	return v, nil
}

//ChunkIDGET generates a new global unique chunk id
func (ms *VolumeKvStateMachine) ChunkIDGET(raftGroupID uint64) (uint64, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return 0, errors.New("not leader")
	}

	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_ALLOCATE_CHUNKID}

	if data, err = pbproto.Marshal(kv); err != nil {
		return 0, err
	}

	ms.chunkIDLocker.Lock()
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		ms.chunkIDLocker.Unlock()
		return 0, fmt.Errorf("Put error[%v]", err)
	}
	chunkID := ms.chunkID
	ms.chunkIDLocker.Unlock()

	return chunkID, nil
}

//InodeIDGET generates a new global unique inode id
func (ms *VolumeKvStateMachine) InodeIDGET(raftGroupID uint64) (uint64, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return 0, errors.New("not leader")
	}

	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_ALLOCATE_INODEID}

	if data, err = pbproto.Marshal(kv); err != nil {
		return 0, err
	}

	ms.inodeIDLocker.Lock()
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		ms.inodeIDLocker.Unlock()
		return 0, fmt.Errorf("Put error[%v]", err)
	}
	inodeID := ms.inodeID
	ms.inodeIDLocker.Unlock()

	return inodeID, nil
}

//AddNode adds new raft peer
func (ms *VolumeKvStateMachine) AddNode(peer proto.Peer) error {
	resp := ms.raft.ChangeMember(1, proto.ConfAddNode, peer, nil)
	_, err := resp.Response()
	if err != nil {
		return errors.New("AddNode error")
	}
	return nil
}

//RemoveNode removes raft peer
func (ms *VolumeKvStateMachine) RemoveNode(peer proto.Peer) error {
	resp := ms.raft.ChangeMember(1, proto.ConfRemoveNode, peer, nil)
	_, err := resp.Response()
	if err != nil {
		return errors.New("RemoveNode error")
	}
	return nil
}

//ApplyMemberChange is not implemented yet
func (ms *VolumeKvStateMachine) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	return nil, nil
}

//HandleLeaderChange is not implemented yet
func (ms *VolumeKvStateMachine) HandleLeaderChange(leader uint64) {
}

//HandleFatalEvent is not implemented yet
func (ms *VolumeKvStateMachine) HandleFatalEvent(err *raft.FatalError) {
	panic(err.Err)
}

//setApplied set current applied id
func (ms *VolumeKvStateMachine) setApplied(index uint64) {
	ms.applied = index
}

// Snapshot takes snapshot of state machine in mem
func (ms *VolumeKvStateMachine) Snapshot() (proto.Snapshot, error) {

	return &volumeKvSnapshot{
		applied:       ms.applied,
		chunkID:       ms.chunkID,
		inodeID:       ms.inodeID,
		curDentryItem: ms.dentryData.Min(),
		curInodeItem:  ms.inodeData.Min(),
		curBgItem:     ms.blockGroupData.Min(),
		bgTree:        ms.blockGroupData,
		dentryTree:    ms.dentryData,
		inodeTree:     ms.inodeData,
	}, nil

}

//ApplySnapshot applies apply by snapshot data
func (ms *VolumeKvStateMachine) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error {

	var (
		err      error
		data     []byte
		uint64ID uint64
	)

	kv := &kvp.Kv{}

	for err == nil {
		if data, err = iter.Next(); len(data) > 0 {

			err := pbproto.Unmarshal(data, kv)
			if err != nil {
				return err
			}

			logger.Debug("---------- ApplySnapshot : %v", kv)

			switch kv.Opt {
			case OPT_APPLIED:
				uint64ID, _ = strconv.ParseUint(kv.K, 10, 64)
				ms.applied = uint64ID
			case OPT_ALLOCATE_INODEID:
				uint64ID, _ = strconv.ParseUint(kv.K, 10, 64)
				ms.inodeID = uint64ID
			case OPT_ALLOCATE_CHUNKID:
				uint64ID, _ = strconv.ParseUint(kv.K, 10, 64)
				ms.chunkID = uint64ID
			case OPT_SET_DENTRY: // set dentryData
				ms.dentryItem.K = kv.K
				ms.dentryItem.V = kv.V
				ms.dentryData.ReplaceOrInsert(ms.dentryItem)
			case OPT_SET_INODE: // set inodeData
				uint64ID, _ = strconv.ParseUint(kv.K, 10, 64)
				ms.inodeItem.K = uint64ID
				ms.inodeItem.V = kv.V
				ms.inodeData.ReplaceOrInsert(ms.inodeItem)
			case OPT_SET_BG:
				uint64ID, _ = strconv.ParseUint(kv.K, 10, 64)
				ms.bgItem.K = uint64ID
				ms.bgItem.V = kv.V
				ms.blockGroupData.ReplaceOrInsert(ms.bgItem)
			}

		}
	}
	if err != nil && err != io.EOF {
		return err
	}

	return nil
}

//volumeKvSnapshot is utility tool for taking snapshot
type volumeKvSnapshot struct {
	applied     uint64
	appliedFlag bool

	chunkID     uint64
	chunkIDFlag bool

	inodeID     uint64
	inodeIDFlag bool

	curBtree string

	curBgItem     btree.Item
	curInodeItem  btree.Item
	curDentryItem btree.Item

	dentryTree *btree.BTree
	inodeTree  *btree.BTree
	bgTree     *btree.BTree
}

//Next implements Next interface
func (s *volumeKvSnapshot) Next() ([]byte, error) {

	if s.appliedFlag && s.chunkIDFlag && s.inodeIDFlag && s.curBtree == "done" {
		return nil, io.EOF
	}

	var data []byte
	defer tmplog(string(data))

	var err error
	kv := &kvp.Kv{}

	if !s.appliedFlag {

		kv.Opt = OPT_APPLIED
		kv.K = strconv.FormatUint(s.applied, 10)

		if data, err = pbproto.Marshal(kv); err != nil {
			return nil, err
		}

		s.appliedFlag = true
		return data, nil
	}
	if !s.chunkIDFlag {

		kv.Opt = OPT_ALLOCATE_CHUNKID
		kv.K = strconv.FormatUint(s.chunkID, 10)

		if data, err = pbproto.Marshal(kv); err != nil {
			return nil, err
		}

		s.chunkIDFlag = true
		return data, nil
	}
	if !s.inodeIDFlag {

		kv.Opt = OPT_ALLOCATE_INODEID
		kv.K = strconv.FormatUint(s.inodeID, 10)

		if data, err = pbproto.Marshal(kv); err != nil {
			return nil, err
		}

		s.inodeIDFlag = true
		return data, nil
	}
	if s.curBtree == "" {
		s.curBtree = "bg"
	}

	if s.curBtree == "bg" {

		kv.Opt = OPT_SET_BG

		s.bgTree.AscendGreaterOrEqual(s.curBgItem, func(a btree.Item) bool {
			if a.(btreeinstance.BGKV).K > s.curBgItem.(btreeinstance.BGKV).K {
				kv.K = strconv.Itoa(int(a.(btreeinstance.BGKV).K))
				kv.V = a.(btreeinstance.BGKV).V
				s.curBgItem = a
				return false
			}
			kv.K = strconv.Itoa(int(a.(btreeinstance.BGKV).K))
			kv.V = a.(btreeinstance.BGKV).V
			return true
		})

		if s.curBgItem.(btreeinstance.BGKV).K == s.bgTree.Max().(btreeinstance.BGKV).K {
			s.curBtree = "inode"
		}

		if data, err = pbproto.Marshal(kv); err != nil {
			return nil, err
		}

		return data, err
	}

	if s.curBtree == "inode" {

		kv.Opt = OPT_SET_INODE

		s.inodeTree.AscendGreaterOrEqual(s.curInodeItem, func(a btree.Item) bool {
			if a.(btreeinstance.InodeKV).K > s.curInodeItem.(btreeinstance.InodeKV).K {
				kv.K = strconv.FormatUint(a.(btreeinstance.InodeKV).K, 10)
				kv.V = a.(btreeinstance.InodeKV).V
				s.curInodeItem = a
				return false
			}
			kv.K = strconv.FormatUint(a.(btreeinstance.InodeKV).K, 10)
			kv.V = a.(btreeinstance.InodeKV).V
			return true
		})

		if s.curInodeItem.(btreeinstance.InodeKV).K == s.inodeTree.Max().(btreeinstance.InodeKV).K {
			s.curBtree = "dentry"
		}

		if data, err = pbproto.Marshal(kv); err != nil {
			return nil, err
		}

		return data, err
	}

	if s.curBtree == "dentry" {
		kv.Opt = OPT_SET_DENTRY

		s.dentryTree.AscendGreaterOrEqual(s.curDentryItem, func(a btree.Item) bool {
			if a.(btreeinstance.DentryKV).K > s.curDentryItem.(btreeinstance.DentryKV).K {
				kv.K = a.(btreeinstance.DentryKV).K
				kv.V = a.(btreeinstance.DentryKV).V
				s.curDentryItem = a
				return false
			}
			kv.K = a.(btreeinstance.DentryKV).K
			kv.V = a.(btreeinstance.DentryKV).V
			return true
		})

		if s.curDentryItem.(btreeinstance.DentryKV).K == s.dentryTree.Max().(btreeinstance.DentryKV).K {
			s.curBtree = "done"
		}

		if data, err = pbproto.Marshal(kv); err != nil {
			return nil, err
		}

		return data, err

	}
	return nil, io.ErrUnexpectedEOF
}

//ApplyIndex gets current apply id
func (s *volumeKvSnapshot) ApplyIndex() uint64 {
	return s.applied
}

//Close is just used for implementing interface
func (s *volumeKvSnapshot) Close() {
	return
}

//VolumeAddrDatabase is map from volmgr nodeis to host
var VolumeAddrDatabase = make(map[uint64]*common.Address)

//VolumeResolver implements address resolver interface
type VolumeResolver struct {
	nodes map[uint64]struct{}
	sync.Mutex
}

//NewVolumeResolver creates a new VolumeResolver
func NewVolumeResolver() *VolumeResolver {
	return &VolumeResolver{nodes: make(map[uint64]struct{})}
}

//AddNode adds new raft peer by nodeid and address
func (r *VolumeResolver) AddNode(nodeID uint64, addr *common.Address) {
	r.Lock()
	r.nodes[nodeID] = struct{}{}
	VolumeAddrDatabase[nodeID] = addr
	r.Unlock()
}

//RemoveNode removes raft peer by nodeid and address
func (r *VolumeResolver) RemoveNode(nodeID uint64, addr *common.Address) {
	r.Lock()
	delete(r.nodes, nodeID)
	delete(VolumeAddrDatabase, nodeID)
	r.Unlock()
}

//AllNodes gets all raft nodes
func (r *VolumeResolver) AllNodes() (all []uint64) {
	r.Lock()
	for k := range r.nodes {
		all = append(all, k)
	}
	r.Unlock()
	return
}

//NodeAddress  gets raft peer address by nodeid
func (r *VolumeResolver) NodeAddress(nodeID uint64, stype raft.SocketType) (addr string, err error) {
	raddr, ok := VolumeAddrDatabase[nodeID]
	if !ok {
		return "", errors.New("no such node")
	}
	switch stype {
	case raft.HeartBeat:
		return raddr.Heartbeat, nil
	case raft.Replicate:
		return raddr.Replicate, nil
	default:
		return "", errors.New("unknown socket type")
	}
}

//TakeVolumeKvSnapShot takes snapshot of kv statemachine in mem
func TakeVolumeKvSnapShot(ms *VolumeKvStateMachine, rsg *wal.Storage, path string) error {

	_, err := os.Stat(path)
	if err == nil {
		t := time.Now()
		os.Rename(path, path+"-backup"+"-"+t.String())
	}
	os.MkdirAll(path, 0777)

	f, err := os.OpenFile(path+"/applied", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		f.Close()
		return err
	}
	w := bufio.NewWriter(f)

	w.Write([]byte(strconv.FormatUint(ms.applied, 10)))
	w.Flush()
	f.Close()

	f, err = os.OpenFile(path+"/chunkid", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		f.Close()
		return err
	}
	w = bufio.NewWriter(f)

	w.Write([]byte(strconv.FormatUint(ms.chunkID, 10)))
	w.Flush()
	f.Close()

	f, err = os.OpenFile(path+"/inodeid", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		f.Close()
		return err
	}
	w = bufio.NewWriter(f)

	w.Write([]byte(strconv.FormatUint(ms.inodeID, 10)))
	w.Flush()
	f.Close()

	f, err = os.OpenFile(path+"/dentrydata", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		f.Close()
		return err
	}
	w = bufio.NewWriter(f)

	var data []byte

	ms.dentryData.Ascend(func(a btree.Item) bool {

		if data, err = json.Marshal(a.(btreeinstance.DentryKV)); err != nil {
			return false
		}

		w.Write(data)
		w.Write([]byte("\n"))
		w.Flush()
		return true
	})

	f.Close()

	f, err = os.OpenFile(path+"/inodedata", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		f.Close()
		return err
	}
	w = bufio.NewWriter(f)

	ms.inodeData.Ascend(func(a btree.Item) bool {

		if data, err = json.Marshal(a.(btreeinstance.InodeKV)); err != nil {
			return false
		}

		w.Write(data)
		w.Write([]byte("\n"))
		w.Flush()
		return true
	})

	f.Close()

	f, err = os.OpenFile(path+"/bgdata", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		f.Close()
		return err
	}
	w = bufio.NewWriter(f)

	ms.blockGroupData.Ascend(func(a btree.Item) bool {

		if data, err = json.Marshal(a.(btreeinstance.BGKV)); err != nil {
			return false
		}

		w.Write(data)
		w.WriteByte('\n')
		w.Flush()
		return true
	})

	f.Close()
	err = rsg.Truncate(ms.applied)
	if err != nil {
		logger.Error("TakeKvSnapShoot Truncate failed index : %v , err :%v", ms.applied, err)
		return err
	}
	logger.Error("TakeKvSnapShoot Truncate Success index : %v ", ms.applied)

	return nil
}

//LoadVolumeKvSnapShot  loads kv state machine from disk data
func LoadVolumeKvSnapShot(ms *VolumeKvStateMachine, path string) (uint64, error) {
	fi, err := os.Open(path + "/applied")
	if err != nil {
		return 0, nil
	}
	data, err := ioutil.ReadAll(fi)
	ms.applied, err = strconv.ParseUint(string(data), 10, 64)
	logger.Debug("ms.applied %v", ms.applied)
	fi.Close()

	fi, err = os.Open(path + "/chunkid")
	if err != nil {
		return 0, nil
	}
	data, err = ioutil.ReadAll(fi)
	ms.chunkID, err = strconv.ParseUint(string(data), 10, 64)
	logger.Debug("ms.chunkID %v", ms.chunkID)
	fi.Close()

	fi, err = os.Open(path + "/inodeid")
	if err != nil {
		return 0, nil
	}
	data, err = ioutil.ReadAll(fi)
	ms.inodeID, err = strconv.ParseUint(string(data), 10, 64)
	logger.Debug("ms.inodeID %v", ms.inodeID)
	fi.Close()

	fi, err = os.Open(path + "/dentrydata")
	if err != nil {
		return 0, err
	}
	buf := bufio.NewReader(fi)
	var dentry btreeinstance.DentryKV

	for {
		line, err := buf.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fi.Close()
			return 0, err
		}
		err = json.Unmarshal(line, &dentry)
		if err != nil {
			fi.Close()
			return 0, err
		}
		logger.Debug("dentry %v", dentry)

		ms.dentryData.ReplaceOrInsert(dentry)
	}
	fi.Close()

	fi, err = os.Open(path + "/inodedata")
	if err != nil {
		return 0, err
	}
	buf = bufio.NewReader(fi)
	var inode btreeinstance.InodeKV

	for {
		line, err := buf.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fi.Close()
			return 0, err
		}
		err = json.Unmarshal(line, &inode)
		if err != nil {
			fi.Close()
			return 0, err
		}
		logger.Debug("inode %v", inode)

		ms.inodeData.ReplaceOrInsert(inode)
	}
	fi.Close()

	fi, err = os.Open(path + "/bgdata")
	if err != nil {
		return 0, err
	}
	buf = bufio.NewReader(fi)
	var bg btreeinstance.BGKV

	for {
		line, err := buf.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fi.Close()
			return 0, err
		}
		err = json.Unmarshal(line, &bg)
		if err != nil {
			fi.Close()
			return 0, err
		}
		logger.Debug("bg %v", bg)

		ms.blockGroupData.ReplaceOrInsert(bg)
	}
	fi.Close()

	logger.Debug("ms.applied end %v", ms.applied)

	return ms.applied, nil

}
