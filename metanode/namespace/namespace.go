// Copyright (c) 2017, TIG All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

package namespace

import (
	"encoding/binary"
	"math/rand"
	"path"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	pbproto "github.com/golang/protobuf/proto"
	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/proto/mp"
	"github.com/tiglabs/containerfs/proto/vp"
	"github.com/tiglabs/containerfs/raftopt"
	"github.com/tiglabs/containerfs/utils"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage/wal"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// VolMgrConn ...
var VolMgrConn *grpc.ClientConn

// VolMgrLeader ...
var VolMgrLeader string

// VolMgrHosts ...
var VolMgrHosts []string

// VolMgrInit ...
var VolMgrInit bool

// nameSpace ..
type nameSpace struct {
	sync.RWMutex
	VolID       string
	RaftGroupID uint64
	RaftGroup   *raftopt.VolumeKvStateMachine
	RaftStorage *wal.Storage
}

//AllNameSpace ...
var (
	AllNameSpace = make(map[string]*nameSpace)
)
var gMutex sync.RWMutex

func init() {
	ticker := time.NewTicker(time.Millisecond * 500)
	go func() {
		for range ticker.C {
			if !VolMgrInit {
				continue
			}
			leader, err := utils.GetVolMgrLeader(VolMgrHosts)
			if err != nil {
				VolMgrLeader = ""
				if VolMgrConn != nil {
					VolMgrConn.Close()
				}
				VolMgrConn = nil

				logger.Error("Leader Timer : Get VolMgr leader failed ")
				continue
			}
			if leader != VolMgrLeader {
				logger.Error("VolMgr Leader Change ! Old Leader %v , New Leader %v", VolMgrLeader, leader)
				_, conn, err := utils.DialVolMgr(VolMgrHosts)
				if conn == nil || err != nil {
					logger.Error("Leader Timer : DialVolMgr failed ")
					continue
				}

				VolMgrLeader = leader
				if VolMgrConn != nil {
					VolMgrConn.Close()
					VolMgrConn = nil
				}
				VolMgrConn = conn

			}

			logger.Debug("--- VolMgr Leader :%v ---", VolMgrLeader)
		}
	}()

}

func catchPanic() {
	if err := recover(); err != nil {
		logger.Error("panic !!! :%v", err)
		logger.Error("stacks:%v", string(debug.Stack()))
	}
}

// CreateGNameSpace ...
func CreateGNameSpace() {
	gMutex.Lock()
	AllNameSpace = make(map[string]*nameSpace)
	gMutex.Unlock()
}

// createRaftGroup ...
func createRaftGroup(rs *raft.RaftServer, peers []proto.Peer, nodeID uint64, dir string, UUID string, raftGroupID uint64) (*raftopt.VolumeKvStateMachine, *wal.Storage, error) {
	sm, sg, err := raftopt.CreateVolumeKvStateMachine(rs, peers, nodeID, dir, UUID, raftGroupID)
	if err != nil {
		return nil, nil, err
	}
	return sm, sg, nil
}

// initNameSpace ...
func initNameSpace(rs *raft.RaftServer, nameSpace *nameSpace, UUID string, bgs []*mp.BlockGroup) int32 {

	defer catchPanic()
	logger.Debug("======= Begin initNameSpace for volume:%v raftgroupID:%v ", UUID, nameSpace.RaftGroupID)

	var err error

	//wait til raftgroup election to finish
	time.Sleep(time.Second * 2)

	var flag bool
	for i := 0; i < 3; i++ {
		if rs.IsLeader(nameSpace.RaftGroupID) {
			flag = true
			break
		}
		time.Sleep(time.Second)
	}
	if !flag {
		return 0
	}
	for _, v := range bgs {

		err = nameSpace.blockGroupDBSet(v.BlockGroupID, v)
		if err != nil {
			logger.Error("Set BlockGroup ID:%v Info:%v failed, err:%v", v.BlockGroupID, v, err)
			continue
		}
	}

	tmpInodeInfo := mp.InodeInfo{
		AccessTime: time.Now().Unix(),
		ModifiTime: time.Now().Unix(),
	}

	err = nameSpace.inodeDBSet(0, &tmpInodeInfo)
	if err != nil {
		logger.Error("Set Inode Info:%v failed, err:%v", tmpInodeInfo, err)
		return 1
	}
	logger.Debug("Set Volume:%v raftgroupid:%v  --- InodeInfo:%v to this volume leader MetaNode Success", UUID, nameSpace.RaftGroupID, tmpInodeInfo)
	return 0
}

// CreateNameSpace ...
func CreateNameSpace(rs *raft.RaftServer, peers []proto.Peer, nodeID uint64, dir string, UUID string, raftGroupID uint64, bgs []*mp.BlockGroup, IsLoad bool) int32 {

	defer catchPanic()

	var err error
	var errno int32

	nameSpace := nameSpace{}
	nameSpace.VolID = UUID
	nameSpace.RaftGroupID = raftGroupID
	nameSpace.RaftGroup, nameSpace.RaftStorage, err = createRaftGroup(rs, peers, nodeID, dir, UUID, nameSpace.RaftGroupID)
	if err != nil {
		logger.Error("createRaftGroup, failed,err:%v", err)
		errno = -1
		return errno
	}

	logger.Info("createRaftGroup:%v for volume:%v success", nameSpace.RaftGroupID, UUID)

	gMutex.Lock()
	AllNameSpace[UUID] = &nameSpace
	gMutex.Unlock()

	if !IsLoad {
		go initNameSpace(rs, &nameSpace, UUID, bgs)
	}

	return errno
}

// SnapShotNameSpace ...
func SnapShotNameSpace(rs *raft.RaftServer, UUID string, dir string) int32 {

	defer catchPanic()

	ret, nameSpace := GetNameSpace(UUID)
	if ret != 0 {
		return 0
	}

	raftopt.TakeVolumeKvSnapShot(nameSpace.RaftGroup, nameSpace.RaftStorage, path.Join(dir, UUID, "wal", "snap"))
	return 0
}

// DeleteNameSpace ...
func DeleteNameSpace(rs *raft.RaftServer, UUID string) int32 {

	defer catchPanic()

	ret, nameSpace := GetNameSpace(UUID)
	if ret != 0 {
		return 0
	}
	rs.RemoveRaft(nameSpace.RaftGroupID)

	gMutex.Lock()
	delete(AllNameSpace, UUID)
	gMutex.Unlock()
	return 0
}

// GetNameSpace ...
func GetNameSpace(UUID string) (int32, *nameSpace) {

	defer catchPanic()

	gMutex.RLock()

	if v, ok := AllNameSpace[UUID]; ok {
		gMutex.RUnlock()
		return 0, v
	}
	gMutex.RUnlock()
	return -1, nil
}

// GetFSInfo ...
func (ns *nameSpace) GetFSInfo(volID string) mp.GetFSInfoAck {
	ns.RLock()
	defer ns.RUnlock()
	defer catchPanic()

	ack := mp.GetFSInfoAck{}
	var totalSpace uint64
	var freeSpace uint64

	ret, bgs := ns.blockGroupDBGetAll()
	if !ret {
		return ack
	}

	for _, v := range bgs {
		totalSpace = totalSpace + uint64(utils.BlockGroupSize)
		freeSpace = freeSpace + uint64(v.FreeSize)
	}

	ack.TotalSpace = totalSpace
	ack.FreeSpace = freeSpace
	ack.Ret = 0

	return ack
}

// GetFSInfo ...
func (ns *nameSpace) ExpandNameSpace(blockGroups []*mp.BlockGroup) int32 {
	ns.Lock()
	defer ns.Unlock()

	defer catchPanic()

	logger.Debug("ExpandNameSpace %v , blockgroups num %v", blockGroups, len(blockGroups))

	for _, v := range blockGroups {
		err := ns.blockGroupDBSet(v.BlockGroupID, v)
		if err != nil {
			return -1
		}
	}
	return 0
}

// CreateDirDirect ...
func (ns *nameSpace) CreateDirDirect(pinode uint64, name string) (int32, uint64) {

	//to check parent dir's existence

	if _, err := ns.inodeDBGet(pinode); err != nil {
		if err == raftopt.ErrKeyNotFound {

			logger.Error("CreateDirDirect ErrKeyNotFound , pinode %v name %v ", pinode, name)

			return utils.ENO_NOTEXIST, 0
		}
		return 2, 0
	}

	/*update inode info*/
	inodeID, err := ns.AllocateInodeID()
	if err != nil {
		return 2, 0
	}
	tmpInodeInfo := mp.InodeInfo{
		AccessTime: time.Now().Unix(),
		ModifiTime: time.Now().Unix(),
	}

	err = ns.inodeDBSet(inodeID, &tmpInodeInfo)
	if err != nil {
		return 1, 0
	}

	err = ns.dentryDBSet(pinode, name, utils.INODE_DIR, inodeID)
	if err != nil {
		ns.inodeDBDelete(inodeID)
		return 1, 0
	}

	return 0, inodeID
}

// GetSymLinkInfoDirect ...
func (ns *nameSpace) GetSymLinkInfoDirect(pinode uint64, name string) (int32, uint64) {

	gRet, dirent := ns.dentryDBGet(pinode, name)
	if gRet != 0 {
		return gRet, 0
	}

	return 0, dirent.Inode
}

// GetInodeInfoDirect ...
func (ns *nameSpace) GetInodeInfoDirect(pinode uint64, name string) (int32, *mp.InodeInfo, uint64) {

	var pInodeInfo *mp.InodeInfo

	gRet, dirent := ns.dentryDBGet(pinode, name)
	if gRet != 0 {
		return gRet, nil, 0
	}
	var err error
	if pInodeInfo, err = ns.inodeDBGet(dirent.Inode); err != nil {
		return 2, nil, 0
	}
	return 0, pInodeInfo, dirent.Inode
}

// StatDirect ...
func (ns *nameSpace) StatDirect(pinode uint64, name string) (uint32, uint64, int32) {

	gRet, dirent := ns.dentryDBGet(pinode, name)
	if gRet != 0 {
		return 0, 0, gRet
	}

	return dirent.InodeType, dirent.Inode, 0
}

// ListDirect ...
func (ns *nameSpace) ListDirect(pinode uint64) ([]*mp.DirentN, int32) {

	if _, err := ns.inodeDBGet(pinode); err != nil {
		if err == raftopt.ErrKeyNotFound {
			return nil, utils.ENO_NOTEXIST
		}
		return nil, 2
	}
	ret, v := ns.dentryGetRange(pinode)
	if ret {
		return v, 0
	}

	return nil, utils.ENO_NOENT
}

// DeleteDirDirect ...
func (ns *nameSpace) DeleteDirDirect(pinode uint64, name string) int32 {

	gRet, dirent := ns.dentryDBGet(pinode, name)
	if gRet != 0 {
		return 1
	}
	ns.inodeDBDelete(dirent.Inode)
	ns.dentryDBDelete(pinode, name)

	return 0
}

// RenameDirect ...
func (ns *nameSpace) RenameDirect(oldpinode uint64, oldName string, newpinode uint64, newName string) int32 {

	gRet, dirent := ns.dentryDBGet(oldpinode, oldName)
	if gRet != 0 {
		return gRet
	}

	err := ns.dentryDBSet(newpinode, newName, dirent.InodeType, dirent.Inode)
	if err != nil {
		return 1
	}
	err = ns.dentryDBDelete(oldpinode, oldName)
	if err != nil {
		ns.dentryDBDelete(newpinode, newName)
		return 1
	}
	return 0
}

// CreateFileDirect ...
func (ns *nameSpace) CreateFileDirect(pinode uint64, name string) (int32, uint64) {

	//to check parent dir's existence
	if _, err := ns.inodeDBGet(pinode); err != nil {
		if err == raftopt.ErrKeyNotFound {
			return utils.ENO_NOTEXIST, 0
		}
		return 2, 0
	}

	/*update inode info*/
	inodeID, err := ns.AllocateInodeID()
	if err != nil {
		return 1, 0
	}
	tmpInodeInfo := mp.InodeInfo{
		AccessTime: time.Now().Unix(),
		ModifiTime: time.Now().Unix(),
		Link:       1,
	}

	err = ns.inodeDBSet(inodeID, &tmpInodeInfo)
	if err != nil {
		return 1, 0
	}

	err = ns.dentryDBSet(pinode, name, utils.INODE_FILE, inodeID)
	if err != nil {
		ns.inodeDBDelete(inodeID)
		return 1, 0
	}

	return 0, inodeID
}

// DeleteFileDirect ...
func (ns *nameSpace) DeleteFileDirect(pinode uint64, name string) int32 {

	gRet, pDirent := ns.dentryDBGet(pinode, name)
	if gRet != 0 {
		return gRet
	}

	value, err := ns.RaftGroup.InodeGet(ns.RaftGroupID, pDirent.Inode)
	if err != nil {
		value, err = ns.RaftGroup.InodeGet(ns.RaftGroupID, pDirent.Inode)
		if err == raftopt.ErrKeyNotFound {
			return 0
		}
		return 4
	}
	inodeInfo := mp.InodeInfo{}
	err = pbproto.Unmarshal(value, &inodeInfo)
	if err != nil {
		return 5
	}

	if inodeInfo.Link == 1 {
		for _, v := range inodeInfo.Chunks {
			ns.releaseBlockGroup(v.BlockGroupID, v.ChunkSize)
		}
		ns.inodeDBDelete(pDirent.Inode)
	} else {
		inodeInfo.Link = inodeInfo.Link - 1
		ns.inodeDBSet(pDirent.Inode, &inodeInfo)
	}

	ns.dentryDBDelete(pinode, name)

	return 0
}

// DeleteSymLinkDirect ...
func (ns *nameSpace) DeleteSymLinkDirect(pinode uint64, name string) int32 {

	ns.dentryDBDelete(pinode, name)

	return 0
}

// GetFileChunksDirect ...
func (ns *nameSpace) GetFileChunksDirect(pinode uint64, name string) (int32, []*mp.ChunkInfo, uint64, uint32) {

	//to check parent dir's existence
	if _, err := ns.inodeDBGet(pinode); err != nil {
		if err == raftopt.ErrKeyNotFound {
			return utils.ENO_NOTEXIST, nil, 0, 0
		}
		return 2, nil, 0, 0
	}

	gRet, dirent := ns.dentryDBGet(pinode, name)
	if gRet != 0 {
		return gRet, nil, 0, 0
	}
	pInodeInfo, err := ns.inodeDBGet(dirent.Inode)
	if err != nil {
		return 1, nil, 0, 0
	}
	return 0, pInodeInfo.Chunks, dirent.Inode, pInodeInfo.Link
}

// AllocateChunk ...
func (ns *nameSpace) AllocateChunk() (int32, *mp.ChunkInfoWithBG) {

	var chunkInfo = mp.ChunkInfoWithBG{}
	ret, _, blockGroup := ns.chooseBlockGroup()

	if ret != 0 {
		logger.Error("AllocateChunk chooseBlockGroup Failed ret :%v", ret)
		return ret, nil //ENOSPC
	}

	var err error
	chunkInfo.ChunkID, err = ns.AllocateChunkID()
	if err != nil {
		logger.Error("AllocateChunk AllocateChunkID Failed err :%v", err)
		return 1, nil
	}

	chunkInfo.ChunkSize = 0
	chunkInfo.BlockGroupWithHost = blockGroup

	return 0, &chunkInfo

}

// SyncChunk ...
func (ns *nameSpace) SyncChunk(pinode uint64, name string, chunkinfo *mp.ChunkInfo) int32 {

	var ret int32

	gRet, dirent := ns.dentryDBGet(pinode, name)
	if gRet != 0 {
		ret = 2 /*ENOENT*/
		return ret
	}

	inodeInfo, err := ns.inodeDBGet(dirent.Inode)
	if err != nil {
		ret = 2 /*ENOENT*/
		return ret
	}

	inodeInfo.ModifiTime = time.Now().Unix()

	var lastChunkID uint64
	var blockGroupUsed int32
	if len(inodeInfo.Chunks) > 0 {
		//for appned write
		lastChunkID = inodeInfo.Chunks[len(inodeInfo.Chunks)-1].ChunkID
		if lastChunkID == chunkinfo.ChunkID {
			inodeInfo.FileSize = inodeInfo.FileSize + int64(chunkinfo.ChunkSize) - int64(inodeInfo.Chunks[len(inodeInfo.Chunks)-1].ChunkSize)
			blockGroupUsed = chunkinfo.ChunkSize - inodeInfo.Chunks[len(inodeInfo.Chunks)-1].ChunkSize
			inodeInfo.Chunks[len(inodeInfo.Chunks)-1] = chunkinfo
		} else {
			inodeInfo.Chunks = append(inodeInfo.Chunks, chunkinfo)
			inodeInfo.FileSize += int64(chunkinfo.ChunkSize)
			blockGroupUsed = chunkinfo.ChunkSize
		}
	} else {
		inodeInfo.Chunks = append(inodeInfo.Chunks, chunkinfo)
		inodeInfo.FileSize += int64(chunkinfo.ChunkSize)
		blockGroupUsed = chunkinfo.ChunkSize
	}

	if err := ns.inodeDBSet(dirent.Inode, inodeInfo); err != nil {
		return 1
	}

	ns.Lock()

	ok, pTmpBlockGroup := ns.blockGroupDBGet(chunkinfo.BlockGroupID)
	if !ok {
		ns.Unlock()
		return 2
	}

	pTmpBlockGroup.FreeSize = pTmpBlockGroup.FreeSize - int64(blockGroupUsed)

	if pTmpBlockGroup.FreeSize <= utils.ChunkSize {
		pTmpBlockGroup.Status = blockGroupFull
	}

	err = ns.blockGroupDBSet(chunkinfo.BlockGroupID, pTmpBlockGroup)
	if err != nil {
		ns.Unlock()
		return 1
	}

	ns.Unlock()
	return 0

}

// AsyncChunk ...
func (ns *nameSpace) AsyncChunk(pinode uint64, name string, chunkid uint64, commitSize uint32, blockGroupID uint64) int32 {

	gRet, dirent := ns.dentryDBGet(pinode, name)
	if gRet != 0 {
		return gRet
	}

	inodeInfo, err := ns.inodeDBGet(dirent.Inode)
	if err != nil {
		if err == raftopt.ErrKeyNotFound {
			return utils.ENO_NOTEXIST
		}
		return 2
	}

	//to check parent dir's existence
	if _, err := ns.inodeDBGet(pinode); err != nil {
		if err == raftopt.ErrKeyNotFound {
			return utils.ENO_NOTEXIST
		}
		return 2
	}

	inodeInfo.ModifiTime = time.Now().Unix()

	var lastChunkID uint64
	var blockGroupUsed uint32
	if len(inodeInfo.Chunks) > 0 {
		//for appned write
		lastChunkID = inodeInfo.Chunks[len(inodeInfo.Chunks)-1].ChunkID
		if lastChunkID == chunkid {
			inodeInfo.FileSize = inodeInfo.FileSize + int64(commitSize)
			blockGroupUsed = commitSize
			inodeInfo.Chunks[len(inodeInfo.Chunks)-1].ChunkSize = inodeInfo.Chunks[len(inodeInfo.Chunks)-1].ChunkSize + int32(commitSize)
		} else {
			chunkinfo := &mp.ChunkInfo{ChunkID: chunkid, ChunkSize: int32(commitSize), BlockGroupID: blockGroupID}
			inodeInfo.Chunks = append(inodeInfo.Chunks, chunkinfo)
			inodeInfo.FileSize += int64(commitSize)
			blockGroupUsed = commitSize
		}
	} else {
		chunkinfo := &mp.ChunkInfo{ChunkID: chunkid, ChunkSize: int32(commitSize), BlockGroupID: blockGroupID}
		inodeInfo.Chunks = append(inodeInfo.Chunks, chunkinfo)
		inodeInfo.FileSize += int64(commitSize)
		blockGroupUsed = commitSize
	}

	if err = ns.inodeDBSet(dirent.Inode, inodeInfo); err != nil {
		return 1
	}

	ns.Lock()

	ok, pTmpBlockGroup := ns.blockGroupDBGet(blockGroupID)
	if !ok {
		ns.Unlock()
		return 2
	}
	pTmpBlockGroup.FreeSize = pTmpBlockGroup.FreeSize - int64(blockGroupUsed)

	if pTmpBlockGroup.FreeSize <= utils.ChunkSize {
		pTmpBlockGroup.Status = blockGroupFull
	}

	err = ns.blockGroupDBSet(blockGroupID, pTmpBlockGroup)
	if err != nil {
		ns.Unlock()
		return 1
	}

	ns.Unlock()

	return 0

}

const (
	blockGroupFree = 0
	blockGroupFull = 2
)

// chooseBlockGroup ...
func (ns *nameSpace) chooseBlockGroup() (int32, uint64, *mp.BlockGroupWithHost) {

	var blockGroupIndexs []int

	ret, bgs := ns.blockGroupDBGetAll()
	if !ret {
		return -1, 0, nil
	}

	for i, v := range bgs {
		if v.Status == blockGroupFull {
			continue
		}
		blockGroupIndexs = append(blockGroupIndexs, i)
	}
	if len(blockGroupIndexs) == 0 {
		return -2, 0, nil
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	in := r.Perm(len(blockGroupIndexs))

	vc := vp.NewVolMgrClient(VolMgrConn)

	pGetBlockGroupByIDReq := &vp.GetBlockGroupByIDReq{}

	var blockGroup mp.BlockGroupWithHost

	for _, v := range in {
		pGetBlockGroupByIDReq.BlockGroupID = bgs[blockGroupIndexs[v]].BlockGroupID
		ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
		pGetBlockGroupByIDAck, err := vc.GetBlockGroupByID(ctx, pGetBlockGroupByIDReq)
		if err != nil || pGetBlockGroupByIDAck.Ret != 0 {
			logger.Error("chooseBlockGroup GetBlockGroupByID failed ...")
			continue
		} else {
			if pGetBlockGroupByIDAck.BlockGroup.Status == 0 {
				blockGroup.BlockGroupID = bgs[blockGroupIndexs[v]].BlockGroupID
				blockGroup.Hosts = pGetBlockGroupByIDAck.BlockGroup.Hosts
				return 0, blockGroup.BlockGroupID, &blockGroup
			}
		}

	}

	return -3, 0, nil
}

// releaseBlockGroup ...
func (ns *nameSpace) releaseBlockGroup(blockGroupID uint64, chunSize int32) {

	ok, blockGroup := ns.blockGroupDBGet(blockGroupID)
	if !ok {
		return
	}

	blockGroup.FreeSize = blockGroup.FreeSize + int64(chunSize)

	/*
		if blockGroup.FreeSize > utils.BlockGroupSize {
			blockGroup.FreeSize = utils.BlockGroupSize
		}
	*/

	if blockGroup.FreeSize > int64(utils.ChunkSize) {

		if blockGroup.Status == blockGroupFull {
			blockGroup.Status = blockGroupFree
		}

	}

	ns.blockGroupDBSet(blockGroupID, blockGroup)

}

// SymLink ...
func (ns *nameSpace) SymLink(pInode uint64, newName string, target string) (int32, uint64) {

	/*update inode info*/
	inodeID, err := ns.AllocateInodeID()
	if err != nil {
		return 1, 0
	}
	tmpSymLinkInfo := mp.SymLinkInfo{
		Target: target,
	}

	err = ns.symLinkDBSet(inodeID, &tmpSymLinkInfo)
	if err != nil {
		return 1, 0
	}

	err = ns.dentryDBSet(pInode, newName, utils.INODE_SYMLINK, inodeID)
	if err != nil {
		ns.symLinkDBDelete(inodeID)
		return 1, 0
	}

	return 0, inodeID
}

// ReadLink ...
func (ns *nameSpace) ReadLink(inode uint64) (int32, string) {

	var ret int32

	ok, symLinkInfo := ns.symLinkDBGet(inode)
	if !ok {
		ret = 2 /*ENOENT*/
		return ret, ""
	}

	return 0, symLinkInfo.Target
}

// Link ...
func (ns *nameSpace) Link(pInode uint64, newName string, OldInode uint64) int32 {

	oldInodeInfo, err := ns.inodeDBGet(OldInode)
	if err != nil {
		if err == raftopt.ErrKeyNotFound {
			return utils.ENO_NOTEXIST
		}
		return 2
	}

	err = ns.dentryDBSet(pInode, newName, utils.INODE_FILE, OldInode)
	if err != nil {
		return 1
	}

	atomic.AddUint32(&oldInodeInfo.Link, 1)

	err = ns.inodeDBSet(OldInode, oldInodeInfo)
	if err != nil {
		ns.dentryDBDelete(pInode, newName)
		return 1
	}

	return 0
}

// AllocateInodeID ...
func (ns *nameSpace) AllocateInodeID() (uint64, error) {
	return ns.RaftGroup.InodeIDGET(ns.RaftGroupID)
}

// AllocateChunkID ...
func (ns *nameSpace) AllocateChunkID() (uint64, error) {
	return ns.RaftGroup.ChunkIDGET(ns.RaftGroupID)
}

// inodeDBGet ...
func (ns *nameSpace) inodeDBGet(inode uint64) (*mp.InodeInfo, error) {

	value, err := ns.RaftGroup.InodeGet(ns.RaftGroupID, inode)
	if err != nil {
		value, err = ns.RaftGroup.InodeGet(ns.RaftGroupID, inode)
		if err != nil {
			logger.Error("inodeDBGet failed , inode %v ", inode)
			return nil, err
		}
	}
	inodeInfo := mp.InodeInfo{}
	err = pbproto.Unmarshal(value, &inodeInfo)
	if err != nil {
		return nil, err
	}
	return &inodeInfo, nil
}

// inodeDBSet ...
func (ns *nameSpace) inodeDBSet(inode uint64, v *mp.InodeInfo) error {

	val, _ := pbproto.Marshal(v)
	err := ns.RaftGroup.InodeSet(ns.RaftGroupID, inode, val)
	if err != nil {
		err := ns.RaftGroup.InodeSet(ns.RaftGroupID, inode, val)
		if err != nil {
			return err
		}
	}

	return nil

}

// inodeDBDelete ...
func (ns *nameSpace) inodeDBDelete(inode uint64) error {

	err := ns.RaftGroup.InodeDel(ns.RaftGroupID, inode)
	if err != nil {
		err := ns.RaftGroup.InodeDel(ns.RaftGroupID, inode)
		if err != nil {
			return err
		}
	}
	return nil
}

// symLinkDBGet ...
func (ns *nameSpace) symLinkDBGet(inode uint64) (bool, *mp.SymLinkInfo) {

	value, err := ns.RaftGroup.InodeGet(ns.RaftGroupID, inode)
	if err != nil {
		value, err = ns.RaftGroup.InodeGet(ns.RaftGroupID, inode)
		if err != nil {
			return false, nil
		}
	}

	symLinkInfo := mp.SymLinkInfo{}
	err = pbproto.Unmarshal(value, &symLinkInfo)
	if err != nil {
		return false, nil
	}

	return true, &symLinkInfo
}

// symLinkDBSet ...
func (ns *nameSpace) symLinkDBSet(inode uint64, v *mp.SymLinkInfo) error {

	val, _ := pbproto.Marshal(v)
	err := ns.RaftGroup.InodeSet(ns.RaftGroupID, inode, val)
	if err != nil {
		err := ns.RaftGroup.InodeSet(ns.RaftGroupID, inode, val)
		if err != nil {
			return err
		}
	}

	return nil

}

// symLinkDBDelete ...
func (ns *nameSpace) symLinkDBDelete(inode uint64) error {

	err := ns.RaftGroup.InodeDel(ns.RaftGroupID, inode)
	if err != nil {
		err := ns.RaftGroup.InodeDel(ns.RaftGroupID, inode)
		if err != nil {
			return err
		}
	}
	return nil
}

// encodeKey ...
func encodeKey(pid uint64, name string) string {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, pid)
	return string(b) + "-" + name
}

// decodeKey ...
func decodeKey(key string) (uint64, string) {

	return binary.BigEndian.Uint64([]byte(key)), string([]byte(key)[9:])
}

// DentryDBGet ...
func (ns *nameSpace) DentryDBGet(pinode uint64, name string) (ret int32, d *mp.Dirent) {
	ret, d = ns.dentryDBGet(pinode, name)
	return
}

// dentryDBGet ...
func (ns *nameSpace) dentryDBGet(pinode uint64, name string) (int32, *mp.Dirent) {
	value, err := ns.RaftGroup.DentryGet(ns.RaftGroupID, encodeKey(pinode, name))
	if err != nil {
		value, err = ns.RaftGroup.DentryGet(ns.RaftGroupID, encodeKey(pinode, name))
		if err != nil {
			if err == raftopt.ErrKeyNotFound {
				return utils.ENO_NOTEXIST, nil
			}
			return 2, nil
		}
	}

	dirent := mp.Dirent{}
	err = pbproto.Unmarshal(value, &dirent)
	if err != nil {
		return 3, nil
	}

	return 0, &dirent
}

// dentryGetRange ...
func (ns *nameSpace) dentryGetRange(pinode uint64) (bool, []*mp.DirentN) {

	value, err := ns.RaftGroup.DentryGetRange(ns.RaftGroupID, encodeKey(pinode, ""), encodeKey(pinode+1, ""))
	if err != nil {
		value, err = ns.RaftGroup.DentryGetRange(ns.RaftGroupID, encodeKey(pinode, ""), encodeKey(pinode+1, ""))
		if err != nil {
			//logger.Error("dentryDBGet vol:%v,key:%v,err:%v\n", ns.VolID, dentryKey, err)
			return false, nil
		}
	}

	var direntNs []*mp.DirentN

	for _, v := range value {
		dirent := mp.Dirent{}
		err = pbproto.Unmarshal(v.V, &dirent)
		if err != nil {
			return false, []*mp.DirentN{}
		}
		_, name := decodeKey(v.K)
		direntN := mp.DirentN{Name: name, Inode: dirent.Inode, InodeType: dirent.InodeType}
		direntNs = append(direntNs, &direntN)
	}

	return true, direntNs

}

// dentryDBSet ...
func (ns *nameSpace) dentryDBSet(pinode uint64, name string, inodeType uint32, inode uint64) error {

	dirent := &mp.Dirent{InodeType: inodeType, Inode: inode}
	val, _ := pbproto.Marshal(dirent)
	err := ns.RaftGroup.DentrySet(ns.RaftGroupID, encodeKey(pinode, name), val)
	if err != nil {
		err := ns.RaftGroup.DentrySet(ns.RaftGroupID, encodeKey(pinode, name), val)
		if err != nil {
			logger.Error("dentryDBSet vol:%v,key:%v,err:%v\n", ns.VolID, encodeKey(pinode, name), err)
			return err
		}
	}
	return nil

}

// dentryDBDelete ...
func (ns *nameSpace) dentryDBDelete(pinode uint64, name string) error {

	err := ns.RaftGroup.DentryDel(ns.RaftGroupID, encodeKey(pinode, name))
	if err != nil {
		err := ns.RaftGroup.DentryDel(ns.RaftGroupID, encodeKey(pinode, name))
		if err != nil {
			logger.Error("dentryDBDelete vol:%v,key:%v,err:%v\n", ns.VolID, encodeKey(pinode, name), err)
			return err
		}
	}

	return nil
}

// BlockGroupDBGet ...
func (ns *nameSpace) BlockGroupDBGet(k uint64) (ret bool, bg *mp.BlockGroup) {
	ret, bg = ns.blockGroupDBGet(k)
	return
}

// blockGroupDBGet ...
func (ns *nameSpace) blockGroupDBGet(k uint64) (bool, *mp.BlockGroup) {
	value, err := ns.RaftGroup.BGGet(ns.RaftGroupID, k)
	if err != nil {
		value, err = ns.RaftGroup.BGGet(ns.RaftGroupID, k)
		if err != nil {
			logger.Error("blockGroupDBGet vol:%v,key:%v,err:%v\n", ns.VolID, k, err)
			return false, nil
		}
	}

	blockGroup := mp.BlockGroup{}
	err = pbproto.Unmarshal(value, &blockGroup)
	if err != nil {
		return false, nil
	}
	return true, &blockGroup

}

// blockGroupDBSet ...
func (ns *nameSpace) blockGroupDBSet(k uint64, v *mp.BlockGroup) error {
	val, _ := pbproto.Marshal(v)
	err := ns.RaftGroup.BGSet(ns.RaftGroupID, k, val)
	if err != nil {
		err := ns.RaftGroup.BGSet(ns.RaftGroupID, k, val)
		if err != nil {
			logger.Error("blockGroupDBSet vol:%v,key:%v,err=%v\n", ns.VolID, k, err)
			return err
		}
	}
	return nil
}

// blockGroupDBGetAll ...
func (ns *nameSpace) blockGroupDBGetAll() (bool, []*mp.BlockGroup) {

	value, err := ns.RaftGroup.BGGetAll(ns.RaftGroupID)
	if err != nil {
		value, err = ns.RaftGroup.BGGetAll(ns.RaftGroupID)
		if err != nil {
			return false, nil
		}
	}

	var blockGroups []*mp.BlockGroup

	for _, v := range value {
		blockGroup := mp.BlockGroup{}

		err = pbproto.Unmarshal(v.V, &blockGroup)
		if err != nil {
			return false, []*mp.BlockGroup{}
		}
		blockGroups = append(blockGroups, &blockGroup)

	}

	return true, blockGroups

}
