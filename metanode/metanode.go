// Copyright (c) 2017, TIG All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

package metanode

import (
	"fmt"
	"net"
	// pprof
	_ "net/http/pprof"
	"sync"
	"time"

	"github.com/tiglabs/containerfs/logger"
	ns "github.com/tiglabs/containerfs/metanode/namespace"
	"github.com/tiglabs/containerfs/proto/mp"
	"github.com/tiglabs/containerfs/proto/vp"
	"github.com/tiglabs/containerfs/raftopt"
	"github.com/tiglabs/containerfs/raftopt/common"
	"github.com/tiglabs/containerfs/utils"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type addr struct {
	Host        string
	NodeID      uint64
	Waldir      string
	Log         string
	VolmgrHosts []string
}

// MetaNodeServerAddr ...
var MetaNodeServerAddr addr

// MetaNodeServer ...
type MetaNodeServer struct {
	NodeID     uint64
	Addr       *common.Address
	Resolver   common.Resolver
	RaftServer *raft.RaftServer
	sync.Mutex
}

// GetMetaLeader ...
func (ms *MetaNodeServer) GetMetaLeader(ctx context.Context, in *mp.GetMetaLeaderReq) (*mp.GetMetaLeaderAck, error) {
	ack := mp.GetMetaLeaderAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	for id, addr := range raftopt.VolumeAddrDatabase {
		logger.Debug("id:%v addr:%v", id, *addr)
	}
	leaderID, _ := ms.RaftServer.LeaderTerm(nameSpace.RaftGroupID)
	if leaderID <= 0 {
		ack.Ret = 1
		return &ack, nil
	}
	ack.Ret = 0
	ack.Leader = raftopt.VolumeAddrDatabase[leaderID].Grpc
	return &ack, nil
}

//CreateNameSpace ...
func (ms *MetaNodeServer) CreateNameSpace(ctx context.Context, in *mp.CreateNameSpaceReq) (*mp.CreateNameSpaceAck, error) {
	ack := mp.CreateNameSpaceAck{}

	for _, v := range in.Volume.VolumePeers {
		addr := &common.Address{
			Grpc:      v.Host + ":9901",
			Heartbeat: v.Host + ":9902",
			Replicate: v.Host + ":9903",
			Pprof:     v.Host + ":9904",
		}
		ms.Resolver.AddNode(v.NodeID, addr)
	}

	var peers []proto.Peer
	for _, v := range in.Volume.VolumePeers {
		peers = append(peers, proto.Peer{ID: v.NodeID})
	}

	ack.Ret = ns.CreateNameSpace(ms.RaftServer, peers, MetaNodeServerAddr.NodeID, MetaNodeServerAddr.Waldir, in.VolID, in.Volume.RaftGroupID, in.Volume.BlockGroups, false)
	return &ack, nil
}

//ExpandNameSpace ...
func (ms *MetaNodeServer) ExpandNameSpace(ctx context.Context, in *mp.ExpandNameSpaceReq) (*mp.ExpandNameSpaceAck, error) {

	ack := mp.ExpandNameSpaceAck{}

	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.ExpandNameSpace(in.BlockGroups)

	return &ack, nil
}

// SnapShotNameSpace ...
func (ms *MetaNodeServer) SnapShotNameSpace(ctx context.Context, in *mp.SnapShotNameSpaceReq) (*mp.SnapShotNameSpaceAck, error) {
	go ns.SnapShotNameSpace(ms.RaftServer, in.VolID, MetaNodeServerAddr.Waldir)
	return &mp.SnapShotNameSpaceAck{Ret: 0}, nil
}

// DeleteNameSpace ...
func (ms *MetaNodeServer) DeleteNameSpace(ctx context.Context, in *mp.DeleteNameSpaceReq) (*mp.DeleteNameSpaceAck, error) {
	ack := mp.DeleteNameSpaceAck{}
	ack.Ret = ns.DeleteNameSpace(ms.RaftServer, in.VolID)
	return &ack, nil
}

//GetFSInfo ...
func (ms *MetaNodeServer) GetFSInfo(ctx context.Context, in *mp.GetFSInfoReq) (*mp.GetFSInfoAck, error) {
	ack := mp.GetFSInfoAck{}

	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}

	ack = nameSpace.GetFSInfo(in.VolID)
	return &ack, nil
}

//CreateDirDirect ...
func (ms *MetaNodeServer) CreateDirDirect(ctx context.Context, in *mp.CreateDirDirectReq) (*mp.CreateDirDirectAck, error) {
	ack := mp.CreateDirDirectAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret, ack.Inode = nameSpace.CreateDirDirect(in.PInode, in.Name)
	return &ack, nil
}

//GetInodeInfoDirect ...
func (ms *MetaNodeServer) GetInodeInfoDirect(ctx context.Context, in *mp.GetInodeInfoDirectReq) (*mp.GetInodeInfoDirectAck, error) {
	ack := mp.GetInodeInfoDirectAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret, ack.InodeInfo, ack.Inode = nameSpace.GetInodeInfoDirect(in.PInode, in.Name)
	return &ack, nil
}

//StatDirect ...
func (ms *MetaNodeServer) StatDirect(ctx context.Context, in *mp.StatDirectReq) (*mp.StatDirectAck, error) {
	ack := mp.StatDirectAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.InodeType, ack.Inode, ack.Ret = nameSpace.StatDirect(in.PInode, in.Name)
	return &ack, nil
}

//ListDirect ...
func (ms *MetaNodeServer) ListDirect(ctx context.Context, in *mp.ListDirectReq) (*mp.ListDirectAck, error) {
	ack := mp.ListDirectAck{}

	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Dirents, ack.Ret = nameSpace.ListDirect(in.PInode)
	return &ack, nil
}

// DeleteDirDirect ...
func (ms *MetaNodeServer) DeleteDirDirect(ctx context.Context, in *mp.DeleteDirDirectReq) (*mp.DeleteDirDirectAck, error) {

	ack := mp.DeleteDirDirectAck{}

	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.DeleteDirDirect(in.PInode, in.Name)
	return &ack, nil

}

// RenameDirect ...
func (ms *MetaNodeServer) RenameDirect(ctx context.Context, in *mp.RenameDirectReq) (*mp.RenameDirectAck, error) {
	ack := mp.RenameDirectAck{}

	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.RenameDirect(in.OldPInode, in.OldName, in.NewPInode, in.NewName)
	return &ack, nil
}

//CreateFileDirect ...
func (ms *MetaNodeServer) CreateFileDirect(ctx context.Context, in *mp.CreateFileDirectReq) (*mp.CreateFileDirectAck, error) {
	ack := mp.CreateFileDirectAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret, ack.Inode = nameSpace.CreateFileDirect(in.PInode, in.Name)
	return &ack, nil
}

// DeleteFileDirect ...
func (ms *MetaNodeServer) DeleteFileDirect(ctx context.Context, in *mp.DeleteFileDirectReq) (*mp.DeleteFileDirectAck, error) {

	ack := mp.DeleteFileDirectAck{}

	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.DeleteFileDirect(in.PInode, in.Name)
	return &ack, nil

}

// DeleteSymLinkDirect ...
func (ms *MetaNodeServer) DeleteSymLinkDirect(ctx context.Context, in *mp.DeleteSymLinkDirectReq) (*mp.DeleteSymLinkDirectAck, error) {

	ack := mp.DeleteSymLinkDirectAck{}

	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.DeleteSymLinkDirect(in.PInode, in.Name)
	return &ack, nil

}

// GetFileChunksDirect ...
func (ms *MetaNodeServer) GetFileChunksDirect(ctx context.Context, in *mp.GetFileChunksDirectReq) (*mp.GetFileChunksDirectAck, error) {
	ack := mp.GetFileChunksDirectAck{}

	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	gRet, chunkInfos, inode, link := nameSpace.GetFileChunksDirect(in.PInode, in.Name)
	if gRet != 0 {
		ack.Ret = gRet
		return &ack, nil
	}

	vc := vp.NewVolMgrClient(ns.VolMgrConn)
	pGetBlockGroupByIDReq := &vp.GetBlockGroupByIDReq{}

	for _, v := range chunkInfos {
		var chunkInfoWithBG mp.ChunkInfoWithBG
		chunkInfoWithBG.ChunkID = v.ChunkID
		chunkInfoWithBG.ChunkSize = v.ChunkSize

		blockGroup := &mp.BlockGroupWithHost{}

		pGetBlockGroupByIDReq.BlockGroupID = v.BlockGroupID
		ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
		pGetBlockGroupByIDAck, err := vc.GetBlockGroupByID(ctx, pGetBlockGroupByIDReq)
		if err != nil || pGetBlockGroupByIDAck.Ret != 0 {
			ack.Ret = -1
			return &ack, nil
		}

		blockGroup.BlockGroupID = v.BlockGroupID
		blockGroup.Hosts = pGetBlockGroupByIDAck.BlockGroup.Hosts

		chunkInfoWithBG.BlockGroupWithHost = blockGroup
		ack.ChunkInfos = append(ack.ChunkInfos, &chunkInfoWithBG)
	}

	ack.Ret = 0
	ack.Inode = inode
	ack.Link = link

	return &ack, nil
}

// AllocateChunk ...
func (ms *MetaNodeServer) AllocateChunk(ctx context.Context, in *mp.AllocateChunkReq) (*mp.AllocateChunkAck, error) {
	ack := mp.AllocateChunkAck{}

	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ret, chunkInfo := nameSpace.AllocateChunk()
	if ret != 0 {
		ack.Ret = ret
		logger.Error("AllocateChunk Failed ret %v", ret)
		return &ack, nil
	}

	ack.ChunkInfo = chunkInfo
	return &ack, nil
}

// SyncChunk ...
func (ms *MetaNodeServer) SyncChunk(ctx context.Context, in *mp.SyncChunkReq) (*mp.SyncChunkAck, error) {
	ack := mp.SyncChunkAck{}
	chunkinfo := in.ChunkInfo
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.SyncChunk(in.ParentInodeID, in.Name, chunkinfo)
	return &ack, nil
}

// AsyncChunk ...
func (ms *MetaNodeServer) AsyncChunk(ctx context.Context, in *mp.AsyncChunkReq) (*mp.AsyncChunkAck, error) {
	ack := mp.AsyncChunkAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.AsyncChunk(in.ParentInodeID, in.Name, in.ChunkID, in.CommitSize, in.BlockGroupID)
	return &ack, nil
}

// SymLink ...
func (ms *MetaNodeServer) SymLink(ctx context.Context, in *mp.SymLinkReq) (*mp.SymLinkAck, error) {
	ack := mp.SymLinkAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret, ack.Inode = nameSpace.SymLink(in.PInode, in.Name, in.Target)
	return &ack, nil
}

// Link ...
func (ms *MetaNodeServer) Link(ctx context.Context, in *mp.LinkReq) (*mp.LinkAck, error) {
	ack := mp.LinkAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.Link(in.PInode, in.Name, in.OldInode)
	return &ack, nil
}

// ReadLink ...
func (ms *MetaNodeServer) ReadLink(ctx context.Context, in *mp.ReadLinkReq) (*mp.ReadLinkAck, error) {
	ack := mp.ReadLinkAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret, ack.Target = nameSpace.ReadLink(in.Inode)
	return &ack, nil
}

// GetSymLinkInfoDirect ...
func (ms *MetaNodeServer) GetSymLinkInfoDirect(ctx context.Context, in *mp.GetSymLinkInfoDirectReq) (*mp.GetSymLinkInfoDirectAck, error) {
	ack := mp.GetSymLinkInfoDirectAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret, ack.Inode = nameSpace.GetSymLinkInfoDirect(in.PInode, in.Name)
	return &ack, nil
}

// StartMetaDataService ...
func StartMetaDataService(metaServer *MetaNodeServer) {

	lis, err := net.Listen("tcp", metaServer.Addr.Grpc)
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on:%v", metaServer.Addr.Grpc))
	}
	s := grpc.NewServer()
	mp.RegisterMetaNodeServer(s, metaServer)
	// Register reflection service on gRPC server.
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		panic("Failed to serve")
	}
}

// LoadMetaData ...
func (ms *MetaNodeServer) LoadMetaData() int32 {

	vc := vp.NewVolMgrClient(ns.VolMgrConn)
	pGetMetaNodeRGPeersReq := &vp.GetMetaNodeRGPeersReq{
		MetaNodeID: MetaNodeServerAddr.NodeID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	pGetMetaNodeRGPeersAck, err := vc.GetMetaNodeRGPeers(ctx, pGetMetaNodeRGPeersReq)
	if err != nil || pGetMetaNodeRGPeersAck.Ret != 0 {
		logger.Error("loadMetaData GetMetaNodeRGPeers failed ...")
		return -1
	}

	for _, v := range pGetMetaNodeRGPeersAck.RaftGroups {
		for _, vv := range v.MetaNodes {
			addr := &common.Address{
				Grpc:      vv.Host + ":9901",
				Heartbeat: vv.Host + ":9902",
				Replicate: vv.Host + ":9903",
				Pprof:     vv.Host + ":9904",
			}
			ms.Resolver.AddNode(vv.Id, addr)
		}
	}

	for _, v := range pGetMetaNodeRGPeersAck.RaftGroups {
		logger.Debug("loadMetaData,Vol:%v", v)

		var peers []proto.Peer
		for _, vv := range v.MetaNodes {
			peers = append(peers, proto.Peer{ID: vv.Id})
		}

		ns.CreateNameSpace(ms.RaftServer, peers, MetaNodeServerAddr.NodeID, MetaNodeServerAddr.Waldir, v.UUID, v.RGID, nil, true)
	}
	return 0
}

// MetaNodeHealthCheck ...
func (ms *MetaNodeServer) MetaNodeHealthCheck(ctx context.Context, in *mp.MetaNodeHealthCheckReq) (*mp.MetaNodeHealthCheckAck, error) {
	ack := mp.MetaNodeHealthCheckAck{}
	return &ack, nil
}

// GetBlockGroupInfo ...
func (ms *MetaNodeServer) GetBlockGroupInfo(ctx context.Context, in *mp.GetBlockGroupInfoReq) (*mp.GetBlockGroupInfoAck, error) {

	ack := mp.GetBlockGroupInfoAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ok, blockGroup := nameSpace.BlockGroupDBGet(in.BGID)
	if ok {
		ack.BlockGroup = blockGroup
		return &ack, nil
	}
	ack.Ret = -2
	return &ack, nil
}

// RegistryToVolMgr ...
func (ms *MetaNodeServer) RegistryToVolMgr() int {

	_, conn, err := utils.DialVolMgr(MetaNodeServerAddr.VolmgrHosts)
	if err != nil {
		logger.Error("registryToVolMgr DialVolMgr failed ...")
		return -1
	}

	vc := vp.NewVolMgrClient(conn)

	pMetaNode := &vp.MetaNode{
		Id:   MetaNodeServerAddr.NodeID,
		Host: MetaNodeServerAddr.Host,
		Mem:  utils.MemStat().Free,
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	pMetaNodeAck, err := vc.MetaNodeRegistry(ctx, pMetaNode)
	if err != nil || pMetaNodeAck.Ret != 0 {
		logger.Error("registryToVolMgr MetaNodeRegistry failed ...")
		return -1
	}

	ns.VolMgrLeader, err = utils.GetVolMgrLeader(MetaNodeServerAddr.VolmgrHosts)
	if err != nil {
		return -1
	}

	_, ns.VolMgrConn, err = utils.DialVolMgr(MetaNodeServerAddr.VolmgrHosts)
	if ns.VolMgrConn == nil || err != nil {
		return -1
	}

	ns.VolMgrInit = true

	return 0
}
