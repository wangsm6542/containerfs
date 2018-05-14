// Copyright (c) 2017, TIG All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

package volmgr

import (
	"time"

	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/proto/mp"
	"github.com/tiglabs/containerfs/proto/vp"
	"github.com/tiglabs/containerfs/utils"
	"golang.org/x/net/context"
)

//ClusterInfo gets cluster info
func (s *VolMgrServer) ClusterInfo(ctx context.Context, in *vp.ClusterInfoReq) (*vp.ClusterInfoAck, error) {
	ack := vp.ClusterInfoAck{}
	ack.MetaNum = 3

	v, err := s.Cluster.RaftGroup.DataNodeGetAll(1)
	if err != nil {
		logger.Error("GetAllDataNode Info failed:%v for ClusterInfo", err)
		ack.Ret = 1
		return &ack, nil
	}

	ack.DataNum = int32(len(v))

	var total int32
	var free int32

	for _, vv := range v {
		total = total + vv.Capacity
		free = free + vv.Free
	}
	ack.ClusterSpace = total
	ack.ClusterFreeSpace = free

	volumes, err := s.Cluster.RaftGroup.VolumeGetAll(1)
	if err != nil {
		logger.Error("GetAllVolume Info failed:%v for ClusterInfo", err)
		ack.Ret = -1
		return &ack, nil
	}

	ack.VolNum = int32(len(volumes))

	logger.Debug("ClusterInfo: %v", ack)

	return &ack, nil
}

//GetMetaNode implements GetMetaNode of volmgr service
func (s *VolMgrServer) GetMetaNode(ctx context.Context, in *vp.GetAllMetaNodeReq) (*vp.GetAllMetaNodeAck, error) {
	ack := vp.GetAllMetaNodeAck{}
	if mns, err := s.Cluster.RaftGroup.MetaNodeGetAll(1); err == nil {
		ack.MetaNodes = mns
	} else {
		ack.Ret = -1
		return &ack, err
	}
	return &ack, nil
}

//MetaNodeInfo is dumped now
func (s *VolMgrServer) MetaNodeInfo(ctx context.Context, in *vp.MetaNodeInfoReq) (*vp.MetaNodeInfoAck, error) {
	ack := vp.MetaNodeInfoAck{}

	return &ack, nil
}

//VolMgrInfo implements VolMgrInfo of volmgr service
func (s *VolMgrServer) VolMgrInfo(ctx context.Context, in *vp.VolMgrInfoReq) (*vp.VolMgrInfoAck, error) {
	ack := vp.VolMgrInfoAck{}
	ack.VolMgrID = s.NodeID
	ack.IsLeader = s.RaftServer.IsLeader(s.NodeID)
	return &ack, nil
}

//VolumeInfos gets all descriptions of all volumes
func (s *VolMgrServer) VolumeInfos(ctx context.Context, in *vp.VolumeInfosReq) (*vp.VolumeInfosAck, error) {
	ack := vp.VolumeInfosAck{}

	v, err := s.Cluster.RaftGroup.VolumeGetAll(1)
	if err != nil {
		logger.Error("GetAllVolume Info failed:%v for VolumeInfo", err)
		ack.Ret = -1
		return &ack, nil
	}

	for _, vv := range v {
		volume := vp.Volume{}
		volume.RGID = vv.RGID
		volume.UUID = vv.UUID
		volume.Name = vv.Name
		volume.Tier = vv.Tier

		ack.Volumes = append(ack.Volumes, &volume)
	}

	logger.Debug("VolumeInfos: %v", ack.Volumes)

	return &ack, nil
}

//GetVolInfo implements GetVolInfo of volmgr service
func (s *VolMgrServer) GetVolInfo(ctx context.Context, in *vp.GetVolInfoReq) (*vp.GetVolInfoAck, error) {
	ack := vp.GetVolInfoAck{}

	volume, err := s.Cluster.RaftGroup.VolumeGet(1, in.UUID)
	if err != nil {
		return &ack, nil
	}

	ack.Volume = volume
	ack.Ret = 0

	return &ack, nil
}

//GetBlockGroupInfo gives details of specific blockgroup
func (s *VolMgrServer) GetBlockGroupInfo(ctx context.Context, in *vp.GetBlockGroupInfoReq) (*vp.GetBlockGroupInfoAck, error) {
	ack := vp.GetBlockGroupInfoAck{}

	//Get blockgroup info from volmgr
	blockGroup, err := s.Cluster.RaftGroup.BlockGroupGet(in.BGID)
	if err != nil {
		logger.Error("BlockGroupGet[%v] failed: %v", in.BGID, err)
		return &ack, err
	}

	//Get blockgroup info from metanode
	mnrg, err := s.Cluster.RaftGroup.MetaNodeRGGet(blockGroup.RGID)
	if err != nil {
		logger.Error("BlockGroupGet[bg:%v, rg:%v] failed: %v", in.BGID, blockGroup.RGID, err)
		return &ack, err
	}

	metaNodes, err := s.getMetaNodesViaIds(mnrg.MetaNodes)
	if err != nil {
		logger.Error("Get metanodes failed: %v", err)
		return &ack, err
	}
	pGetBlockGroupInfoReq := mp.GetBlockGroupInfoReq{VolID: blockGroup.VolID, BGID: blockGroup.BlockGroupID}

	var flag bool
	for _, m := range metaNodes {
		conn, err := utils.Dial(m.Host + ":9901")
		if err != nil {
			logger.Error("Dial metanode host[%v] failed: %v", m.Host+":9901", err)
			continue
		}
		defer conn.Close()
		mc := mp.NewMetaNodeClient(conn)

		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pGetBlockGroupInfoAck, err := mc.GetBlockGroupInfo(ctx, &pGetBlockGroupInfoReq)
		if err != nil {
			logger.Error("GetBlockGroupInfo from host[%v] failed: %v", m.Host+":9901", err)
			continue
		}
		if pGetBlockGroupInfoAck.Ret != 0 {
			continue
		}
		blockGroup.FreeSize = pGetBlockGroupInfoAck.BlockGroup.FreeSize
		flag = true
		break
	}
	if !flag {
		ack.Ret = -1
	} else {
		ack.BlockGroup = blockGroup
	}
	return &ack, nil
}
