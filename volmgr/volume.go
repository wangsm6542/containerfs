// Copyright (c) 2017, TIG All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

package volmgr

import (
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/proto/dp"
	"github.com/tiglabs/containerfs/proto/mp"
	"github.com/tiglabs/containerfs/proto/vp"
	"github.com/tiglabs/containerfs/raftopt"
	"github.com/tiglabs/containerfs/utils"
	"golang.org/x/net/context"
)

//CreateVol creates a voluem as new filesystem instance as volume
func (vs *VolMgrServer) CreateVol(ctx context.Context, in *vp.CreateVolReq) (*vp.CreateVolAck, error) {

	vs.Lock()
	defer vs.Unlock()
	ack := vp.CreateVolAck{}

	//todo: check raft leader
	if !vs.RaftServer.IsLeader(1) {
		ack.Ret = -1
		return &ack, utils.ErrNotLeader
	}

	voluuid, err := utils.GenUUID()
	if err != nil {
		logger.Error("Create volume uuid err:%v", err)
		return &ack, err
	}

	rgID, err := vs.Cluster.RaftGroup.RGIDGET(1)
	if err != nil {
		logger.Error("Create Volume name:%v uuid:%v raftGroupID error:%v", voluuid, in.VolName, err)
		return &ack, err
	}

	//choose 3 metanodes as a raftgroup
	mng, err := vs.chooseMetaNodes()
	if err != nil {
		logger.Error("Create Volume name:%v  error:%v", in.VolName, err)
		return &ack, err
	}

	//the volume need block group total numbers
	var blockGroupNum int32
	if in.SpaceQuota%utils.BlkSizeG == 0 {
		blockGroupNum = in.SpaceQuota / utils.BlkSizeG
	} else {
		blockGroupNum = in.SpaceQuota/utils.BlkSizeG + 1
		in.SpaceQuota = blockGroupNum * utils.BlkSizeG
	}

	v, err := vs.Cluster.RaftGroup.DataNodeGetAll(1)
	if err != nil {
		logger.Error("GetAllDataNode Info failed:%v for CreateVol", err)
		return &ack, err
	}
	for _, vv := range v {
		logger.Debug("DataNode: %v", vv.Host)
	}
	inuseNodes := make(map[string][]*vp.DataNode)
	allip := make([]string, 0)

	// pass bad status and free not enough datanodes
	for _, vv := range v {
		if vv.Status != 0 || vv.Free < utils.BlkSizeG*2 || vv.Tier != in.Tier {
			continue
		}
		tmp := strings.Split(vv.Host, ":")
		k := tmp[0]
		inuseNodes[k] = append(inuseNodes[k], vv)
	}

	for k := range inuseNodes {
		allip = append(allip, k)
	}

	copies, _ := strconv.Atoi(in.Copies)

	if len(allip) < copies {
		logger.Error("Create Volume:%v Tier:%v Copies:%v but DataNode nums:%v less than Copies:%v, so forbid CreateVol", voluuid, in.Tier, copies, len(allip), copies)
		ack.Ret = -1
		return &ack, nil
	}

	vol := &vp.Volume{
		UUID:      voluuid,
		Name:      in.VolName,
		Tier:      in.Tier,
		Copies:    int32(copies),
		TotalSize: in.SpaceQuota,
		RGID:      rgID,
	}

	dataNodesUsedMap := make(map[string][]uint64)
	dataNodesForUpdate := make(map[string]*vp.DataNode)
	var blockGroups []*mp.BlockGroup
	for i := int32(0); i < blockGroupNum; i++ {
		bgID, err := vs.Cluster.RaftGroup.BGIDGET(1)
		if err != nil {
			logger.Error("AllocateBGID for CreateVol failed, err:%v", err)
			return &ack, err
		}

		idxs := utils.GenerateRandomNumber(0, len(allip), copies)
		if len(idxs) != copies {
			ack.Ret = -5
			return &ack, nil
		}
		var hosts []string
		bg := &vp.BlockGroup{BlockGroupID: bgID,
			RGID:     rgID,
			VolID:    voluuid,
			FreeSize: utils.BlockGroupSize}

		for n := 0; n < copies; n++ {
			ipkey := allip[idxs[n]]
			idx := utils.GenerateRandomNumber(0, len(inuseNodes[ipkey]), 1)
			if len(idx) <= 0 {
				ack.Ret = -1
				return &ack, nil
			}
			dataNode := inuseNodes[ipkey][idx[0]]
			bg.Hosts = append(bg.Hosts, dataNode.Host)
			hosts = append(hosts, dataNode.Host)
			dataNode.Free = dataNode.Free - utils.BlkSizeG
			dataNodesForUpdate[dataNode.Host] = dataNode
			if inuseNodes[ipkey][idx[0]].Free < utils.BlkSizeG*2 {
				inuseNodes[ipkey] = append(inuseNodes[ipkey][:idx[0]], inuseNodes[ipkey][idx[0]+1:]...)
			}
		}
		var newAllIp []string
		for _, ip := range allip {
			if len(inuseNodes[ip]) <= 0 {
				delete(inuseNodes, ip)
			} else {
				newAllIp = append(newAllIp, ip)
			}
		}
		allip = newAllIp

		err = vs.Cluster.RaftGroup.BlockGroupSet(bgID, bg)
		if err != nil {
			logger.Error("Create Volume:%v Set blockgroup:%v blocks:%v failed:%v", voluuid, bgID, bg, err)
			return &ack, err
		}
		//to set hosts blockgroups
		for _, host := range hosts {
			dataNodesUsedMap[host] = append(dataNodesUsedMap[host], bgID)
			if err = vs.Cluster.RaftGroup.DataNodeBGAddBG(host, bgID); err != nil {
				logger.Error("failed to add bg to datandoe: %v", err)
				return &ack, err
			}
		}
		logger.Debug("Create Volume:%v Tier:%v Set one blockgroup:%v blocks:%v to Cluster Map success", voluuid, in.Tier, bgID, bg)
		vol.BlockGroups = append(vol.BlockGroups, bgID)
		blockGroup := &mp.BlockGroup{}
		blockGroup.BlockGroupID = bgID
		blockGroup.FreeSize = utils.BlockGroupSize
		blockGroups = append(blockGroups, blockGroup)
	}

	vs.Cluster.RaftGroup.VolumeSet(1, voluuid, vol)
	ack.Ret = 0
	ack.UUID = voluuid
	ack.RaftGroupID = rgID

	var volumeValue mp.VolumeValue
	var volumePeers []*mp.VolumePeer

	for _, mn := range mng {
		volumePeers = append(volumePeers, &mp.VolumePeer{NodeID: mn.Id, Host: mn.Host})
	}
	volumeValue.RaftGroupID = rgID
	volumeValue.VolumePeers = volumePeers
	volumeValue.BlockGroups = blockGroups
	flag := true
	for _, mn := range mng {
		conn, err := utils.Dial(mn.Host + ":9901")
		if err != nil {
			logger.Error("Dial metanode[%v] failed: %v", mn.Host, err)
			flag = false
			break
		}

		mc := mp.NewMetaNodeClient(conn)

		pCreateNameSpaceReq := &mp.CreateNameSpaceReq{
			Volume: &volumeValue,
			VolID:  voluuid,
		}
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pCreateNameSpaceAck, err := mc.CreateNameSpace(ctx, pCreateNameSpaceReq)
		if err != nil {
			logger.Error("CreateVolume CreateNameSpace failed host : %v err : %v", mn.Host, err)
			flag = false
			break
		}
		if pCreateNameSpaceAck.Ret != 0 {
			logger.Error("CreateVolume CreateNameSpace failed host : %v ret : %v", mn.Host, pCreateNameSpaceAck.Ret)
			flag = false
			break
		}

	}

	//clean kv data on fail
	if !flag {
		//clean DataNodeBG kv
		for host := range dataNodesUsedMap {
			vs.Cluster.RaftGroup.DataNodeBGDelBG(host, dataNodesUsedMap[host])
		}
	}

	metaNodeRG := &vp.MetaNodeRG{}
	metaNodeRG.RGID = rgID
	metaNodeRG.UUID = voluuid
	for _, mn := range mng {
		metaNode := &vp.MetaNode{Id: mn.Id}
		metaNodeRG.MetaNodes = append(metaNodeRG.MetaNodes, metaNode)
	}
	err = vs.Cluster.RaftGroup.MetaNodeRGSet(rgID, metaNodeRG)
	if err != nil {
		logger.Error("set kv failed: %v", err)
		return &ack, err
	}

	// update datanode kv data
	for host, dataNode := range dataNodesForUpdate {
		vs.Cluster.RaftGroup.DataNodeSet(1, host, dataNode)
	}
	return &ack, nil
}

//ExpandVol expands a volume's assigned size
func (vs *VolMgrServer) ExpandVol(ctx context.Context, in *vp.ExpandVolReq) (*vp.ExpandVolAck, error) {

	vs.Lock()
	defer vs.Unlock()
	ack := vp.ExpandVolAck{}

	if !vs.RaftServer.IsLeader(1) {
		ack.Ret = -1
		return &ack, utils.ErrNotLeader
	}

	vol, err := vs.Cluster.RaftGroup.VolumeGet(1, in.UUID)
	if err != nil {
		logger.Error("Get volume info[%v] faield: %v", in.UUID, err)
		return &ack, err
	}

	//the volume need block group total numbers
	var blockGroupNum int32
	if in.Space%utils.BlkSizeG == 0 {
		blockGroupNum = in.Space / utils.BlkSizeG
	} else {
		blockGroupNum = in.Space/utils.BlkSizeG + 1
		in.Space = blockGroupNum * utils.BlkSizeG
	}

	vol.TotalSize = vol.TotalSize + in.Space
	v, err := vs.Cluster.RaftGroup.DataNodeGetAll(1)
	if err != nil {
		logger.Error("GetAllDataNode Info failed:%v for CreateVol", err)
		return &ack, err
	}
	for _, vv := range v {
		logger.Debug("DataNode: %v", vv.Host)
	}
	inuseNodes := make(map[string][]*vp.DataNode)
	allip := make([]string, 0)

	// pass bad status and free not enough datanodes
	for _, vv := range v {
		if vv.Status != 0 || vv.Free < utils.BlkSizeG*2 || vv.Tier != vol.Tier {
			continue
		}
		tmp := strings.Split(vv.Host, ":")
		k := tmp[0]
		inuseNodes[k] = append(inuseNodes[k], vv)
	}

	for k := range inuseNodes {
		allip = append(allip, k)
	}

	if len(allip) < int(vol.Copies) {
		logger.Error("Expand Volume:%v Tier:%v but DataNode nums:%v less than Copies:%v, so forbid CreateVol", vol.UUID, vol.Tier, len(allip), vol.Copies)
		ack.Ret = -1
		return &ack, nil
	}

	dataNodesUsedMap := make(map[string][]uint64)
	dataNodesForUpdate := make(map[string]*vp.DataNode)

	var blockGroups []*mp.BlockGroup
	for i := int32(0); i < blockGroupNum; i++ {
		bgID, err := vs.Cluster.RaftGroup.BGIDGET(1)
		if err != nil {
			logger.Error("AllocateBGID for CreateVol failed, err:%v", err)
			return &ack, err
		}

		idxs := utils.GenerateRandomNumber(0, len(allip), int(vol.Copies))
		if len(idxs) != int(vol.Copies) {
			ack.Ret = -1
			return &ack, nil
		}

		var hosts []string
		bg := &vp.BlockGroup{BlockGroupID: bgID, FreeSize: utils.BlockGroupSize}
		for n := 0; n < int(vol.Copies); n++ {
			ipkey := allip[idxs[n]]
			idx := utils.GenerateRandomNumber(0, len(inuseNodes[ipkey]), 1)
			if len(idx) <= 0 {
				ack.Ret = -1
				return &ack, nil
			}
			dataNode := inuseNodes[ipkey][idx[0]]
			host := inuseNodes[ipkey][idx[0]].Host
			bg.Hosts = append(bg.Hosts, host)
			hosts = append(hosts, host)
			dataNode.Free = dataNode.Free - utils.BlkSizeG
			dataNodesForUpdate[dataNode.Host] = dataNode
			if inuseNodes[ipkey][idx[0]].Free < utils.BlkSizeG*2 {
				inuseNodes[ipkey] = append(inuseNodes[ipkey][:idx[0]], inuseNodes[ipkey][idx[0]+1:]...)
			}
		}
		err = vs.Cluster.RaftGroup.BlockGroupSet(bgID, bg)
		if err != nil {
			logger.Error("Expand Volume:%v Set blockgroup:%v blocks:%v failed:%v", vol.UUID, bgID, bg, err)
			return &ack, err
		}
		//to set hosts blockgroups
		for _, host := range hosts {
			dataNodesUsedMap[host] = append(dataNodesUsedMap[host], bgID)
			if err = vs.Cluster.RaftGroup.DataNodeBGAddBG(host, bgID); err != nil {
				logger.Error("failed to add bg to datandoe: %v", err)
				return &ack, err
			}
		}
		logger.Debug("Expand Volume:%v Tier:%v Set one blockgroup:%v blocks:%v to Cluster Map success", vol.UUID, vol.Tier, bgID, bg)
		vol.BlockGroups = append(vol.BlockGroups, bgID)
		blockGroup := &mp.BlockGroup{}
		blockGroup.BlockGroupID = bgID
		blockGroup.FreeSize = utils.BlockGroupSize
		blockGroups = append(blockGroups, blockGroup)
	}

	if err = vs.Cluster.RaftGroup.VolumeSet(1, vol.UUID, vol); err != nil {
		ack.Ret = -1
		for host := range dataNodesUsedMap {
			vs.Cluster.RaftGroup.DataNodeBGDelBG(host, dataNodesUsedMap[host])
		}
	}

	metaNodeRG, err := vs.Cluster.RaftGroup.MetaNodeRGGet(vol.RGID)
	if err != nil {
		logger.Error("MetaNodeRGGet failed: %v", err)
		return &ack, err
	}
	metaNodes, err := vs.getMetaNodesViaIds(metaNodeRG.MetaNodes)
	if err != nil {
		logger.Error("MetaNodes Get failed: %v", err)
		return &ack, err
	}
	var mHosts []string
	for _, m := range metaNodes {
		mHosts = append(mHosts, m.Host+":9901")
	}
	mLeader, err := utils.GetMetaNodeLeader(mHosts, in.UUID)
	if err != nil {
		logger.Error("GetMetaNodeLeader[ hosts: %v] failed: %v", mHosts, err)
		return &ack, err
	}
	mConn, err := utils.Dial(mLeader)
	if err != nil {
		logger.Error("Dial metanode leader[%v] failed: %v", mLeader, err)
		return &ack, err
	}
	defer mConn.Close()
	mc := mp.NewMetaNodeClient(mConn)
	pExpandNameSpaceReq := mp.ExpandNameSpaceReq{
		VolID:       in.UUID,
		BlockGroups: blockGroups,
	}
	ctx, _ = context.WithTimeout(context.Background(), 5*time.Second)
	pExpandNameSpaceAck, err := mc.ExpandNameSpace(ctx, &pExpandNameSpaceReq)
	if err != nil {
		logger.Error("ExpandNameSpace failed: %v", err)
		return &ack, err
	}
	if pExpandNameSpaceAck.Ret != 0 {
		if err != nil {
			logger.Error("ExpandNameSpace failed: %v", pExpandNameSpaceAck.Ret)
			return &ack, err
		}
	}
	// update datanode kv data
	for host, dataNode := range dataNodesForUpdate {
		vs.Cluster.RaftGroup.DataNodeSet(1, host, dataNode)
	}
	return &ack, nil
}

//ExpandVolRS is not implemented yet
func (vs *VolMgrServer) ExpandVolRS(ctx context.Context, in *vp.ExpandVolRSReq) (*vp.ExpandVolRSAck, error) {

	vs.Lock()
	defer vs.Unlock()

	ack := vp.ExpandVolRSAck{}
	ack.Ret = 0
	return &ack, nil
}

//DelVolRSForExpand is not implemented yet
func (vs *VolMgrServer) DelVolRSForExpand(ctx context.Context, in *vp.DelVolRSForExpandReq) (*vp.DelVolRSForExpandAck, error) {
	ack := vp.DelVolRSForExpandAck{}
	ack.Ret = 0
	return &ack, nil
}

//DeleteVol drops and deletes a volume
func (vs *VolMgrServer) DeleteVol(ctx context.Context, in *vp.DeleteVolReq) (*vp.DeleteVolAck, error) {
	ack := vp.DeleteVolAck{}

	volume, err := vs.Cluster.RaftGroup.VolumeGet(1, in.UUID)
	if err != nil {
		logger.Error("Get BGS map from Cluster MetaNodeAddr for DeleteVol:%v failed, err:%v", in.UUID, err)
		return &ack, err
	}

	delBlockGroupBadNum := 0
	var bg *vp.BlockGroup
	for _, v := range volume.BlockGroups {
		if bg, err = vs.Cluster.RaftGroup.BlockGroupGet(v); err != nil {
			logger.Error("DeleteVol BlockGroupGet failed: %v, bgID: %v", err, v)
			delBlockGroupBadNum = delBlockGroupBadNum + 1
			continue
		}
		delBlockBadNum := 0
		for _, vv := range bg.Hosts {
			datanodeKey := vv
			tmpDataNode, err := vs.Cluster.RaftGroup.DataNodeGet(1, datanodeKey)
			if err != nil {
				logger.Error("Get DataNode:%v map from Cluster MetaNodeAddr for Update Free+5:%v failed for DeleteVol, err:%v", datanodeKey, err)
				delBlockBadNum += 1
				continue
			}

			tmpDataNode.Free = tmpDataNode.Free + 5
			err = vs.Cluster.RaftGroup.DataNodeSet(1, datanodeKey, tmpDataNode)
			if err != nil {
				logger.Error("Set DataNode:%v map value.Free Add 5G from Cluster MetaNodeAddr for failed for DeleteVol, err:%v", datanodeKey, err)
				delBlockBadNum += 1
				continue
			}
		}

		bgKey := in.UUID + fmt.Sprintf("-%d", bg.BlockGroupID)
		err = vs.Cluster.RaftGroup.BlockGroupDel(1, bgKey)
		if err != nil || delBlockBadNum != 0 {
			logger.Error("Delete BG:%v from Cluster MetaNodeAddr failed", bgKey)
			delBlockGroupBadNum += 1
		}
	}
	if delBlockGroupBadNum != 0 {
		ack.Ret = -1
		return &ack, nil
	}
	err = vs.Cluster.RaftGroup.VolumeDel(1, in.UUID)
	if err != nil {
		logger.Error("Delete Volume:%v map from Cluster MetaNodeAddr failed, err:%v", in.UUID, err)
		return &ack, err
	}

	logger.Debug("Delete Volume:%v from Cluster MetadataAddr Success", in.UUID)
	return &ack, nil
}

//Migrate move data from one datanode to another
func (vs *VolMgrServer) Migrate(ctx context.Context, in *vp.MigrateReq) (*vp.MigrateAck, error) {
	ack := vp.MigrateAck{}

	//DataNode
	dataNode, err := vs.Cluster.RaftGroup.DataNodeGet(1, in.DataNodeHost)
	if err != nil {
		logger.Error("DataNodeGet for datanode[%v] failed: %v", in.DataNodeHost, err)
		return &ack, err
	}

	tier := dataNode.Tier

	//DataNodeBGS
	dataNodeBGS, err := vs.Cluster.RaftGroup.DataNodeBGGet(in.DataNodeHost)
	if err != nil {
		logger.Error("DataNodeBGGet for datanode[%v] failed: %v", in.DataNodeHost, err)
		return &ack, err
	}
	go func() {
		var ret int
		for _, bg := range dataNodeBGS.BGS {
			logger.Debug("Begin Migrage BlockGroup %v ...", bg)
			ret = vs.beginMigrate(bg, in.DataNodeHost, tier)
			logger.Debug("End Migrage BlockGroup %v ret %v ...", bg, ret)
		}
	}()

	ack.Ret = 0
	return &ack, nil
}

//beginMigrate is an utility method for migrating data from one datanode to another
func (vs *VolMgrServer) beginMigrate(bgID uint64, badHost string, tier string) int {
	var shost string

	blockGroup, err := vs.Cluster.RaftGroup.BlockGroupGet(bgID)
	if err != nil {
		logger.Error("BlockGroupGet[%v] faild: %v", bgID, err)
		return -1
	}
	for _, host := range blockGroup.Hosts {
		if host != badHost {
			shost = host
			break
		}
	}
	var dataNodes []*vp.DataNode
	v, err := vs.Cluster.RaftGroup.DataNodeGetAll(1)
	if err != nil {
		logger.Error("DataNodeGetAll failed: %v", err)
		return -3
	}
	for _, vv := range v {
		flag := true
		for _, host := range blockGroup.Hosts {
			if host == vv.Host {
				flag = false
				break
			}
		}
		logger.Debug("DataNode: %v", vv)
		if flag && vv.Status == 0 && vv.Tier == tier && vv.Free > utils.BlkSizeG {
			dataNodes = append(dataNodes, vv)
		}
	}

	if len(dataNodes) < 1 {
		logger.Error("No enough free datanode hosts available")
		return -5
	}

	idxs := utils.GenerateRandomNumber(0, len(dataNodes), 1)
	dHost := dataNodes[idxs[0]].Host
	//migrate blockgroup
	ret := vs.migrateBlockGroup(bgID, shost, dHost)
	if ret != 0 {
		return -6
	}

	//update datanode/datanodebgs/blockgroup
	dataNode, err := vs.Cluster.RaftGroup.DataNodeGet(1, dHost)
	if err != nil {
		logger.Error("DataNodeGet[%v] failed: %v", dHost, err)
		return -7
	}
	dataNode.Free = dataNode.Free - utils.BlkSizeG

	if err = vs.Cluster.RaftGroup.DataNodeSet(1, dHost, dataNode); err != nil {
		logger.Error("DataNodeSet failed: %v", err)
		return -10
	}
	dataNodeBGS, err := vs.Cluster.RaftGroup.DataNodeBGGet(dHost)
	if err != nil && err != raftopt.ErrKeyNotFound {
		logger.Error("DataNodeBGGet failed: %v", err)
		return -11
	}
	if err == raftopt.ErrKeyNotFound {
		dataNodeBGS = &vp.DataNodeBGS{Host: dHost}
	}
	flag := true
	if flag {
		dataNodeBGS.BGS = append(dataNodeBGS.BGS, bgID)
		err = vs.Cluster.RaftGroup.DataNodeBGSet(dHost, dataNodeBGS)
		if err != nil {
			logger.Error("DataNodeBGSet failed: %v", err)
			return -14
		}
	}

	for i := range blockGroup.Hosts {
		if blockGroup.Hosts[i] == badHost {
			blockGroup.Hosts[i] = dHost
		}
	}
	if err = vs.Cluster.RaftGroup.BlockGroupSet(bgID, blockGroup); err != nil {
		logger.Error("BlockGroupSet failed: %v", err)
		return -15
	}
	return 0
}

//migrateBlockGroup is an utility method for moving blockgroup
func (vs *VolMgrServer) migrateBlockGroup(bgID uint64, shost string, dhost string) int32 {
	//migrate
	pRecvMigrateReq := &dp.RecvMigrateReq{BlockGroupID: bgID, DstHost: dhost}
	conn, err := utils.Dial(shost)
	if err != nil {
		logger.Error("Dial datanode[%v] failed: %v", shost, err)
		return -1
	}
	defer conn.Close()
	dc := dp.NewDataNodeClient(conn)
	pRecvMigrateAck, err := dc.RecvMigrateMsg(context.Background(), pRecvMigrateReq)
	if err != nil {
		logger.Error("RecvMigrateMsg failed: %v", err)
		return -2
	}
	if pRecvMigrateAck.Ret != 0 {
		logger.Error("RecvMigrateMsg Ret[%v] failed", pRecvMigrateAck.Ret)
		return -3
	}
	return 0

}

//GetBlockGroupByID gets blockgroup info by blockgroup id
func (vs *VolMgrServer) GetBlockGroupByID(ctx context.Context, in *vp.GetBlockGroupByIDReq) (*vp.GetBlockGroupByIDAck, error) {
	ack := vp.GetBlockGroupByIDAck{}

	blockGroup, err := vs.Cluster.RaftGroup.BlockGroupGet(in.BlockGroupID)
	if err != nil {
		return &ack, err
	}
	ack.BlockGroup = blockGroup
	return &ack, nil
}

//updateBlockGroupStatus is an utility method for updating blockgroup status into state machine
func (vs *VolMgrServer) updateBlockGroupStatus() {
	var blockGroup *vp.BlockGroup
	var err error
	for bgID, status := range vs.BgStatusMap {
		if status > 0 {
			vs.BgStatusMap[bgID] = 0
			status = 1
		}
		blockGroup, err = vs.Cluster.RaftGroup.BlockGroupGet(bgID)
		if err != nil {
			logger.Error("updateBlockGroup BlockGroupGet failed: %v", err)
			continue
		}
		if blockGroup.Status != status {
			blockGroup.Status = status
			vs.Cluster.RaftGroup.BlockGroupSet(bgID, blockGroup)
		}
	}
}

//SnapShotCluster takes snapshot of cluster and volumes status into disk data
func (vs *VolMgrServer) SnapShotCluster(ctx context.Context, in *vp.SnapShotClusterReq) (*vp.SnapShotClusterAck, error) {
	go raftopt.TakeClusterKvSnapShot(vs.Cluster.RaftGroup, vs.Cluster.RaftStorage, path.Join(vs.volmgrAddr.Waldir, "Cluster", "wal", "snap"))
	return &vp.SnapShotClusterAck{Ret: 0}, nil
}

//GetVolMgrRG gets metanode group of the specific volume
func (vs *VolMgrServer) GetVolMgrRG(ctx context.Context, in *vp.GetVolMgrRGReq) (*vp.GetVolMgrRGAck, error) {
	ack := vp.GetVolMgrRGAck{}
	leaderID, _ := vs.RaftServer.LeaderTerm(1)
	if leaderID <= 0 {
		ack.Ret = 1
		return &ack, nil
	}
	ack.Ret = 0
	ack.Leader = raftopt.ClusterAddrDatabase[leaderID].Grpc
	for _, rft := range raftopt.ClusterAddrDatabase {
		ack.Peers = append(ack.Peers, rft.Grpc)
	}
	return &ack, nil
}
