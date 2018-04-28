// Copyright (c) 2017, TIG All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

package volmgr

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/proto/vp"
	"github.com/tiglabs/containerfs/raftopt"
	"github.com/tiglabs/containerfs/raftopt/common"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

//VolMgrServerAddr is a wrapper of volmgr service address and path
type VolMgrServerAddr struct {
	Host   string
	NodeID uint64
	Peers  []proto.Peer
	Ips    []string
	Waldir string
	Log    string
}

//Cluster is a wrapper of raftgroup service
type Cluster struct {
	sync.Mutex
	RaftGroup   *raftopt.ClusterKvStateMachine
	RaftStorage *wal.Storage
	IsLoaded    bool
}

//VolMgrServer implemnts cluster and volumes management service interface
type VolMgrServer struct {
	NodeID          uint64
	Addr            *common.Address
	Resolver        common.Resolver
	RaftServer      *raft.RaftServer
	Cluster         *Cluster
	wg              sync.WaitGroup
	BgStatusMap     map[uint64]int32
	BgStatusMapSync sync.Mutex
	volmgrAddr      *VolMgrServerAddr
	sync.Mutex
}

//NewVolMgrServer creates a new VolMgrServer and carry out part of service initialization
func NewVolMgrServer(volmgrAddr *VolMgrServerAddr) (*VolMgrServer, error) {

	var vs VolMgrServer
	var err error

	vs.NodeID = volmgrAddr.NodeID
	vs.volmgrAddr = volmgrAddr
	vs.Resolver = raftopt.NewClusterResolver()
	vs.BgStatusMap = make(map[uint64]int32)

	addr, ok := raftopt.ClusterAddrDatabase[volmgrAddr.NodeID]
	if !ok {
		logger.Error("NewVolMgrServer ClusterAddrDatabase get failed")
		return nil, fmt.Errorf("no such address info. nodeId: %d", volmgrAddr.NodeID)
	}
	vs.Addr = addr

	err = common.StartRaftServer(&vs.RaftServer, vs.Resolver, addr, volmgrAddr.NodeID)
	if err != nil {
		logger.Error("NewVolMgrServer StartRaftServer failed err %v", err)
		return nil, fmt.Errorf("failed to start raft service %v", err)
	}

	for _, p := range volmgrAddr.Peers {
		vs.Resolver.AddNode(p.ID, nil)
	}

	vs.showLeaders()
	vs.startHealthCheck()

	return &vs, nil
}

//Load loads cluster meta data and volumes from disk
func (vs *VolMgrServer) Load() error {

	fmt.Println(vs.RaftServer)
	fmt.Println(vs.volmgrAddr.Peers)
	fmt.Println(vs.volmgrAddr.NodeID)
	fmt.Println(vs.volmgrAddr.Waldir)

	sm, sg, err := raftopt.CreateClusterKvStateMachine(vs.RaftServer, vs.volmgrAddr.Peers, vs.volmgrAddr.NodeID, vs.volmgrAddr.Waldir, "Cluster", 1)
	if err != nil {
		return err
	}
	vs.Cluster = &Cluster{RaftGroup: sm, RaftStorage: sg}
	logger.Debug("VolMgrServer.Load success...")
	return nil
}

//StartService registers and starts volmgr service
func (vs *VolMgrServer) StartService() {

	lis, err := net.Listen("tcp", vs.Addr.Grpc)
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on:%v", vs.Addr.Grpc))
	}
	s := grpc.NewServer()
	vp.RegisterVolMgrServer(s, vs)
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		panic("Failed to serve")
	}
}

//showLeaders prints volmgr cluster status into log file
func (vs *VolMgrServer) showLeaders() {
	ticker := time.NewTicker(time.Second * 10)
	go func() {
		for range ticker.C {
			l, t := vs.RaftServer.LeaderTerm(1)
			logger.Debug("ShowLeaders RaftGroup LeaderID %v Term %v", l, t)
		}
	}()
}

//startHealthCheck starts goroutines to check health state of metanodes and datanodes
func (vs *VolMgrServer) startHealthCheck() {
	td := time.NewTicker(time.Second * 1)
	go func() {
		for range td.C {
			if vs.RaftServer.IsLeader(1) {
				vs.DetectDataNodes()
			}
		}
	}()

	tm := time.NewTicker(time.Second * 1)
	go func() {
		for range tm.C {
			if vs.RaftServer.IsLeader(1) {
				vs.DetectMetaNodes()
			}
		}
	}()
}
