// Copyright (c) 2017, TIG All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

package utils

import (
	"errors"
	"time"

	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/proto/mp"
	"github.com/tiglabs/containerfs/proto/vp"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

//GetMetaNodeLeader is utility tool for getting metanode raft group leader
func GetMetaNodeLeader(hosts []string, UUID string) (string, error) {

	//logger.Debug("GetMetaNodeLeader hosts %v", hosts)

	var leader string
	var flag bool
	for _, host := range hosts {
		conn, err := grpc.Dial(host, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Millisecond*300), grpc.FailOnNonTempDialError(true))
		if err != nil {
			logger.Error("GetMetaNodeLeader Failed host %v,err %v", host, err)
			continue
		}
		defer conn.Close()
		mc := mp.NewMetaNodeClient(conn)
		pGetMetaLeaderReq := &mp.GetMetaLeaderReq{
			VolID: UUID,
		}
		ctx, _ := context.WithTimeout(context.Background(), time.Second)
		pGetMetaLeaderAck, err := mc.GetMetaLeader(ctx, pGetMetaLeaderReq)
		if err != nil {
			logger.Error("GetMetaNodeLeader GetMetaLeader failed host %v", host)
			time.Sleep(time.Millisecond * 300)
			continue
		}
		if pGetMetaLeaderAck.Ret != 0 {
			logger.Error("GetMetaNodeLeader GetMetaLeader failed host %v", host)
			continue
		}
		leader = pGetMetaLeaderAck.Leader
		flag = true
		break
	}
	if !flag {
		logger.Debug("GetMetaNodeLeader hosts %v", hosts)

		return "", errors.New("Get leader failed")
	}
	//logger.Debug("GetMetaNodeLeader success ")

	return leader, nil

}

//Dial is tool for creating a new grpc connection
func Dial(host string) (*grpc.ClientConn, error) {
	var conn *grpc.ClientConn
	var err error
	conn, err = grpc.Dial(host, grpc.WithMaxMsgSize(32*1024*1024), grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Millisecond*300), grpc.FailOnNonTempDialError(true))
	if err != nil {
		time.Sleep(300 * time.Millisecond)
		conn, err = grpc.Dial(host, grpc.WithMaxMsgSize(32*1024*1024), grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Millisecond*300), grpc.FailOnNonTempDialError(true))
		if err != nil {
			time.Sleep(300 * time.Millisecond)
			conn, err = grpc.Dial(host, grpc.WithMaxMsgSize(32*1024*1024), grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Millisecond*300), grpc.FailOnNonTempDialError(true))
		}
	}
	return conn, err
}

//TryDial dials and closes connection for checking network
func TryDial(host string) error {
	conn, err := grpc.Dial(host, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Millisecond*300), grpc.FailOnNonTempDialError(true))
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}

//GetVolMgrLeader is tool for getting volmgr cluster leader
func GetVolMgrLeader(hosts []string) (string, error) {

	var leader string
	var flag bool
	for _, ip := range hosts {
		conn, err := grpc.Dial(ip, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Millisecond*300), grpc.FailOnNonTempDialError(true))
		if err != nil {
			continue
		}
		defer conn.Close()
		vc := vp.NewVolMgrClient(conn)
		pGetVolMgrRGReq := &vp.GetVolMgrRGReq{}
		ctx, _ := context.WithTimeout(context.Background(), time.Second)
		pGetVolMgrRGAck, err := vc.GetVolMgrRG(ctx, pGetVolMgrRGReq)
		if err != nil {
			time.Sleep(time.Millisecond * 300)
			continue
		}
		if pGetVolMgrRGAck.Ret != 0 {
			//time.Sleep(time.Millisecond * 300)
			continue
		}
		leader = pGetVolMgrRGAck.Leader
		flag = true
		break
	}
	if !flag {
		return "", errors.New("Get leader failed")
	}
	return leader, nil

}

//DialVolMgr is a tool for creating a new grpc connection to volmgr
func DialVolMgr(hosts []string) (string, *grpc.ClientConn, error) {
	var conn *grpc.ClientConn
	var err error

	leader, err := GetVolMgrLeader(hosts)
	if err != nil {
		return "", nil, err
	}
	conn, err = grpc.Dial(leader, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Millisecond*300), grpc.FailOnNonTempDialError(true))
	if err != nil {
		time.Sleep(300 * time.Millisecond)
		leader, err = GetVolMgrLeader(hosts)
		if err != nil {
			return "", nil, err
		}
		conn, err = grpc.Dial(leader, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Millisecond*300), grpc.FailOnNonTempDialError(true))
		if err != nil {
			time.Sleep(300 * time.Millisecond)
			leader, err = GetVolMgrLeader(hosts)
			if err != nil {
				return "", nil, err
			}
			conn, err = grpc.Dial(leader, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Millisecond*300), grpc.FailOnNonTempDialError(true))
		}
	}
	return leader, conn, err
}
