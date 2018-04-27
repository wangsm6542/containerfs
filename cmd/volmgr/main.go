// Copyright (c) 2017, TIG All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/raftopt"
	"github.com/tiglabs/containerfs/utils"
	"github.com/tiglabs/containerfs/volmgr"
	"github.com/tiglabs/raft/proto"
)

//volmgrAddr used for parsing parameters
var volmgrAddr volmgr.VolMgrServerAddr

//init does startup pars parsing
func init() {

	flag.StringVar(&volmgrAddr.Host, "host", "127.0.0.1", "ContainerFS VolMgr Host")
	nodeid := flag.Int64("nodeid", 1, "ContainerFS VolMgr ID")
	peers := flag.String("nodepeer", "1,2,3", "ContainerFS VolMgr peers")
	ips := flag.String("nodeips", "127.0.0.1,127.0.0.1,127.0.0.1", "ContainerFS VolMgr ips")
	flag.StringVar(&volmgrAddr.Waldir, "wal", "/export/containerfs/VolMgr/data", "ContainerFS VolMgr waldir")
	flag.StringVar(&volmgrAddr.Log, "logpath", "/export/Logs/containerfs/logs/", "ContainerFS VolMgr log")
	loglevel := flag.String("loglevel", "error", "ContainerFS VolMgr log level")

	flag.Parse()
	if len(os.Args) >= 2 && (os.Args[1] == "version") {
		fmt.Println(utils.Version())
		os.Exit(0)
	}
	volmgrAddr.NodeID = uint64(*nodeid)
	volmgrAddr.Ips = strings.Split(*ips, ",")
	peerarray := strings.Split(*peers, ",")
	var err error
	volmgrAddr.Peers, err = parsePeers(peerarray)
	if err != nil {
		logger.Fatal("parse peers failed!. peers=%v", peers)
		os.Exit(1)
	}

	logger.SetConsole(true)
	logger.SetRollingFile(volmgrAddr.Log, "volmgr.log", 10, 100, logger.MB) //each 100M rolling
	switch *loglevel {
	case "error":
		logger.SetLevel(logger.ERROR)
	case "debug":
		logger.SetLevel(logger.DEBUG)
	case "info":
		logger.SetLevel(logger.INFO)
	default:
		logger.SetLevel(logger.ERROR)
	}

	logger.Info("VolMgrServerAddr: %v version %v", volmgrAddr, utils.Version())

}

//parsePeers is an utility of parsing volmgr cluster peers
func parsePeers(peersstr []string) (peers []proto.Peer, err error) {
	for _, s := range peersstr {
		p, err := strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
		peers = append(peers, proto.Peer{ID: uint64(p)})
	}
	return
}

//main starts the world
func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	raftopt.AddInit(volmgrAddr.Ips)

	logger.Fatal("init volmgr service  %v", volmgrAddr)

	vs, err := volmgr.NewVolMgrServer(&volmgrAddr)
	if err != nil {
		logger.Fatal("init volmgr service failed: %v, volmgr stopped!", err)
		os.Exit(1)
	}

	if err = vs.Load(); err != nil {
		logger.Fatal("load cluster data failed: %v, volmgr stopped!", err)
		os.Exit(1)
	}

	http.HandleFunc("/logleveldebug", utils.Logleveldebug)
	http.HandleFunc("/loglevelerror", utils.Loglevelerror)
	go func() {
		http.ListenAndServe(vs.Addr.Pprof, nil)
	}()

	vs.StartService()
}
