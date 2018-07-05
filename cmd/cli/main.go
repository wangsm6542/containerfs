// Copyright (c) 2017, TIG All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	fs "github.com/tiglabs/containerfs/cfs"
	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/utils"
)

func main() {

	var loglevel string
	var logpath string
	var peers string
	var cmd string

	flag.StringVar(&peers, "volmgr", "10.8.64.216,10.8.64.217,10.8.64.218", "ContainerFS VolMgr Host")
	flag.StringVar(&logpath, "logpath", "/export/Logs/containerfs/logs/", "ContainerFS Log Path")
	flag.StringVar(&loglevel, "loglevel", "error", "ContainerFS Log Level")
	flag.StringVar(&cmd, "action", "", `
		createvol [volumename ] size [sata/sas/ssd/nvme... default:sas] [copies default:3]
		expandvol [volumeuuid ] size
		migrate [host]
		deletevol [volumeuuid ]
		snapshotvol [volumeuuid ]
		snapshotcluster
		getvolmetaleader [volumeuuid ]
		getvolmgrleader
		getvolinfo [volumeuuid ]
		getbginfo [bgID]
		getvols
		getinodeinfo [volumeuuid ] parentinodeid name
		getdatanodes
		deldatanode ip port
		getmetanodes`)

	flag.Parse()
	if len(os.Args) >= 2 && (os.Args[1] == "version") {
		fmt.Println(utils.Version())
		os.Exit(0)
	}

	tmp := strings.Split(peers, ",")
	fs.VolMgrHosts = make([]string, 3)
	fs.VolMgrHosts[0] = tmp[0] + ":7703"
	fs.VolMgrHosts[1] = tmp[1] + ":7713"
	fs.VolMgrHosts[2] = tmp[2] + ":7723"

	fs.BufferSize = 1024 * 1024

	logger.SetConsole(true)
	logger.SetRollingFile(logpath, "CLI.log", 10, 100, logger.MB) //each 100M rolling
	switch loglevel {
	case "error":
		logger.SetLevel(logger.ERROR)
	case "debug":
		logger.SetLevel(logger.DEBUG)
	case "info":
		logger.SetLevel(logger.INFO)
	default:
		logger.SetLevel(logger.ERROR)
	}

	//switch flag.Arg(0) {
	switch cmd {

	case "createvol":
		argNum := flag.NArg()
		if argNum != 2 && argNum != 4 {
			fmt.Println("createvol [volname] [space GB] [sata/sas/ssd/nvme...] [copies]")
			os.Exit(1)
		}

		var ret int32
		if argNum == 2 {
			ret = fs.CreateVol(flag.Arg(0), flag.Arg(1), "sas", "3")
		} else if argNum == 4 {
			ret = fs.CreateVol(flag.Arg(0), flag.Arg(1), flag.Arg(2), flag.Arg(3))
		}
		if ret != 0 {
			fmt.Println("failed", ret)
		}

	case "expandvol":
		argNum := flag.NArg()
		if argNum != 2 {
			fmt.Println("expandvol [volUUID] [space GB]")
			os.Exit(1)
		}
		ret := fs.ExpandVol(flag.Arg(0), flag.Arg(1))
		if ret != 0 {
			fmt.Println("failed")
		}

	case "migrate":
		argNum := flag.NArg()
		if argNum != 1 {
			fmt.Println("migrate host")
			os.Exit(1)
		}
		ret := fs.Migrate(flag.Arg(0))
		if ret != 0 {
			fmt.Println("Migrate failed")
		} else {
			fmt.Println("Migrate process on background")
		}

	case "deletevol":
		argNum := flag.NArg()
		if argNum != 1 {
			fmt.Println("deletevol [voluuid]")
			os.Exit(1)
		}
		ret := fs.DeleteVol(flag.Arg(0))
		if ret != 0 {
			fmt.Println("failed")
		}
	case "snapshotvol":
		argNum := flag.NArg()
		if argNum != 1 {
			fmt.Println("snapshotvol [voluuid]")
			os.Exit(1)
		}

		cfs := fs.OpenFileSystem(flag.Arg(0))
		if cfs == nil {
			fmt.Println("no such volume")
			os.Exit(1)
		}

		ret := fs.SnapShotVol(flag.Arg(0))
		if ret != 0 {
			fmt.Println("failed")
		}
	case "snapshotcluster":
		argNum := flag.NArg()
		if argNum != 0 {
			fmt.Println("snapshotcluster")
			os.Exit(1)
		}
		ret := fs.SnapShotCluster()
		if ret != 0 {
			fmt.Println("failed")
		}
	case "getvolmetaleader":
		argNum := flag.NArg()
		if argNum != 1 {
			fmt.Println("getvolmetaleader [voluuid]")
			os.Exit(1)
		}
		leader, _ := fs.GetVolMetaLeader(flag.Arg(0))
		fmt.Println(leader)
	case "getvolmgrleader":
		leader, _ := utils.GetVolMgrLeader(fs.VolMgrHosts)
		fmt.Println(leader)
	case "getvolinfo":
		argNum := flag.NArg()
		if argNum != 1 {
			fmt.Println("getvolinfo [volUUID]")
			os.Exit(1)
		}
		ret, vi := fs.GetVolInfo(flag.Arg(0))
		if ret == 0 {
			fmt.Println(vi)
		} else {
			fmt.Printf("get volume info failed , ret :%d", ret)
		}
	case "getbginfo":
		argNum := flag.NArg()
		if argNum != 1 {
			fmt.Println("getbginfo [bgID]")
			os.Exit(1)
		}
		ret, vi := fs.GetBlockGroupInfo(flag.Arg(0))
		if ret == 0 {
			fmt.Println(vi)
		} else {
			fmt.Printf("get blockgroup info failed , ret :%d", ret)
		}
	case "getvols":
		argNum := flag.NArg()
		if argNum != 0 {
			fmt.Println("getvols")
			os.Exit(1)
		}
		ret, vi := fs.GetAllVolumeInfos()
		if ret == 0 {
			for _, v := range vi {
				fmt.Println(v)
			}
		} else {
			fmt.Printf("get all volumes info failed , ret :%d", ret)
		}

	case "getinodeinfo":
		argNum := flag.NArg()
		if argNum != 3 {
			fmt.Println("getinodeinfo [volUUID] parentinodeid name")
			os.Exit(1)
		}
		cfs := fs.OpenFileSystem(flag.Arg(0))
		if cfs == nil {
			fmt.Println("no such volume")
			os.Exit(1)
		}

		pinode, err := strconv.ParseUint(flag.Arg(1), 10, 64)
		if err == nil {
			ret, inode, inodeinfo := cfs.GetInodeInfoDirect(pinode, flag.Arg(2))
			if ret != 0 {
				fmt.Println("no such inode")
			} else {
				fmt.Printf("inode:%v\ninodeinfo:%v\n", inode, inodeinfo)
				ret, chunkinfo, _, _ := cfs.GetFileChunksDirect(pinode, flag.Arg(2))
				if ret != 0 {
					fmt.Printf("no chunkinfo")
				} else {
					fmt.Printf("chunkinfo:%v\n", chunkinfo)

				}
			}
		} else {
			fmt.Println("err parent inode", err)
		}
	case "getdatanodes":
		argNum := flag.NArg()
		if argNum != 0 {
			fmt.Println("getdatanodes")
			os.Exit(1)
		}
		ret, vi := fs.GetAllDatanode()
		if ret == 0 {
			for _, v := range vi {
				fmt.Println(v)
			}
		} else {
			fmt.Printf("get all datanodes info failed , ret :%d", ret)
		}

	case "deldatanode":
		argNum := flag.NArg()
		if argNum != 2 {
			fmt.Println("deldatanode ip port")
			os.Exit(1)
		}
		ret := fs.DelDatanode(flag.Arg(0))
		if ret == 0 {
			fmt.Printf("del success")
		} else {
			fmt.Printf("del failed , ret :%d", ret)
		}
	case "getmetanodes":
		argNum := flag.NArg()
		if argNum != 0 {
			fmt.Println("getmetanodes")
			os.Exit(1)
		}
		ret, vi := fs.GetAllMetanode()
		if ret == 0 {
			for _, v := range vi {
				fmt.Println(v)
			}
		} else {
			fmt.Printf("get all datanodes info failed , ret :%d", ret)
		}
	default:
		fmt.Println("wrong action operation")
	}

}
