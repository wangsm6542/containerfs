// Copyright (c) 2017, TIG All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/tiglabs/containerfs/cfs"
	"github.com/tiglabs/containerfs/client/fuse"
	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/utils"
)

func main() {

	var peers string

	flag.StringVar(&peers, "volmgr", "10.8.64.216,10.8.64.217,10.8.64.218", "ContainerFS VolMgr Host")

	buffertype := flag.Int("buffertype", 0, "ContainerFS per file buffertype : 0 512KB 1 256KB 2 128KB")
	uuid := flag.String("uuid", "xxx", "ContainerFS Volume UUID")
	mountPoint := flag.String("mountpoint", "/mnt", "ContainerFS MountPoint")
	log := flag.String("log", "/export/Logs/containerfs/logs/", "ContainerFS log level")
	loglevel := flag.String("loglevel", "error", "ContainerFS log level")
	isReadOnly := flag.Int("readonly", 0, "Is readonly Volume 1 for ture ,0 for false")
	writeBuff := flag.Int("writebuffer", 0, "Write buffer size in mb, must be no larger than 32")
	flag.Parse()
	if len(os.Args) >= 2 && (os.Args[1] == "version") {
		fmt.Println(utils.Version())
		os.Exit(0)
	}

	if *writeBuff < 0 || *writeBuff > 32 {
		fmt.Println("bad writeBuff, must be no larger than 32")
		os.Exit(0)
	}
	tmp := strings.Split(peers, ",")
	if len(tmp) != 3 {
		fmt.Println("bad peers, must be 3 peer")
		os.Exit(0)
	}
	cfs.VolMgrHosts = make([]string, 3)
	cfs.VolMgrHosts[0] = tmp[0] + ":7703"
	cfs.VolMgrHosts[1] = tmp[1] + ":7713"
	cfs.VolMgrHosts[2] = tmp[2] + ":7723"

	switch *buffertype {
	case 0:
		cfs.BufferSize = 512 * 1024
	case 1:
		cfs.BufferSize = 256 * 1024
	case 2:
		cfs.BufferSize = 128 * 1024
	default:
		cfs.BufferSize = 512 * 1024
	}

	cfs.WriteBufferSize = *writeBuff * 1024 * 1024

	logger.SetConsole(true)
	logger.SetRollingFile(*log, "fuse.log", 10, 100, logger.MB) //each 100M rolling

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

	http.HandleFunc("/logleveldebug", utils.Logleveldebug)
	http.HandleFunc("/loglevelerror", utils.Loglevelerror)
	go func() {
		http.ListenAndServe(":10000", nil)
	}()

	err := fuse.Mount(*uuid, *mountPoint, *isReadOnly)
	if err != nil {
		fmt.Println("mount failed ...", err)
	}
}
