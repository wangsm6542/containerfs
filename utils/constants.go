// Copyright (c) 2017, TIG All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

package utils

import (
	"time"
)

//filesystem error number
const (
	ENO_OK         = iota
	ENO_NOTEXIST   = 20
	ENO_NOENT      = 21
	ENO_NOTDEFINED = 22
)

//filesystem inode type
const (
	INODE_DIR     = 1
	INODE_FILE    = 2
	INODE_SYMLINK = 3
)

//fuse cache life-cycle
const (
	FUSE_ATTR_CACHE_LIFE   = 0 * time.Second
	FUSE_LOOKUP_CACHE_LIFE = 0 * time.Second
)

//block and chunk size
const (
	BlkSizeG       = 16
	BlockGroupSize = int64(BlkSizeG * 1024 * 1024 * 1024)
	ChunkSize      = 64 * 1024 * 1024
)

//gRPC MaxMsgSize
const (
	MaxMsgSize = 4 * 1024 * 1024
)
