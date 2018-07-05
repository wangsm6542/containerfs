// Copyright (c) 2017, TIG All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

package fuse

import (
	"math"
	"os"
	"sync"
	"syscall"
	"time"

	bfuse "bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/tiglabs/containerfs/cfs"
	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/utils"
	"golang.org/x/net/context"
)

// File to store file infos
type File struct {
	mu    sync.Mutex
	inode uint64

	fileType uint32
	parent   *dir
	name     string
	writers  uint
	handles  uint32
	cfile    *cfs.CFile
}

var _ node = (*File)(nil)
var _ = fs.Node(&File{})
var _ = fs.Handle(&File{})
var _ = fs.NodeForgetter(&File{})
var _ = fs.NodeOpener(&File{})
var _ = fs.HandleReleaser(&File{})
var _ = fs.HandleReader(&File{})
var _ = fs.HandleWriter(&File{})
var _ = fs.HandleFlusher(&File{})
var _ = fs.NodeFsyncer(&File{})
var _ = fs.NodeSetattrer(&File{})
var _ = fs.NodeReadlinker(&File{})

// Forget to forget the file
func (f *File) Forget() {
	if f.parent == nil {
		return
	}

	f.mu.Lock()
	name := f.name
	f.mu.Unlock()

	f.parent.forgetChild(name, f)
}

// setName to set name of the file
func (f *File) setName(name string) {

	f.mu.Lock()
	f.name = name
	f.mu.Unlock()

}

// setParentInode to set parent inode of the file
func (f *File) setParentInode(pdir *dir) {

	f.mu.Lock()
	f.parent = pdir
	f.mu.Unlock()
}

// Attr to get attributes of the file
func (f *File) Attr(ctx context.Context, a *bfuse.Attr) error {

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fileType == utils.INODE_FILE {

		ret, inode, inodeInfo := f.parent.fs.cfs.GetInodeInfoDirect(f.parent.inode, f.name)
		if ret == 0 {
		} else if ret == utils.ENO_NOTEXIST {
			delete(f.parent.active, f.name)
			return bfuse.Errno(syscall.ENOENT)
		} else if ret != 0 || inodeInfo == nil {
			return nil
		}
		a.Valid = utils.FUSE_ATTR_CACHE_LIFE
		a.Ctime = time.Unix(inodeInfo.ModifiTime, 0)
		a.Mtime = time.Unix(inodeInfo.ModifiTime, 0)
		a.Atime = time.Unix(inodeInfo.AccessTime, 0)
		a.Size = uint64(inodeInfo.FileSize)
		if f.cfile != nil && a.Size < uint64(f.cfile.FileSizeInCache) {
			a.Size = uint64(f.cfile.FileSizeInCache)
		}
		a.Inode = uint64(inode)
		a.Nlink = inodeInfo.Link

		a.BlockSize = BLOCK_SIZE
		a.Blocks = uint64(math.Ceil(float64(a.Size)/float64(a.BlockSize))) * 8
		a.Mode = 0666
		a.Valid = time.Second

	} else if f.fileType == utils.INODE_SYMLINK {

		ret, inode := f.parent.fs.cfs.GetSymLinkInfoDirect(f.parent.inode, f.name)
		if ret == 0 {
		} else if ret == utils.ENO_NOTEXIST {
			delete(f.parent.active, f.name)
			return bfuse.Errno(syscall.ENOENT)
		} else if ret != 0 {
			return nil
		}

		a.Inode = uint64(inode)
		a.Mode = 0666 | os.ModeSymlink
		a.Valid = 0
	}

	return nil
}

// Open to open the file by name
func (f *File) Open(ctx context.Context, req *bfuse.OpenRequest, resp *bfuse.OpenResponse) (fs.Handle, error) {

	logger.Debug("Open start : name %v inode %v Flags %v pinode %v pname %v", f.name, f.inode, req.Flags, f.parent.inode, f.parent.name)

	if int(req.Flags)&os.O_TRUNC != 0 {
		logger.Debug("Open OpenFileDirect 0000")
		return nil, bfuse.Errno(syscall.EPERM)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	//logger.Debug("Open get locker : name %v inode %v Flags %v pinode %v pname %v", f.name, f.inode, req.Flags, f.parent.inode, f.parent.name)

	if f.writers > 0 {
		if int(req.Flags)&os.O_WRONLY != 0 || int(req.Flags)&os.O_RDWR != 0 {
			logger.Error("Open failed writers > 0")
			return nil, bfuse.Errno(syscall.EPERM)
		}
	}

	var ret int32

	logger.Debug("Open OpenFileDirect 11111")

	if f.cfile == nil && f.handles == 0 {
		ret, f.cfile = f.parent.fs.cfs.OpenFileDirect(f.parent.inode, f.name, int(req.Flags))
		if ret == 0 {
			logger.Debug("Open OpenFileDirect 22222")

		} else if ret == utils.ENO_NOTEXIST {
			//clean dirty cache in dir map
			delete(f.parent.active, f.name)

			if int(req.Flags) != os.O_RDONLY && (int(req.Flags)&os.O_CREATE > 0 || int(req.Flags)&^(os.O_WRONLY|os.O_TRUNC) == 0) {
				ret, f.cfile = f.parent.fs.cfs.CreateFileDirect(f.parent.inode, f.name, int(req.Flags))
				if ret != 0 {
					if ret == 17 {
						return nil, bfuse.Errno(syscall.EEXIST)
					}
					return nil, bfuse.Errno(syscall.EIO)
				}

				f.inode = f.cfile.Inode
				f.handles = 0
				f.writers = 0
				f.parent.active[f.name] = &refcount{node: f}
			} else {
				return nil, bfuse.Errno(syscall.ENOENT)
			}
		} else {
			logger.Error("Open failed OpenFileDirect ret %v", ret)
			return nil, bfuse.Errno(syscall.EIO)
		}
	} else {
		logger.Debug("Open OpenFileDirect 33333")

		f.parent.fs.cfs.UpdateOpenFileDirect(f.parent.inode, f.name, f.cfile, int(req.Flags))
	}

	tmp := f.handles + 1
	f.handles = tmp

	if int(req.Flags)&os.O_WRONLY != 0 || int(req.Flags)&os.O_RDWR != 0 {
		tmp := f.writers + 1
		f.writers = tmp
	}

	logger.Debug("Open end : name %v inode %v Flags %v pinode %v pname %v", f.name, f.inode, req.Flags, f.parent.inode, f.parent.name)

	return f, nil
}

// Read to read the data of file by offset and length
func (f *File) Read(ctx context.Context, req *bfuse.ReadRequest, resp *bfuse.ReadResponse) error {

	f.mu.Lock()
	defer f.mu.Unlock()

	logger.Debug("Read start : name %v ,inode %v, pinode %v pname %v", f.name, f.inode, f.parent.inode, f.parent.name)

	if req.Offset == f.cfile.FileSizeInCache {

		logger.Debug("Request Read file offset equal filesize")
		return nil
	}

	length := f.cfile.Read(&resp.Data, req.Offset, int64(req.Size))
	if length != int64(req.Size) {
		logger.Debug("== Read reqsize:%v, but return datasize:%v ==\n", req.Size, length)
	}
	if length < 0 {
		logger.Error("Request Read file I/O Error(return data from cfs less than zero)")
		return bfuse.Errno(syscall.EIO)
	}
	return nil
}

// Write to write data to the file
func (f *File) Write(ctx context.Context, req *bfuse.WriteRequest, resp *bfuse.WriteResponse) error {

	f.mu.Lock()
	defer f.mu.Unlock()

	logger.Debug("Write start : name %v ,inode %v, pinode %v pname %v", f.name, f.inode, f.parent.inode, f.parent.name)

	w := f.cfile.Write(req.Data, req.Offset, int32(len(req.Data)))
	if w != int32(len(req.Data)) {
		if w == -1 {
			logger.Error("Write Failed Err:ENOSPC")
			return bfuse.Errno(syscall.ENOSPC)
		}
		logger.Error("Write Failed Err:EIO")
		return bfuse.Errno(syscall.EIO)

	}
	resp.Size = int(w)

	return nil
}

// Flush to sync the file
func (f *File) Flush(ctx context.Context, req *bfuse.FlushRequest) error {

	logger.Debug("Flush start : name %v ,inode %v, pinode %v pname %v", f.name, f.inode, f.parent.inode, f.parent.name)

	f.mu.Lock()
	defer f.mu.Unlock()

	if ret := f.cfile.Flush(); ret != 0 {
		logger.Error("Flush Flush err ...")
		return bfuse.Errno(syscall.EIO)
	}

	return nil
}

// Fsync to sync the file
func (f *File) Fsync(ctx context.Context, req *bfuse.FsyncRequest) error {

	logger.Debug("Fsync start : name %v ,inode %v, pinode %v pname %v", f.name, f.inode, f.parent.inode, f.parent.name)

	f.mu.Lock()
	defer f.mu.Unlock()

	if ret := f.cfile.Flush(); ret != 0 {
		logger.Error("Fsync Flush err ...")
		return bfuse.Errno(syscall.EIO)
	}

	logger.Debug("Fsync end : name %v ,inode %v, pinode %v pname %v", f.name, f.inode, f.parent.inode, f.parent.name)

	return nil
}

// Release to release the file
func (f *File) Release(ctx context.Context, req *bfuse.ReleaseRequest) error {

	logger.Debug("Release start : name %v pinode %v pname %v", f.name, f.parent.inode, f.parent.name)

	f.mu.Lock()
	defer f.mu.Unlock()

	var err error
	if int(req.Flags)&os.O_WRONLY != 0 || int(req.Flags)&os.O_RDWR != 0 {
		f.writers--
		if ret := f.cfile.CloseWrite(); ret != 0 {
			logger.Error("Release CloseWrite err ...")
			err = bfuse.Errno(syscall.EIO)
		}
	}
	f.handles--
	if f.handles == 0 {
		f.cfile.Close()
		f.cfile = nil
	}
	logger.Debug("Release end : name %v pinode %v pname %v", f.name, f.parent.inode, f.parent.name)

	return err
}

// Setattr to set attributes of the file
func (f *File) Setattr(ctx context.Context, req *bfuse.SetattrRequest, resp *bfuse.SetattrResponse) error {
	logger.Debug("File Setattr  ...")
	return nil
}

// Readlink to read symlink
func (f *File) Readlink(ctx context.Context, req *bfuse.ReadlinkRequest) (string, error) {

	logger.Debug("ReadLink ...")

	ret, target := f.parent.fs.cfs.ReadLink(f.inode)
	if ret != 0 {
		logger.Error("ReadLink req ret %v", ret)
		return "", bfuse.EIO
	}

	return target, nil

}
