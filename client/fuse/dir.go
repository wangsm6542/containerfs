// Copyright (c) 2017, TIG All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

package fuse

import (
	"os"
	"sync"
	"syscall"
	"time"

	bfuse "bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/utils"
	"golang.org/x/net/context"
)

// dir to store directory infos
type dir struct {
	inode  uint64
	parent *dir
	fs     *FS
	mu     sync.Mutex
	name   string
	active map[string]*refcount
}

// refcount to manage structure reference
type refcount struct {
	node   node
	kernel bool
	refs   uint32
}

// newDir to create a new directory
func newDir(filesys *FS, inode uint64, parent *dir, name string) *dir {
	d := &dir{
		inode:  inode,
		name:   name,
		parent: parent,
		fs:     filesys,
		active: make(map[string]*refcount),
	}
	return d
}

var _ node = (*dir)(nil)
var _ fs.Node = (*dir)(nil)
var _ fs.NodeCreater = (*dir)(nil)
var _ fs.NodeForgetter = (*dir)(nil)
var _ fs.NodeMkdirer = (*dir)(nil)
var _ fs.NodeRemover = (*dir)(nil)
var _ fs.NodeRenamer = (*dir)(nil)
var _ fs.NodeFsyncer = (*dir)(nil)
var _ fs.NodeSymlinker = (*dir)(nil)
var _ fs.NodeRequestLookuper = (*dir)(nil)
var _ fs.HandleReadDirAller = (*dir)(nil)

// setName to set name of the directory
func (d *dir) setName(name string) {

	d.mu.Lock()
	d.name = name
	d.mu.Unlock()

}

// setParentInode to set parent inode of the directory
func (d *dir) setParentInode(pdir *dir) {

	d.mu.Lock()
	defer d.mu.Unlock()
	d.parent = pdir
}

// Attr to get directory attributes infos
func (d *dir) Attr(ctx context.Context, a *bfuse.Attr) error {

	logger.Debug("Dir Attr")

	a.Mode = os.ModeDir | 0755
	a.Inode = d.inode
	a.Valid = utils.FUSE_ATTR_CACHE_LIFE
	if d.parent != nil {
		logger.Debug("d p inode:%v", d.parent.inode)
		ret, inode, inodeInfo := d.fs.cfs.GetInodeInfoDirect(d.parent.inode, d.name)
		if ret == 0 {
		} else if ret == utils.ENO_NOTEXIST {
			d.mu.Lock()
			//clean dirty cache in dir map
			delete(d.parent.active, d.name)
			d.mu.Unlock()
			return bfuse.Errno(syscall.ENOENT)
		} else {
			return bfuse.Errno(syscall.EIO)
		}
		a.Ctime = time.Unix(inodeInfo.ModifiTime, 0)
		a.Mtime = time.Unix(inodeInfo.ModifiTime, 0)
		a.Atime = time.Unix(inodeInfo.AccessTime, 0)
		a.Inode = uint64(inode)
	}
	return nil
}

// Lookup to lookup file indoe by path
func (d *dir) Lookup(ctx context.Context, req *bfuse.LookupRequest, resp *bfuse.LookupResponse) (fs.Node, error) {

	d.mu.Lock()
	defer d.mu.Unlock()

	logger.Debug("Dir Lookup %v in dir %v", req.Name, d.name)
	resp.EntryValid = utils.FUSE_LOOKUP_CACHE_LIFE

	if utils.FUSE_LOOKUP_CACHE_LIFE != 0*time.Second {
		if a, ok := d.active[req.Name]; ok {
			return a.node, nil
		}
	}

	ret, inodeType, inode := d.fs.cfs.StatDirect(d.inode, req.Name)
	if ret != 0 {
		return nil, bfuse.ENOENT
	}
	n, _ := d.reviveNode(inodeType, inode, req.Name)

	a := &refcount{node: n}
	d.active[req.Name] = a

	a.kernel = true

	return a.node, nil
}

// reviveDir to revive the directory
func (d *dir) reviveDir(inode uint64, name string) (*dir, error) {
	child := newDir(d.fs, inode, d, name)
	return child, nil
}

// reviveNode to revive the inode
func (d *dir) reviveNode(inodeType uint32, inode uint64, name string) (node, error) {
	if inodeType == utils.INODE_DIR {
		child, _ := d.reviveDir(inode, name)
		return child, nil
	}
	child := &File{
		inode:    inode,
		name:     name,
		parent:   d,
		fileType: inodeType,
	}
	return child, nil
}

// ReadDirAll to read all items of the directory
func (d *dir) ReadDirAll(ctx context.Context) ([]bfuse.Dirent, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	logger.Debug("Dir ReadDirAll")

	var res []bfuse.Dirent

	ret, dirents := d.fs.cfs.ListDirect(d.inode, d.name)

	if ret == utils.ENO_NOTEXIST {
		if d.parent != nil {
			//clean dirty cache in dir map
			delete(d.parent.active, d.name)
		}
		return nil, bfuse.Errno(syscall.EPERM)
	} else if ret == 2 || ret == utils.ENO_NOENT {
		return nil, bfuse.Errno(syscall.ENOENT)
	} else if ret != 0 {
		return nil, bfuse.Errno(syscall.EIO)
	}

	for _, v := range dirents {
		de := bfuse.Dirent{
			Name: v.Name,
		}
		if v.InodeType == utils.INODE_DIR {
			de.Type = bfuse.DT_Dir
		} else if v.InodeType == utils.INODE_FILE {
			de.Type = bfuse.DT_File
		} else if v.InodeType == utils.INODE_SYMLINK {
			de.Type = bfuse.DT_Link
		}
		res = append(res, de)
	}

	return res, nil
}

// Create to create a new file under the directory
func (d *dir) Create(ctx context.Context, req *bfuse.CreateRequest, resp *bfuse.CreateResponse) (fs.Node, fs.Handle, error) {
	logger.Debug("Create file start ,  name %v parentino %v parentname %v", req.Name, d.inode, d.name)

	d.mu.Lock()
	defer d.mu.Unlock()

	/*
		if a, ok := d.active[req.Name]; ok {
			logger.Info("asked to create with existing node: %q %#v", req.Name, a.node)
			return a.node, a.node, nil
		}
	*/

	logger.Debug("Create file get locker ,  name %v parentino %v parentname %v", req.Name, d.inode, d.name)

	ret, cfile := d.fs.cfs.CreateFileDirect(d.inode, req.Name, int(req.Flags))
	if ret != 0 {
		if ret == 17 {
			return nil, nil, bfuse.Errno(syscall.EEXIST)

		}
		return nil, nil, bfuse.Errno(syscall.EIO)

	}

	child := &File{
		inode:    cfile.Inode,
		name:     req.Name,
		parent:   d,
		handles:  1,
		writers:  1,
		cfile:    cfile,
		fileType: utils.INODE_FILE,
	}

	d.active[req.Name] = &refcount{node: child}

	logger.Debug("Create file end , inode %v name %v parentino %v parentname %v", cfile.Inode, req.Name, d.inode, d.name)

	return child, child, nil
}

// forgetChild to forget the child of the directory
func (d *dir) forgetChild(name string, child node) {
	if name == "" {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	a, ok := d.active[name]
	if !ok {
		return
	}

	a.kernel = false
	if a.refs == 0 {
		delete(d.active, name)
	}
}

// Forget to forget the directory
func (d *dir) Forget() {
	logger.Debug("Forget dir %v inode %v", d.name, d.inode)
	if d.parent == nil {
		return
	}

	d.mu.Lock()
	name := d.name
	d.mu.Unlock()

	d.parent.forgetChild(name, d)
}

// Mkdir to make a new directory
func (d *dir) Mkdir(ctx context.Context, req *bfuse.MkdirRequest) (fs.Node, error) {

	logger.Debug("Mkdir start ,  name %v parentino %v parentname %v", req.Name, d.inode, d.name)

	d.mu.Lock()
	defer d.mu.Unlock()

	//logger.Debug("Mkdir get locker ,  name %v parentino %v parentname %v", req.Name, d.inode, d.name)

	ret, inode := d.fs.cfs.CreateDirDirect(d.inode, req.Name)
	if ret == -1 {
		return nil, bfuse.Errno(syscall.EIO)
	}
	if ret == 1 {
		return nil, bfuse.Errno(syscall.EPERM)
	}
	if ret == 2 {
		return nil, bfuse.Errno(syscall.ENOENT)
	}
	if ret == 17 {
		return nil, bfuse.Errno(syscall.EEXIST)
	}
	if ret == utils.ENO_NOTEXIST {
		return nil, bfuse.Errno(syscall.EIO)
	}

	logger.Debug("Mkdir end , inode %v name %v parentino %v parentname %v", inode, req.Name, d.inode, d.name)

	child := newDir(d.fs, inode, d, req.Name)

	d.active[req.Name] = &refcount{node: child, kernel: true}

	return child, nil
}

// Remove to remove the directory
func (d *dir) Remove(ctx context.Context, req *bfuse.RemoveRequest) error {

	logger.Debug("Remove start , name %v parentino %v parentname %v", req.Name, d.inode, d.name)

	d.mu.Lock()
	defer d.mu.Unlock()

	if a, ok := d.active[req.Name]; ok {
		delete(d.active, req.Name)
		a.node.setName("")
	}

	if req.Dir {
		ret := d.fs.cfs.DeleteDirDirect(d.inode, req.Name)
		if ret != 0 {
			if ret == 2 {
				return bfuse.Errno(syscall.EPERM)
			}
			return bfuse.Errno(syscall.EIO)
		}
	} else {

		var symlimkFlag bool

		if node, ok := d.active[req.Name]; ok {
			if node.node.(*File).fileType == utils.INODE_SYMLINK {
				logger.Debug("symlink file in active ...")
				symlimkFlag = true
			}
		} else {
			ret, inodeType, _ := d.fs.cfs.StatDirect(d.inode, req.Name)
			if ret == 0 {
				if inodeType == utils.INODE_SYMLINK {
					symlimkFlag = true
				}
			} else {
				return bfuse.Errno(syscall.EIO)
			}

		}

		if symlimkFlag {

			ret := d.fs.cfs.DeleteSymLinkDirect(d.inode, req.Name)

			logger.Debug("symlink DeleteSymLinkDirect ret  %v", ret)

			if ret != 0 {
				if ret == 2 {
					return bfuse.Errno(syscall.EPERM)
				} else if ret != utils.ENO_NOTEXIST {
					return bfuse.Errno(syscall.EIO)
				}
				return nil
			}

		} else {

			ret := d.fs.cfs.DeleteFileDirect(d.inode, req.Name)
			if ret != 0 {
				if ret == 2 {
					return bfuse.Errno(syscall.EPERM)
				} else if ret != utils.ENO_NOTEXIST {
					return bfuse.Errno(syscall.EIO)
				}
				return nil
			}

		}

	}

	logger.Debug("Remove end , name %v parentino %v parentname %v", req.Name, d.inode, d.name)

	return nil
}

// Rename to rename the directory
func (d *dir) Rename(ctx context.Context, req *bfuse.RenameRequest, newDir fs.Node) error {

	logger.Debug("Rename start d.inode %v, req.OldName %v, newDir.(*dir).inode %v , req.NewName %v", d.inode, req.OldName, newDir.(*dir).inode, req.NewName)

	d.mu.Lock()
	defer d.mu.Unlock()

	ret, inodeType, _ := d.fs.cfs.StatDirect(newDir.(*dir).inode, req.NewName)
	if ret == 0 {
		logger.Debug("newName in newDir is already exsit, inodeType: %v", inodeType)
		if utils.INODE_DIR == inodeType {
			logger.Error("Rename newName %v in newDir %v is an exsit dir, un-supportted rename", req.NewName, d.name)
			return bfuse.Errno(syscall.EPERM)
		} else if utils.INODE_FILE == inodeType {
			ret = d.fs.cfs.DeleteFileDirect(newDir.(*dir).inode, req.NewName)
			if ret != 0 {
				logger.Error("Rename Delete the exist newName %v in newDir %v failed!", req.NewName, d.name)
				return bfuse.Errno(syscall.EPERM)
			}
		} else if utils.INODE_SYMLINK == inodeType {
			ret = d.fs.cfs.DeleteSymLinkDirect(newDir.(*dir).inode, req.NewName)
			if ret != 0 {
				logger.Error("Rename Delete the exist newName %v in newDir %v failed!", req.NewName, d.name)
				return bfuse.Errno(syscall.EPERM)
			}
		}

	}

	if newDir != d {

		logger.Debug("Rename d.inode %v, req.OldName %v, newDir.(*dir).inode %v , req.NewName %v", d.inode, req.OldName, newDir.(*dir).inode, req.NewName)

		ret := d.fs.cfs.RenameDirect(d.inode, req.OldName, newDir.(*dir).inode, req.NewName)
		if ret != 0 {
			if ret == 1 || ret == 17 {
				return bfuse.Errno(syscall.EPERM)
			} else if ret != 2 {
				return bfuse.Errno(syscall.EIO)
			}
			return bfuse.Errno(syscall.ENOENT)
		}

		if aOld, ok := d.active[req.OldName]; ok {
			delete(d.active, req.OldName)
			aOld.node.setName(req.NewName)
			aOld.node.setParentInode(newDir.(*dir))
			newDir.(*dir).active[req.NewName] = aOld
		}

	} else {

		logger.Debug("Rename d.inode %v, req.OldName %v, newDir.(*dir).inode %v , req.NewName %v", d.inode, req.OldName, newDir.(*dir).inode, req.NewName)

		ret := d.fs.cfs.RenameDirect(d.inode, req.OldName, d.inode, req.NewName)
		if ret != 0 {
			if ret == 2 {
				return bfuse.Errno(syscall.ENOENT)
			} else if ret == 1 || ret == 17 {
				return bfuse.Errno(syscall.EPERM)
			} else if ret != utils.ENO_NOTEXIST {
				return bfuse.Errno(syscall.EIO)
			}
			delete(d.active, req.OldName)
			return bfuse.Errno(syscall.ENOENT)
		}

		if a, ok := d.active[req.NewName]; ok {
			a.node.setName("")
		}

		if aOld, ok := d.active[req.OldName]; ok {
			aOld.node.setName(req.NewName)
			delete(d.active, req.OldName)
			d.active[req.NewName] = aOld
		}
	}

	logger.Debug("Rename end d.inode %v, req.OldName %v, newDir.(*dir).inode %v , req.NewName %v", d.inode, req.OldName, newDir.(*dir).inode, req.NewName)

	return nil
}

// Symlink to create a new symlink under the diretory
func (d *dir) Symlink(ctx context.Context, req *bfuse.SymlinkRequest) (fs.Node, error) {

	logger.Debug("Symlink req %v", req)

	ret, inode := d.fs.cfs.SymLink(d.inode, req.NewName, req.Target)
	if ret != 0 {
		logger.Error("Symlink ret %v,pinode %v,newname %v,target %v", ret, d.inode, req.NewName, req.Target)
		return nil, bfuse.EPERM
	}

	child := &File{
		inode:    inode,
		name:     req.NewName,
		parent:   d,
		handles:  1,
		writers:  1,
		fileType: utils.INODE_SYMLINK,
	}

	d.active[req.NewName] = &refcount{node: child}

	return child, nil
}

// Fsync .to sync the directory
func (d *dir) Fsync(ctx context.Context, req *bfuse.FsyncRequest) error {

	return nil
}

type node interface {
	fs.Node
	setName(name string)
	setParentInode(pdir *dir)
}
