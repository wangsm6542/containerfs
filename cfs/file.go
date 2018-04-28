// Copyright (c) 2017, TIG All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

package cfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/proto/dp"
	"github.com/tiglabs/containerfs/proto/mp"
	"github.com/tiglabs/containerfs/utils"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// FILE status
const (
	FILE_NORMAL    = 0
	FILE_ERROR     = 2
	FILE_NOT_EXIST = 3

	WRITE_RETRY_CNT = 5
)

// BufferSize ...
var BufferSize int32

// WriteBufferSize ...
var WriteBufferSize int

// Data is a buffer to store bytes writing to Datanode
type Data struct {
	DataBuf *bytes.Buffer
	Status  int32
	timer   *time.Timer
	ID      uint64
}

// ReadCache is a buffer cache to store bytes readed from Datanode
type ReadCache struct {
	LastOffset int64
	readBuf    []byte
	Ch         chan *bytes.Buffer
}

// wBuffer is a buffer to merge multi buffers of write ops
type wBuffer struct {
	freeSize    int32               // chunk size
	chunkInfo   *mp.ChunkInfoWithBG // chunk info
	buffer      *bytes.Buffer       // chunk data
	startOffset int64
	endOffset   int64
}

// chanData is a channel for asynchronous writing
type chanData struct {
	flushFlag bool
	data      []byte
}

// Chunk to store state of current writing chunk
type Chunk struct {
	CFile                    *CFile
	ChunkFreeSize            int
	ChunkInfo                *mp.ChunkInfoWithBG
	ChunkWriteStream         dp.DataNode_C2MReplClient
	ChunkWriteRecvExitSignal chan struct{}
}

// CFile to store infos and states of an opening file
type CFile struct {
	cfs           *CFS
	ParentInodeID uint64
	Name          string
	Inode         uint64

	OpenFlag        int
	FileSize        int64
	FileSizeInCache int64
	Status          int32

	DataConnLocker sync.RWMutex
	DataConn       map[string]*grpc.ClientConn

	// for write
	wBuffer          wBuffer
	wgWriteReps      sync.WaitGroup
	atomicNum        uint64
	curNum           uint64
	Writer           int32
	DataCacheLocker  sync.RWMutex
	DataCache        map[uint64]*Data
	DataQueue        chan *chanData
	WriteErrSignal   chan bool
	WriteRetrySignal chan bool

	isWrite         bool
	convergeBuffer  *bytes.Buffer
	convergeTimer   *time.Timer
	convergeLocker  sync.Mutex
	convergeFlushCh chan *struct{}
	Closing         bool
	CloseSignal     chan struct{}
	FlushSignal     chan struct{}

	CurChunk *Chunk

	WriteLocker sync.Mutex

	// for read
	RMutex           sync.Mutex
	chunks           []*mp.ChunkInfoWithBG // chunkinfo
	readCache        ReadCache
	errDataNodeCache map[string]bool
}

// extentInfo to describe the extents of an write op
type extentInfo struct {
	pos    int32 //pos in chunks of cfile
	offset int32 //offset in chunk
	length int32 //length in chunk
}

// newDataConn provides new connecting to Datanode by addr, and cache of the connection
func (cfile *CFile) newDataConn(addr string) *grpc.ClientConn {

	cfile.DataConnLocker.RLock()
	if v, ok := cfile.DataConn[addr]; ok {
		cfile.DataConnLocker.RUnlock()
		return v
	}
	cfile.DataConnLocker.RUnlock()

	conn, err := utils.Dial(addr)
	if err != nil || conn == nil {
		logger.Error("Dial to %v failed! err: %v", addr, err)
		return nil
	}

	cfile.DataConnLocker.Lock()
	if v, ok := cfile.DataConn[addr]; ok {
		cfile.DataConnLocker.Unlock()
		conn.Close()
		return v
	}
	cfile.DataConn[addr] = conn
	cfile.DataConnLocker.Unlock()
	return conn
}

// delErrDataConn to close and delete the connection when an error occur
func (cfile *CFile) delErrDataConn(addr string) {
	cfile.DataConnLocker.Lock()
	if v, ok := cfile.DataConn[addr]; ok {
		v.Close()
		delete(cfile.DataConn, addr)
	}
	cfile.DataConnLocker.Unlock()
}

// delAllDataConn to delele all connection when closing the file
func (cfile *CFile) delAllDataConn() {
	cfile.DataConnLocker.Lock()
	for k, v := range cfile.DataConn {
		v.Close()
		delete(cfile.DataConn, k)
	}
	cfile.DataConnLocker.Unlock()
}

// streamRead to submit stream read request and read file data from the stream
func (cfile *CFile) streamRead(chunkidx int, ch chan *bytes.Buffer, offset int64, size int64) {
	var conn *grpc.ClientConn
	var buffer *bytes.Buffer
	outflag := 0
	inflag := 0
	copies := cfile.cfs.Copies
	idxs := utils.GenerateRandomNumber(0, copies, copies)

	for n := 0; n < copies; n++ {
		i := idxs[n]
		addr := cfile.chunks[chunkidx].BlockGroupWithHost.Hosts[i]
		_, ok := cfile.errDataNodeCache[addr]
		if !ok {
			if n != 0 {
				tmp := idxs[0]
				idxs[0] = i
				idxs[n] = tmp
			}
			break
		}
	}

	for n := 0; n < copies; n++ {
		i := idxs[n]

		buffer = new(bytes.Buffer)

		addr := cfile.chunks[chunkidx].BlockGroupWithHost.Hosts[i]

		conn = cfile.newDataConn(addr)
		if conn == nil {
			cfile.errDataNodeCache[addr] = true
			outflag++
			continue
		}

		dc := dp.NewDataNodeClient(conn)
		streamreadChunkReq := &dp.StreamReadChunkReq{
			ChunkID:      cfile.chunks[chunkidx].ChunkID,
			BlockGroupID: cfile.chunks[chunkidx].BlockGroupWithHost.BlockGroupID,
			Offset:       offset,
			Readsize:     size,
		}
		ctx, _ := context.WithTimeout(context.Background(), DATANODE_TIMEOUT_SECONDS*time.Second)
		stream, err := dc.StreamReadChunk(ctx, streamreadChunkReq)
		if err != nil {
			cfile.delErrDataConn(addr)
			conn = cfile.newDataConn(addr)
			if conn == nil {
				logger.Error("StreamReadChunk return error:%v and re-dial failed, so retry other datanode!", err)
				cfile.errDataNodeCache[addr] = true
				outflag++
				continue
			} else {
				dc = dp.NewDataNodeClient(conn)
				streamreadChunkReq := &dp.StreamReadChunkReq{
					ChunkID:      cfile.chunks[chunkidx].ChunkID,
					BlockGroupID: cfile.chunks[chunkidx].BlockGroupWithHost.BlockGroupID,
					Offset:       offset,
					Readsize:     size,
				}
				ctx, _ = context.WithTimeout(context.Background(), DATANODE_TIMEOUT_SECONDS*time.Second)
				stream, err = dc.StreamReadChunk(ctx, streamreadChunkReq)
				if err != nil {
					cfile.delErrDataConn(addr)
					logger.Error("StreamReadChunk StreamReadChunk error:%v, so retry other datanode!", err)
					cfile.errDataNodeCache[addr] = true
					outflag++
					continue
				}

			}

		}

		delete(cfile.errDataNodeCache, addr)

		for {
			ack, err := stream.Recv()

			if err == io.EOF {
				break
			}
			if err != nil {
				logger.Error("=== streamreadChunkReq Recv err:%v ===", err)
				inflag++
				outflag++
				break
			}
			if ack != nil {
				if len(ack.Databuf) == 0 {
					continue
				} else {
					buffer.Write(ack.Databuf)
					inflag = 0
				}
			} else {
				continue
			}

		}

		if inflag == 0 {
			ch <- buffer
			break
		} else if inflag == copies {
			buffer = new(bytes.Buffer)
			buffer.Write([]byte{})
			logger.Error("Stream Read the chunk three copy Recv error")
			ch <- buffer
			break
		} else if inflag < copies {
			logger.Error("Stream Read the chunk %v copy Recv error, so need retry other datanode!!!", inflag)
			continue
		}
	}
	if outflag >= copies {
		buffer = new(bytes.Buffer)
		buffer.Write([]byte{})
		logger.Error("Stream Read the chunk three copy Datanode error")
		ch <- buffer
	}
}

// readChunk to read chunk data by streamRead, the read offset of the chunk and length are desceibed by extentInfo
func (cfile *CFile) readChunk(eInfo extentInfo, data *[]byte, offset int64) int32 {

	//check if hit readBuf
	readBufOffset := cfile.readCache.LastOffset
	readBufLen := len(cfile.readCache.readBuf)
	if offset >= readBufOffset && offset+int64(eInfo.length) <= readBufOffset+int64(readBufLen) {
		pos := int32(offset - readBufOffset)
		*data = append(*data, cfile.readCache.readBuf[pos:pos+eInfo.length]...)

		logger.Debug("cfile %v hit read buffer, offset:%v len:%v, readBuf offset:%v, len:%v", cfile.Name, offset, eInfo.length, readBufOffset, readBufLen)
		return eInfo.length
	}

	//prepare to read from datanode
	cfile.readCache.readBuf = []byte{}
	buffer := new(bytes.Buffer)
	cfile.readCache.Ch = make(chan *bytes.Buffer)
	readSize := eInfo.length
	if readSize < int32(BufferSize) {
		readSize = int32(BufferSize)
	}

	//go streamRead
	go cfile.streamRead(int(eInfo.pos), cfile.readCache.Ch, int64(eInfo.offset), int64(readSize))
	buffer = <-cfile.readCache.Ch
	bLen := buffer.Len()
	if bLen == 0 {
		logger.Error("try to read %v chunk:%v from datanode size:%v, but return:%v", cfile.Name, eInfo.pos, readSize, bLen)
		return -1
	}
	cfile.readCache.readBuf = buffer.Next(bLen)
	cfile.readCache.LastOffset = offset
	appendLen := eInfo.length
	if appendLen > int32(bLen) {
		appendLen = int32(bLen)
	}
	*data = append(*data, cfile.readCache.readBuf[0:appendLen]...)
	buffer.Reset()
	buffer = nil
	return appendLen
}

// disableReadCache to disable all readed data caches for the file
func (cfile *CFile) disableReadCache(wOffset int64, wLen int32) {
	readBufOffset := cfile.readCache.LastOffset
	readBufLen := len(cfile.readCache.readBuf)
	if readBufLen == 0 {
		return
	}

	if wOffset >= readBufOffset+int64(readBufLen) || wOffset+int64(wLen) <= readBufOffset {
		return
	}

	//we need disable read buffer here
	cfile.readCache.readBuf = []byte{}
	logger.Debug("cfile %v disableReadCache: offset: %v len %v --> %v", cfile.Name, readBufOffset, readBufLen, len(cfile.readCache.readBuf))
}

// getExtentInfo to get extent info by [start, end) for a write/read operation
func (cfile *CFile) getExtentInfo(start int64, end int64, eInfo *[]extentInfo) {
	var i int32
	var chunkStart, chunkEnd int64
	var tmpInfo extentInfo

	for i = 0; i < int32(len(cfile.chunks)) && start < end; i++ {
		chunkEnd += int64(cfile.chunks[i].ChunkSize) //@chunkEnd is next chunk's @chunkStart

		if start < chunkEnd {
			tmpInfo.pos = i
			tmpInfo.offset = int32(start - chunkStart)
			if chunkEnd < end {
				tmpInfo.length = int32(chunkEnd - start)
				start = chunkEnd //update @start to next chunk
			} else {
				tmpInfo.length = int32(end - start)
				start = end
			}
			*eInfo = append(*eInfo, tmpInfo)
		}
		chunkStart = chunkEnd
	}
}

func (cfile *CFile) waitWriteCache(end int64) {
	var i int
	logger.Debug("cfile %v, start to wait append write..FileSize %v, FileSizeInCache %v", cfile.Name, cfile.FileSize, cfile.FileSizeInCache)
	for i = 0; i < 10; i++ {
		if cfile.FileSize >= end || cfile.FileSize == cfile.FileSizeInCache {
			break
		}
		if cfile.isWrite && cfile.convergeBuffer.Len() > 0 {
			cfile.convergeFlushCh <- &struct{}{}
		}
		if len(cfile.DataCache) == 0 {
			logger.Debug("cfile %v, FileSize %v, FileSizeInCache %v, but no DataCache", cfile.Name, cfile.FileSize, cfile.FileSizeInCache)
		}
		time.Sleep(40 * time.Millisecond)
	}
	logger.Debug("cfile %v, end waiting with FileSize %v, FileSizeInCache %v, time %v ms", cfile.Name, cfile.FileSize, cfile.FileSizeInCache, i*100)
}

// Read provides the API to read datas from the file
func (cfile *CFile) Read(data *[]byte, offset int64, readsize int64) int64 {

	if cfile.Status != FILE_NORMAL {
		logger.Error("cfile %v status error , read func return -2 ", cfile.Name)
		return -2
	}

	if offset == cfile.FileSizeInCache {
		logger.Debug("cfile:%v read offset:%v  equals file size in cache ", cfile.Name, offset)
		return 0
	} else if offset > cfile.FileSizeInCache {
		logger.Debug("cfile %v unsupport read beyond file size, offset:%v, filesize in cache:%v ", cfile.Name, offset, cfile.FileSizeInCache)
		return 0
	}

	var ret int32
	var doneFlag bool
	start := offset
	end := offset + readsize

	logger.Debug("cfile %v Read start: offset: %v, len: %v", cfile.Name, offset, readsize)

	for start < end && cfile.Status == FILE_NORMAL {

		eInfo := make([]extentInfo, 0, 4)
		cfile.getExtentInfo(start, end, &eInfo)
		logger.Debug("cfile %v getExtentInfo: offset: %v, len: %v, eInfo: %v", cfile.Name, start, end, eInfo)

		for _, ei := range eInfo {
			ret = cfile.readChunk(ei, data, start)
			if ret != ei.length {
				logger.Error("cfile %v eInfo:%v, readChunk ret %v", cfile.Name, ei, ret)
				doneFlag = true
				break
			}
			start += int64(ret)
		}

		if doneFlag || start == end || start >= cfile.FileSizeInCache {
			break
		}

		//wait append write request in caches
		cfile.waitWriteCache(end)
	}

	if cfile.Status != FILE_NORMAL {
		logger.Error("cfile %v status error , read func return -2 ", cfile.Name)
		return -2
	}

	logger.Debug("cfile %v Read end: return %v", cfile.Name, start-offset)
	return start - offset
}

// Write provides the API to write datas to the file
func (cfile *CFile) Write(buf []byte, offset int64, length int32) int32 {

	if cfile.Status != 0 {
		logger.Error("cfile %v status error , Write func return -2 ", cfile.Name)
		return -2
	}

	if offset > cfile.FileSizeInCache {
		logger.Error("cfile %v unsupport write %v beyond file size %v return -3 ", cfile.Name, offset, cfile.FileSizeInCache)
		return -3
	}

	if offset == cfile.FileSizeInCache {
		logger.Debug("cfile %v write append only: offset %v, length %v", cfile.Name, offset, length)
		return cfile.appendWrite(buf, length, false)
	}

	cfile.disableReadCache(offset, length)

	var i int
	var ret, pos int32
	start := offset
	end := offset + int64(length)

	logger.Debug("cfile %v write start: offset: %v, len: %v", cfile.Name, offset, length)

	for start < end && cfile.Status == FILE_NORMAL {

		eInfo := make([]extentInfo, 0, 4)
		cfile.getExtentInfo(start, end, &eInfo)
		logger.Debug("cfile %v getExtentInfo: offset: %v, len: %v, eInfo: %v", cfile.Name, start, end, eInfo)

		for _, ei := range eInfo {
			ret = cfile.seekWrite(ei, buf[pos:(pos+ei.length)])
			if ret < 0 {
				logger.Error("cfile %v seekWrite failed %v", cfile.Name, ei)
				return int32(start - offset)
			}
			start += int64(ei.length)
			pos += ei.length
		}

		if start == end {
			break
		}

		if start == cfile.FileSizeInCache {
			logger.Debug("cfile %v write append only: offset %v, length %v", cfile.Name, start, length-pos)
			ret = cfile.appendWrite(buf[pos:length], length-pos, false)
			if ret < 0 {
				logger.Error("cfile %v appendWrite failed %v", cfile.Name, ret)
				return int32(start - offset)
			}
			start = end
			break
		}

		//wait append write request in caches
		logger.Debug("cfile %v, start to wait append write..FileSize %v, FileSizeInCache %v", cfile.Name, cfile.FileSize, cfile.FileSizeInCache)
		for i = 0; i < 10; i++ {
			if cfile.FileSize >= end || cfile.FileSize == cfile.FileSizeInCache {
				break
			}
			if len(cfile.DataCache) == 0 {
				logger.Debug("cfile %v, FileSize %v, FileSizeInCache %v, but no DataCache", cfile.Name, cfile.FileSize, cfile.FileSizeInCache)
			}
			time.Sleep(100 * time.Millisecond)
		}
		logger.Debug("cfile %v, end waiting with FileSize %v, FileSizeInCache %v, time %v ms", cfile.Name, cfile.FileSize, cfile.FileSizeInCache, i*100)
	}

	logger.Debug("cfile %v Write end: return %v", cfile.Name, start-offset)
	return int32(start - offset)
}

// overwriteBuffer to seek and overwrite the wBuffer of file
func (cfile *CFile) overwriteBuffer(eInfo extentInfo, buf []byte) int32 {

	//read wBuffer all bytes to tmpBuf
	bufLen := cfile.wBuffer.buffer.Len()
	tmpBuf := cfile.wBuffer.buffer.Next(bufLen)
	if len(tmpBuf) != bufLen {
		logger.Error("cfile %v read wBuffer len: %v return: %v ", cfile.Name, bufLen, len(tmpBuf))
		return -1
	}

	//copy buf to tmpBuf
	n := copy(tmpBuf[eInfo.offset:], buf)
	if n != int(eInfo.length) {
		logger.Error("cfile %v copy to wBuffer len: %v return n: %v", cfile.Name, eInfo.length, n)
		return -1
	}

	//write to wBuffer
	cfile.wBuffer.buffer.Reset()
	n, err := cfile.wBuffer.buffer.Write(tmpBuf)
	if n != int(bufLen) || err != nil {
		logger.Error("cfile %v write wBuffer len: %v return n: %v err %v", cfile.Name, bufLen, n, err)
		return -1
	}

	return 0
}

// seekWriteChunk to seek and write datas of chunk to Datanode
func (cfile *CFile) seekWriteChunk(addr string, conn *grpc.ClientConn, req *dp.SeekWriteChunkReq, copies *uint64) {

	if conn == nil {
	} else {
		dc := dp.NewDataNodeClient(conn)
		ctx, _ := context.WithTimeout(context.Background(), METANODE_TIMEOUT_SECONDS*time.Second)
		ret, err := dc.SeekWriteChunk(ctx, req)
		if err != nil {
			cfile.delErrDataConn(addr)
			logger.Error("SeekWriteChunk err %v", err)
		} else {
			if ret.Ret != 0 {
			} else {
				atomic.AddUint64(copies, 1)
			}
		}
	}
	cfile.wgWriteReps.Add(-1)

}

// seekWrite provides seek and write the file
func (cfile *CFile) seekWrite(eInfo extentInfo, buf []byte) int32 {

	chunkInfo := cfile.chunks[eInfo.pos]
	var copies uint64
	conn := make([]*grpc.ClientConn, 3)

	for i, v := range chunkInfo.BlockGroupWithHost.Hosts {

		conn[i] = cfile.newDataConn(v)
		if conn[i] == nil {
			logger.Error("cfile %v dial %v failed!", cfile.Name, v)
			return -1
		}
	}

	for i, v := range chunkInfo.BlockGroupWithHost.Hosts {

		pSeekWriteChunkReq := &dp.SeekWriteChunkReq{
			ChunkID:      chunkInfo.ChunkID,
			BlockGroupID: chunkInfo.BlockGroupWithHost.BlockGroupID,
			Databuf:      buf,
			ChunkOffset:  int64(eInfo.offset),
		}

		cfile.wgWriteReps.Add(1)

		go cfile.seekWriteChunk(v, conn[i], pSeekWriteChunkReq, &copies)

	}

	cfile.wgWriteReps.Wait()

	if copies < uint64(cfile.cfs.Copies) {
		cfile.Status = FILE_ERROR
		logger.Error("cfile %v seekWriteChunk copies: %v, set error!", cfile.Name, copies)
		return -1
	}
	return 0
}

//startFlushConvergeBuffer is a routine to flush the converge buffer
func (cfile *CFile) startFlushConvergeBuffer() {

	if WriteBufferSize <= 0 {
		return
	}
	timer := time.NewTicker(time.Second / 10)
	for {
		if cfile.Closing == true {
			timer.Stop()
			return
		}
		select {
		case <-timer.C:
			break
		case <-cfile.convergeFlushCh:
			cfile.convergeLocker.Lock()
			if cfile.convergeBuffer.Len() > 0 {
				data := &chanData{}
				data.data = append(data.data, cfile.convergeBuffer.Next(cfile.convergeBuffer.Len())...)
				select {
				case <-cfile.WriteErrSignal:
					logger.Error("Write recv WriteErrSignal ,volumeid %v , pid %v ,fname %v!", cfile.cfs.VolID, cfile.ParentInodeID, cfile.Name)
					return
				case cfile.DataQueue <- data:
				}
			}
			cfile.convergeLocker.Unlock()
		}
	}
}

// appendWrite to write the file append only
func (cfile *CFile) appendWrite(buf []byte, length int32, needFlush bool) (ret int32) {

	if cfile.Status == FILE_ERROR {
		return -2
	}

	cfile.FileSizeInCache += int64(length)

	if WriteBufferSize > 0 {
		return cfile.append2Converge(buf, length, needFlush)
	}

	ret = length

	data := &chanData{flushFlag: needFlush}
	data.data = append(data.data, buf...)
	select {
	case <-cfile.WriteErrSignal:
		logger.Error("Write recv WriteErrSignal ,volumeid %v , pid %v ,fname %v!", cfile.cfs.VolID, cfile.ParentInodeID, cfile.Name)
		ret = -2
	case cfile.DataQueue <- data:
	}

	return ret
}

// append2Converge to write datas to the converge buffer of file
func (cfile *CFile) append2Converge(buf []byte, length int32, needFlush bool) (ret int32) {
	ret = length
	cfile.convergeLocker.Lock()

	if length != 0 {
		cfile.convergeBuffer.Write(buf)
	}

	bufferLen := cfile.convergeBuffer.Len()
	if bufferLen < WriteBufferSize && !needFlush {
		cfile.convergeLocker.Unlock()
		return length
	}

	data := &chanData{flushFlag: needFlush}
	data.data = append(data.data, cfile.convergeBuffer.Next(cfile.convergeBuffer.Len())...)

	select {
	case <-cfile.WriteErrSignal:
		logger.Error("Write recv WriteErrSignal ,volumeid %v , pid %v ,fname %v!", cfile.cfs.VolID, cfile.ParentInodeID, cfile.Name)
		ret = -2
	case cfile.DataQueue <- data:
	}

	cfile.convergeLocker.Unlock()
	return ret
}

// WriteThread is a routine for asynchronous write ops
func (cfile *CFile) WriteThread() {

	logger.Debug("Write Thread: file %v start Writethread!\n", cfile.Name)

	for true {
		select {
		case chanData := <-cfile.DataQueue:
			if chanData != nil {

				if cfile.Status != FILE_NORMAL {
					continue
				}

				if len(chanData.data) != 0 {
					newData := &Data{}
					newData.ID = atomic.AddUint64(&cfile.atomicNum, 1)
					newData.DataBuf = new(bytes.Buffer)
					newData.DataBuf.Write(chanData.data)
					newData.Status = 1

					if err := cfile.writeHandler(newData); err != nil {
						logger.Error("WriteThread file %v writeHandler err %v !", cfile.Name, err)
						cfile.Status = FILE_ERROR
						cfile.WriteErrSignal <- true
					}
				}

				var ti uint32
				if chanData.flushFlag {
					for cfile.Status == FILE_NORMAL {
						if len(cfile.DataCache) == 0 {
							break
						}
						ti++
						time.Sleep(time.Millisecond)
					}
					cfile.FlushSignal <- struct{}{}
				}

			} else {

				var ti uint32
				for cfile.Status == FILE_NORMAL {
					if len(cfile.DataCache) == 0 {
						break
					}
					ti++
					time.Sleep(time.Millisecond)
				}

				if cfile.CurChunk != nil {
					if cfile.CurChunk.ChunkWriteStream != nil {
						cfile.CurChunk.ChunkWriteStream.CloseSend()
					}
				}
				cfile.CloseSignal <- struct{}{}
				return
			}

		}
	}

}

// writeHandler to handle an asynchronous write operation
func (cfile *CFile) writeHandler(newData *Data) error {

	length := newData.DataBuf.Len()

	// logger.Debug("writeHandler: file %v, num:%v,  length: %v, \n", cfile.Name, cfile.atomicNum, length)

ALLOCATECHUNK:

	if cfile.CurChunk != nil && cfile.CurChunk.ChunkFreeSize-length < 0 {

		if cfile.CurChunk.ChunkWriteStream != nil {
			var ti uint32
			needClose := bool(true)
			logger.Debug("writeHandler: file %v, begin waiting last chunk: %v\n", cfile.Name, len(cfile.DataCache))
			tmpDataCacheLen := len(cfile.DataCache)

			for cfile.Status == FILE_NORMAL {
				if tmpDataCacheLen == 0 {
					break
				}
				time.Sleep(time.Millisecond)
				if tmpDataCacheLen == len(cfile.DataCache) {
					ti++
				} else {
					tmpDataCacheLen = len(cfile.DataCache)
					ti = 0
				}

				if ti == 500 {
					if cfile.CurChunk.ChunkWriteStream != nil {
						logger.Error("writeHandler: file %v, dataCacheLen: %v  wait last chunk timeout, CloseSend\n", cfile.Name, len(cfile.DataCache))
						cfile.CurChunk.ChunkWriteStream.CloseSend()
						needClose = false
					}
				}
			}
			if cfile.Status != FILE_NORMAL {
				return errors.New("file status err")
			}
			logger.Debug("writeHandler: file %v, end wait after loop times %v\n", cfile.Name, ti)

			if needClose && cfile.CurChunk.ChunkWriteStream != nil {
				cfile.CurChunk.ChunkWriteStream.CloseSend()
			}
		}

		cfile.CurChunk = nil
	}

	if cfile.CurChunk == nil {
		for retryCnt := 0; retryCnt < WRITE_RETRY_CNT; retryCnt++ {
			cfile.CurChunk = cfile.AllocateChunk(true)
			if cfile.CurChunk == nil {
				logger.Error("writeHandler: file %v, alloc chunk failed for %v times\n", cfile.Name, retryCnt+1)
				time.Sleep(time.Millisecond * 500)
				continue
			}
			break
		}
		if cfile.CurChunk == nil {
			return errors.New("AllocateChunk failed after retry")
		}
	}

	cfile.DataCacheLocker.Lock()
	cfile.DataCache[cfile.atomicNum] = newData
	cfile.DataCacheLocker.Unlock()

	var backup string
	if cfile.cfs.Copies == 3 {
		backup = cfile.CurChunk.ChunkInfo.BlockGroupWithHost.Hosts[2]
	} else {
		backup = ""
	}

	req := &dp.StreamWriteReq{
		ChunkID:      cfile.CurChunk.ChunkInfo.ChunkID,
		Master:       cfile.CurChunk.ChunkInfo.BlockGroupWithHost.Hosts[0],
		Slave:        cfile.CurChunk.ChunkInfo.BlockGroupWithHost.Hosts[1],
		Backup:       backup,
		Databuf:      newData.DataBuf.Bytes(),
		DataLen:      uint32(length),
		CommitID:     cfile.atomicNum,
		BlockGroupID: cfile.CurChunk.ChunkInfo.BlockGroupWithHost.BlockGroupID,
	}

	if cfile.CurChunk != nil {
		if cfile.CurChunk.ChunkWriteStream != nil {
			if err := cfile.CurChunk.ChunkWriteStream.Send(req); err != nil {
				logger.Debug("writeHandler: send file %v, chunk %v len: %v failed\n", cfile.Name, cfile.CurChunk, length)
				cfile.CurChunk.ChunkFreeSize = 0
			} else {
				logger.Debug("writeHandler: send file %v, chunk %v len: %v success\n", cfile.Name, cfile.CurChunk, length)
				cfile.CurChunk.ChunkFreeSize -= length
			}
		} else {
			logger.Error("writeHandler: file %v, CurChunk %v has no write stream\n", cfile.Name, cfile.CurChunk.ChunkInfo.ChunkID)
			goto ALLOCATECHUNK
		}
	} else {
		logger.Error("writeHandler: file %v, CurChunk is nil\n", cfile.Name)
		goto ALLOCATECHUNK
	}

	return nil
}

// AllocateChunk to allocate a chunk for append write to the file
func (cfile *CFile) AllocateChunk(IsStream bool) *Chunk {

	logger.Debug("AllocateChunk file: %v begin\n", cfile.Name)

	ret := cfile.cfs.checkMetaConn()
	if ret != 0 {
		logger.Error("AllocateChunk file: %v failed\n", cfile.Name)
		return nil
	}

	mc := mp.NewMetaNodeClient(cfile.cfs.MetaNodeConn)
	pAllocateChunkReq := &mp.AllocateChunkReq{
		VolID: cfile.cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), METANODE_TIMEOUT_SECONDS*time.Second)
	pAllocateChunkAck, err := mc.AllocateChunk(ctx, pAllocateChunkReq)
	if err != nil || pAllocateChunkAck.Ret != 0 {
		time.Sleep(time.Second * 2)

		ret := cfile.cfs.checkMetaConn()
		if ret != 0 {
			logger.Error("AllocateChunk file: %v failed\n", cfile.Name)
			return nil
		}

		mc = mp.NewMetaNodeClient(cfile.cfs.MetaNodeConn)
		ctx, _ := context.WithTimeout(context.Background(), METANODE_TIMEOUT_SECONDS*time.Second)
		pAllocateChunkAck, err = mc.AllocateChunk(ctx, pAllocateChunkReq)
		if err != nil || pAllocateChunkAck.Ret != 0 {
			logger.Error("AllocateChunk file: %v failed, err: %v ret: %v\n", cfile.Name, err, pAllocateChunkAck.Ret)
			return nil
		}
	}

	curChunk := &Chunk{}
	curChunk.CFile = cfile
	curChunk.ChunkInfo = pAllocateChunkAck.ChunkInfo

	logger.Debug("AllocateChunk file: %v from metanode chunk info:%v\n", cfile.Name, curChunk.ChunkInfo)

	if IsStream {

		err := utils.TryDial(curChunk.ChunkInfo.BlockGroupWithHost.Hosts[1])
		if err != nil {
			logger.Error("AllocateChunk file: %v new conn to %v failed, err: %v\n", cfile.Name, curChunk.ChunkInfo.BlockGroupWithHost.Hosts[1], err)
			return nil
		}

		if cfile.cfs.Copies == 3 {
			err = utils.TryDial(curChunk.ChunkInfo.BlockGroupWithHost.Hosts[2])
			if err != nil {
				logger.Error("AllocateChunk file: %v new conn to %v failed, err: %v\n", cfile.Name, curChunk.ChunkInfo.BlockGroupWithHost.Hosts[2], err)
				return nil
			}
		}

		C2Mconn := cfile.newDataConn(curChunk.ChunkInfo.BlockGroupWithHost.Hosts[0])
		if C2Mconn == nil {
			logger.Error("AllocateChunk file: %v new conn to %v failed\n", cfile.Name, curChunk.ChunkInfo.BlockGroupWithHost.Hosts[0])
			return nil
		}
		C2Mclient := dp.NewDataNodeClient(C2Mconn)
		curChunk.ChunkWriteStream, err = C2Mclient.C2MRepl(context.Background())
		if err != nil {
			cfile.delErrDataConn(curChunk.ChunkInfo.BlockGroupWithHost.Hosts[0])
			logger.Error("AllocateChunk file: %v create stream to %v failed, err: %v\n", cfile.Name, curChunk.ChunkInfo.BlockGroupWithHost.Hosts[0], err)
			return nil
		}

		curChunk.ChunkFreeSize = utils.ChunkSize
		curChunk.ChunkWriteRecvExitSignal = make(chan struct{})

		go curChunk.C2MRecv()
	}

	logger.Debug("AllocateChunk file: %v success\n", cfile.Name)

	return curChunk
}

// Retry to retry write ops when error occurred
func (chunk *Chunk) Retry() {

	if chunk.CFile.Status == FILE_NOT_EXIST {
		return
	}
	chunk.CFile.DataCacheLocker.Lock()
	defer chunk.CFile.DataCacheLocker.Unlock()

	if len(chunk.CFile.DataCache) == 0 {
		logger.Debug("C2MRecv thread end success for file %v chunk %v", chunk.CFile.Name, chunk.ChunkInfo.ChunkID)
		return
	}

	logger.Debug("C2MRecv thread Retry write file %v chunk %v start", chunk.CFile.Name, chunk.ChunkInfo.ChunkID)

	retrySuccess := false
	var err error
	for retryCnt := 0; retryCnt < WRITE_RETRY_CNT; retryCnt++ {
		err = chunk.writeRetryHandle()
		if err != nil {
			logger.Error("writeRetryHandle file %v chunk %v err: %v, try again for %v times!", chunk.CFile.Name, chunk.ChunkInfo.ChunkID, err, retryCnt+1)
			time.Sleep(time.Millisecond * 500)
			continue
		} else {
			retrySuccess = true
			break
		}
	}

	if !retrySuccess {
		chunk.CFile.Status = FILE_ERROR
		chunk.CFile.WriteErrSignal <- true
		logger.Error("C2MRecv thread Retry write file %v chunk %v failed, set FILE_ERROR!", chunk.CFile.Name, chunk.ChunkInfo.ChunkID)
	} else {
		chunk.CFile.DataCache = make(map[uint64]*Data)
		chunk.ChunkFreeSize = 0
		chunk.ChunkWriteStream = nil
		logger.Debug("C2MRecv thread Retry write file %v chunk %v success", chunk.CFile.Name, chunk.ChunkInfo.ChunkID)
	}
}

// C2MRecv is a routine to receive replay from the Datanode who is the master of this block goup
func (chunk *Chunk) C2MRecv() {
	logger.Debug("C2MRecv thread started success for file %v chunk %v", chunk.CFile.Name, chunk.ChunkInfo.ChunkID)

	defer chunk.Retry()

	for {
		in, err := chunk.ChunkWriteStream.Recv()
		if err == io.EOF {
			logger.Debug("C2MRecv: file %v chunk %v stream %v EOF\n", chunk.CFile.Name, chunk.ChunkInfo.ChunkID, chunk.ChunkWriteStream)
			break
		}
		if err != nil {
			logger.Debug("C2MRecv: file %v chunk %v stream %v error return : %v\n", chunk.CFile.Name, chunk.ChunkInfo.ChunkID, chunk.ChunkWriteStream, err)
			break
		}

		if in.Ret == -1 {
			logger.Error("C2MRecv: file %v chunk %v ack.Ret -1 , means M2S2B stream err", chunk.CFile.Name, chunk.ChunkInfo.ChunkID)
			break
		}

		atomic.AddUint64(&chunk.CFile.curNum, 1)
		if in.CommitID != chunk.CFile.curNum {
			logger.Error("C2MRecv: write failed! file: %v, ID；%v != curNum: %v, chunk: %v, len: %v\n", chunk.CFile.Name, in.CommitID, chunk.CFile.curNum, in.ChunkID, in.DataLen)
			break
		}

		logger.Debug("C2MRecv: Write success! try to update metadata file: %v, ID；%v, chunk: %v, len: %v\n",
			chunk.CFile.Name, in.CommitID, in.ChunkID, in.DataLen)

		mc := mp.NewMetaNodeClient(chunk.CFile.cfs.MetaNodeConn)
		pAsyncChunkReq := &mp.AsyncChunkReq{
			VolID:         chunk.CFile.cfs.VolID,
			ParentInodeID: chunk.CFile.ParentInodeID,
			Name:          chunk.CFile.Name,
			ChunkID:       in.ChunkID,
			CommitSize:    in.DataLen,
			BlockGroupID:  in.BlockGroupID,
		}
		ctx, _ := context.WithTimeout(context.Background(), METANODE_TIMEOUT_SECONDS*time.Second)
		pAsyncChunkAck, err2 := mc.AsyncChunk(ctx, pAsyncChunkReq)
		if err2 != nil {
			break
		}

		if pAsyncChunkAck.Ret == utils.ENO_NOTEXIST {
			logger.Error("Failed to write a not existing file: %v, inode: %v!", chunk.CFile.Name, chunk.CFile.Inode)
			chunk.CFile.Status = FILE_NOT_EXIST
			chunk.CFile.WriteErrSignal <- true
			for _, dn := range chunk.ChunkInfo.BlockGroupWithHost.Hosts {
				go func(host string) {
					logger.Debug("Request datanode %v to remove junk data block chunk: %v bg: %v", host, chunk.ChunkInfo.ChunkID, chunk.ChunkInfo.BlockGroupWithHost.BlockGroupID)
					if conn := chunk.CFile.newDataConn(host); conn != nil {
						dc := dp.NewDataNodeClient(conn)
						dpDeleteChunkReq := &dp.DeleteChunkReq{
							ChunkID:      chunk.ChunkInfo.ChunkID,
							BlockGroupID: chunk.ChunkInfo.BlockGroupWithHost.BlockGroupID,
						}
						dc.DeleteChunk(context.Background(), dpDeleteChunkReq)
						conn.Close()
					}
				}(dn)
			}
		}
		chunk.CFile.DataCacheLocker.Lock()
		//cfile.DataCache[in.CommitID].timer.Stop()
		delete(chunk.CFile.DataCache, in.CommitID)
		chunk.CFile.DataCacheLocker.Unlock()

		chunk.CFile.updateChunkSize(chunk.ChunkInfo, int32(in.DataLen))
	}
}

// writeRetryHandle is to retry write ops
func (chunk *Chunk) writeRetryHandle() error {

	length := len(chunk.CFile.DataCache)
	if length == 0 {
		return nil
	}

	tmpchunk := chunk.CFile.AllocateChunk(false)
	if tmpchunk == nil {
		return errors.New("AllocateChunk error")
	}

	sortedKeys := make([]int, 0)

	for k := range chunk.CFile.DataCache {
		sortedKeys = append(sortedKeys, int(k))
	}
	sort.Ints(sortedKeys)
	logger.Debug("writeRetryHandle AllocateChunk success, begin to retry item num:%v, commitIDs: %v", length, sortedKeys)

	var chunkSize int

	for _, vv := range sortedKeys {

		bufLen := chunk.CFile.DataCache[uint64(vv)].DataBuf.Len()
		req := dp.WriteChunkReq{ChunkID: tmpchunk.ChunkInfo.ChunkID,
			BlockGroupID: tmpchunk.ChunkInfo.BlockGroupWithHost.BlockGroupID,
			Databuf:      chunk.CFile.DataCache[uint64(vv)].DataBuf.Bytes(),
			CommitID:     uint64(vv),
		}
		for _, v := range tmpchunk.ChunkInfo.BlockGroupWithHost.Hosts {

			conn := chunk.CFile.newDataConn(v)
			if conn == nil {
				logger.Error("writeRetryHandle newDataConn Failed err")
				return fmt.Errorf("writeRetryHandle newDataConn Failed")
			}
			dc := dp.NewDataNodeClient(conn)
			ctx, _ := context.WithTimeout(context.Background(), METANODE_TIMEOUT_SECONDS*time.Second)
			_, err := dc.WriteChunk(ctx, &req)
			if err != nil {
				logger.Error("writeRetryHandle WriteChunk to DataNode Host Failed err %v", err)
				chunk.CFile.delErrDataConn(v)
				return err
			}
		}

		logger.Debug("writeRetryHandle write CommitID %v bufLen %v success", vv, bufLen)
		chunkSize += bufLen
		chunk.CFile.curNum = uint64(vv)
	}

	mc := mp.NewMetaNodeClient(chunk.CFile.cfs.MetaNodeConn)
	pAsyncChunkReq := &mp.AsyncChunkReq{
		VolID:         chunk.CFile.cfs.VolID,
		ParentInodeID: chunk.CFile.ParentInodeID,
		Name:          chunk.CFile.Name,
		ChunkID:       tmpchunk.ChunkInfo.ChunkID,
		CommitSize:    uint32(chunkSize),
		BlockGroupID:  tmpchunk.ChunkInfo.BlockGroupWithHost.BlockGroupID,
	}
	ctx, _ := context.WithTimeout(context.Background(), METANODE_TIMEOUT_SECONDS*time.Second)
	_, err2 := mc.AsyncChunk(ctx, pAsyncChunkReq)
	if err2 != nil {
		logger.Error("writeRetryHandle AsyncChunk to MetaNode Failed err %v", err2)
		return err2
	}
	logger.Debug("writeRetryHandle success with ChunkID %v ChunkSize %v", tmpchunk.ChunkInfo.ChunkID, chunkSize)

	chunk.CFile.updateChunkSize(tmpchunk.ChunkInfo, int32(chunkSize))
	return nil
}

// updateChunkSize to update ChunkSize and FileSize if chunk's data has be writted to datanode and syn to metanode
func (cfile *CFile) updateChunkSize(chunkinfo *mp.ChunkInfoWithBG, length int32) {

	chunkNum := len(cfile.chunks)
	if chunkNum != 0 && cfile.chunks[chunkNum-1].ChunkID == chunkinfo.ChunkID {
		cfile.chunks[chunkNum-1].ChunkSize += length
	} else {
		newchunkinfo := &mp.ChunkInfoWithBG{ChunkID: chunkinfo.ChunkID, ChunkSize: length, BlockGroupWithHost: chunkinfo.BlockGroupWithHost}
		cfile.chunks = append(cfile.chunks, newchunkinfo)
	}
	cfile.FileSize += int64(length)
}

// Flush provides the API to flush file
func (cfile *CFile) Flush() int32 {
	if cfile.isWrite == false {
		return 0
	}

	if cfile.Status != FILE_NORMAL {
		return -1
	}
	cfile.appendWrite(nil, 0, true)
	<-cfile.FlushSignal
	return 0
}

// CloseWrite provides the API to close an opened file with 'WR'
func (cfile *CFile) CloseWrite() int32 {

	cfile.appendWrite(nil, 0, true)
	<-cfile.FlushSignal

	cfile.Closing = true
	close(cfile.DataQueue)
	<-cfile.CloseSignal

	return 0
}

// Close provides the API to close an opened file
func (cfile *CFile) Close() int32 {
	cfile.delAllDataConn()
	return 0
}
