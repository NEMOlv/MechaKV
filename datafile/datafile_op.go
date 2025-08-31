/*
Copyright 2025 Nemo(shengyi) Lv

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package datafile

import (
	. "MechaKV/comment"
	. "MechaKV/datafile/datafile_io"
	"fmt"
	"github.com/valyala/bytebufferpool"
	"hash/crc32"
	"path/filepath"
)

type DataFile struct {
	// 文件id
	FileID uint32
	// 文件写入偏移
	WriteOffset int64
	// IO读写管理
	IOManager IOManager
}

const (
	DatafileSuffix        = ".data"
	HintFileName          = "hint-index"
	BucketFileName        = "Bucket"
	MergeFinishedFileName = "merge-finished"
)

func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fullpath := filepath.Join(dirPath, "Data", fmt.Sprintf("%09d", fileId)+DatafileSuffix)
	return newDataFile(fullpath, fileId)
}

func OpenHintFile(dirPath string) (*DataFile, error) {
	fullpath := filepath.Join(dirPath, HintFileName)
	return newDataFile(fullpath, 0)
}

func OpenBucketFile(dirPath string, fileId uint32) (*DataFile, error) {
	fullpath := filepath.Join(dirPath, BucketFileName+"-"+fmt.Sprintf("%09d", fileId)+DatafileSuffix)
	return newDataFile(fullpath, fileId)
}

func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fullpath := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fullpath, 0)
}

func GetDatafileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DatafileSuffix)
}

func newDataFile(fullpath string, fileId uint32) (*DataFile, error) {
	// 初始化IOManager管理器接口
	manager, err := NewIOManager(fullpath)
	if err != nil {
		return nil, err
	}

	dataFile := &DataFile{
		FileID:      fileId,
		WriteOffset: 0,
		IOManager:   manager,
	}

	return dataFile, nil
}

// Write 直接写入字节数据，编码工作手动实现
func (df *DataFile) Write(buf []byte) error {
	size, err := df.IOManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOffset += int64(size)
	return nil
}

//// todo 补充一个WriteKvPaird
//func (df *DataFile) WriteKvPaird(kvPair *KvPair) error {
//	buf := kvPair.EncodeKvPair()
//	size, err := df.IOManager.Write(buf)
//	if err != nil {
//		return err
//	}
//	df.WriteOffset += int64(size)
//	return nil
//}

func (df *DataFile) WriteHintKvPair(key []byte, pos *KvPairPos, KvPairHeader []byte, KvPairBuffer *bytebufferpool.ByteBuffer) error {
	value := EncodeKvPairPos(pos)
	kvPair := &KvPair{
		Key:       key,
		Value:     value,
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
	}

	encodeKvPair := kvPair.EncodeKvPair(KvPairHeader, KvPairBuffer)

	return df.Write(encodeKvPair)
}

// ReadKvPair 读取解码后的KvPair数据
func (df *DataFile) ReadKvPair(offset int64) (*KvPair, error) {
	//println(offset)
	// 获取文件大小
	filesize, err := df.IOManager.Size()
	//println(filesize)
	if err != nil {
		return nil, err
	}

	// 首部最大大小
	size := MaxKvPairHeaderSize
	// 如果offset+最大字节数超过了文件大小，则只读取到文件末尾
	if offset+size >= filesize {
		size = filesize - offset
	}

	// 新建一个KvPair
	kvPair := new(KvPair)

	// 读取Header
	headerBuf, err := df.readNBytes(size, offset)
	if err != nil {
		return nil, err
	}
	// 解码Header
	kvPair.DecodeKvPairHeader(headerBuf)

	// 读取Payload
	payloadBuf, err := df.readNBytes(kvPair.PayloadSize(), offset+int64(kvPair.HeaderSize))
	if err != nil {
		return nil, err
	}
	// 解码payload
	kvPair.DecodeKvPair(payloadBuf)

	// 读取到文件末尾，返回EOF错误
	if !check(kvPair) {
		return nil, EOF
	}

	// 校验数据的有效性
	crc := computeKvPairCRC(kvPair, headerBuf[crc32.Size:kvPair.HeaderSize])
	if crc != kvPair.Crc {
		return nil, ErrInvalidCRC
	}

	return kvPair, nil
}

// ReadKvPair 读取解码后的KvPair数据
func (df *DataFile) ReadKvPairPos(offset int64) (*KvPair, error) {
	// 获取文件大小
	filesize, err := df.IOManager.Size()
	if err != nil {
		return nil, err
	}

	// 首部最大大小
	size := MaxKvPairHeaderSize
	// 如果offset+最大字节数超过了文件大小，则只读取到文件末尾
	if offset+size >= filesize {
		size = filesize - offset
	}

	// 新建一个KvPair
	kvPair := new(KvPair)

	// 读取Header
	headerBuf, err := df.readNBytes(size, offset)
	if err != nil {
		return nil, err
	}
	// 解码Header
	kvPair.DecodeKvPairHeader(headerBuf)

	// 读取Payload
	payloadBuf, err := df.readNBytes(kvPair.PayloadSize(), offset+int64(kvPair.HeaderSize))
	if err != nil {
		return nil, err
	}
	// 解码payload
	kvPair.DecodeKvPair(payloadBuf)

	// 读取到文件末尾，返回EOF错误
	if !check(kvPair) {
		return nil, EOF
	}

	// 校验数据的有效性
	crc := computeKvPairCRC(kvPair, headerBuf[crc32.Size:kvPair.HeaderSize])
	if crc != kvPair.Crc {
		return nil, ErrInvalidCRC
	}

	return kvPair, nil
}

// readNBytes 读取N个字节数据
func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IOManager.Read(b, offset)
	return b, err
}

func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IOManager.Close()
}

func (df *DataFile) SetIOManager(dirPath string) error {
	if err := df.IOManager.Close(); err != nil {
		return err
	}
	ioManager, err := NewIOManager(GetDatafileName(dirPath, df.FileID))
	if err != nil {
		return err
	}
	df.IOManager = ioManager
	return nil
}

func check(kvPair *KvPair) bool {
	if kvPair == nil {
		return false
	}

	if kvPair.Crc == 0 && len(kvPair.Key) == 0 && len(kvPair.Value) == 0 {
		return false
	}

	return true
}

func (mergeFinishedFile *DataFile) WriteMergeFinished(nonMergeFileID int) (err error) {
	encMergeFinished := EncodeMergeFinished(nonMergeFileID)

	if err = mergeFinishedFile.Write(encMergeFinished); err != nil {
		return
	}

	if err = mergeFinishedFile.Sync(); err != nil {
		return
	}

	return
}

func (mergeFinishedFile *DataFile) ReadMergeFinished() ([]byte, error) {
	// 获取文件大小
	filesize, err := mergeFinishedFile.IOManager.Size()
	if err != nil {
		return nil, err
	}

	// 读取字节内容
	var encMergeFinished = make([]byte, filesize)
	if encMergeFinished, err = mergeFinishedFile.readNBytes(filesize, 0); err != nil {
		return nil, err
	}
	var decMergeFinished []byte
	if decMergeFinished, err = DecodeMergeFinished(encMergeFinished); err != nil {
		return nil, err
	}

	return decMergeFinished, nil
}
