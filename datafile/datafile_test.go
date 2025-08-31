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
	"github.com/stretchr/testify/assert"
	"github.com/valyala/bytebufferpool"
	"os"
	"strconv"
	"testing"
	"time"
)

// 测试DataFile的创建、写入和读取功能
func TestDataFile_WriteAndRead(t *testing.T) {
	// 创建临时目录
	dir, err := os.MkdirTemp("../temp", "test_datafile")
	assert.Nil(t, err)
	defer func(path string) {
		err := os.RemoveAll(path)
		assert.Nil(t, err)
	}(dir)

	// 打开数据文件
	fileID := uint32(1)
	dataFile, err := OpenDataFile(dir, fileID)
	assert.Nil(t, err)
	defer dataFile.Close()

	// 准备测试数据
	testKey := []byte("test_key")
	testValue := []byte("test_value")
	kvPair := NewKvPair(testKey, testValue, RecordPuted)
	kvPair.KeySize = uint32(len(testKey))
	kvPair.ValueSize = uint32(len(testValue))

	// 写入数据
	header := make([]byte, MaxKvPairHeaderSize)
	buffer := bytebufferpool.Get()
	encodeKvPair := kvPair.EncodeKvPair(header, buffer)
	err = dataFile.Write(encodeKvPair)
	assert.Nil(t, err)
	assert.Equal(t, int64(len(encodeKvPair)), dataFile.WriteOffset)
	err = dataFile.Sync()
	assert.Nil(t, err)
	bytebufferpool.Put(buffer)

	// 读取数据
	readKvPair, err := dataFile.ReadKvPair(0)
	assert.Nil(t, err)
	assert.Equal(t, kvPair.Key, readKvPair.Key)
	assert.Equal(t, kvPair.Value, readKvPair.Value)
	assert.Equal(t, kvPair.Type, readKvPair.Type)
}

// 测试HintFile的写入和读取功能
func TestHintFile_WriteAndRead(t *testing.T) {
	// 创建临时目录
	dir, err := os.MkdirTemp("../temp", "test_hintfile")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	// 打开hint文件
	hintFile, err := OpenHintFile(dir)
	assert.Nil(t, err)
	defer hintFile.Close()

	// 准备测试数据
	testKey := []byte("hint_key")
	testPos := NewKvPairPos(1, 100, 200)

	// 写入hint数据
	header := make([]byte, MaxKvPairHeaderSize)
	buffer := bytebufferpool.Get()
	err = hintFile.WriteHintKvPair(testKey, &testPos, header, buffer)
	assert.Nil(t, err)
	err = hintFile.Sync()
	assert.Nil(t, err)
	bytebufferpool.Put(buffer)

	// 读取hint数据
	readKvPair, err := hintFile.ReadKvPair(0)
	assert.Nil(t, err)
	assert.Equal(t, testKey, readKvPair.Key)

	// 解码位置信息
	decodedPos := DecodeKvPairPos(readKvPair.Value)
	assert.Equal(t, testPos.Fid, decodedPos.Fid)
	assert.Equal(t, testPos.Offset, decodedPos.Offset)
}

// 测试MergeFinishedFile的写入和读取功能
func TestMergeFinishedFile_WriteAndRead(t *testing.T) {
	// 创建临时目录
	dir, err := os.MkdirTemp("../temp", "test_mergefinished")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	// 打开merge finished文件
	mergeFile, err := OpenMergeFinishedFile(dir)
	assert.Nil(t, err)
	defer mergeFile.Close()

	// 测试写入和读取
	testFileID := 123
	err = mergeFile.WriteMergeFinished(testFileID)
	assert.Nil(t, err)

	result, err := mergeFile.ReadMergeFinished()
	assert.Nil(t, err)
	assert.Equal(t, []byte(strconv.Itoa(testFileID)), result)
}

// 测试KvPair的CRC校验
func TestKvPair_CRC(t *testing.T) {
	// 创建临时目录
	dir, err := os.MkdirTemp("../temp", "test_crc")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	// 打开数据文件
	dataFile, err := OpenDataFile(dir, 1)
	assert.Nil(t, err)
	defer dataFile.Close()

	// 准备测试数据
	// 错误数据
	kvPair := NewKvPair([]byte("crc_key"), []byte("crc_value"), RecordPuted)
	kvPair.KeySize = uint32(len(kvPair.Key))
	kvPair.ValueSize = uint32(len(kvPair.Value))
	header1 := make([]byte, MaxKvPairHeaderSize)
	buffer1 := bytebufferpool.Get()
	encoded := kvPair.EncodeKvPair(header1, buffer1)
	encoded[5] = encoded[5] + 1

	// 正确数据
	kvPair2 := NewKvPair([]byte("crc_key"), []byte("crc_value"), RecordPuted)
	kvPair2.KeySize = uint32(len(kvPair2.Key))
	kvPair2.ValueSize = uint32(len(kvPair2.Value))
	header2 := make([]byte, MaxKvPairHeaderSize)
	buffer2 := bytebufferpool.Get()
	encoded2 := kvPair2.EncodeKvPair(header2, buffer2)

	// 写入数据到文件
	// 写入错误数据
	writeOffset := dataFile.WriteOffset
	err = dataFile.Write(encoded)
	assert.Nil(t, err)
	writeOffset2 := dataFile.WriteOffset
	err = dataFile.Write(encoded2)
	assert.Nil(t, err)
	// 同步到磁盘
	err = dataFile.Sync()
	assert.Nil(t, err)

	// 从文件中读取被篡改的数据
	decoded, err := dataFile.ReadKvPair(writeOffset)
	// 预期会返回CRC错误
	assert.Equal(t, ErrInvalidCRC, err)
	assert.Nil(t, decoded)
	// 从文件中读取被篡改的数据
	decoded2, err := dataFile.ReadKvPair(writeOffset2)
	// 预期会返回正确数据
	assert.Nil(t, err)
	assert.NotNil(t, decoded2)

	bytebufferpool.Put(buffer1)
	bytebufferpool.Put(buffer2)
}

// 测试KvPair的过期检查
func TestKvPair_IsExpired(t *testing.T) {
	// 持久化数据（不过期）
	permanent := NewKvPair([]byte("perm_key"), []byte("perm_val"), RecordPuted)
	permanent.TTL = PERSISTENT
	assert.False(t, permanent.IsExpired())

	// 未过期数据
	notExpired := NewKvPair([]byte("not_exp_key"), []byte("not_exp_val"), RecordPuted)
	notExpired.TTL = 3600 // 1小时过期
	notExpired.Timestamp = uint64(time.Now().UnixMilli())
	assert.False(t, notExpired.IsExpired())

	// 已过期数据
	expired := NewKvPair([]byte("exp_key"), []byte("exp_val"), RecordPuted)
	expired.TTL = 1 // 1秒过期
	expired.Timestamp = uint64(time.Now().Add(-2 * time.Second).UnixMilli())
	assert.True(t, expired.IsExpired())
}

// 测试KvPairPos的编码和解码
func TestKvPairPos_EncodeDecode(t *testing.T) {
	// 准备测试数据
	pos := &KvPairPos{
		Fid:    5,
		Offset: 1000,
		Size:   200,
		Type:   RecordPuted,
	}

	// 编码解码
	encoded := EncodeKvPairPos(pos)
	decoded := DecodeKvPairPos(encoded)

	// 验证
	assert.Equal(t, pos.Fid, decoded.Fid)
	assert.Equal(t, pos.Offset, decoded.Offset)
}

// 测试DataFile的Sync和Close方法
func TestDataFile_SyncAndClose(t *testing.T) {
	dir, err := os.MkdirTemp("../temp", "test_sync_close")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	dataFile, err := OpenDataFile(dir, 1)
	assert.Nil(t, err)

	// 测试Sync
	err = dataFile.Write([]byte("test_sync"))
	assert.Nil(t, err)
	err = dataFile.Sync()
	assert.Nil(t, err)

	// 测试Close
	err = dataFile.Close()
	assert.Nil(t, err)

	// 验证文件已关闭（再次操作应出错）
	err = dataFile.Write([]byte("after_close"))
	assert.Error(t, err)
}

// 测试DataFile的边界情况
func TestDataFile_BoundaryCases(t *testing.T) {
	dir, err := os.MkdirTemp("../temp", "test_boundary")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	dataFile, err := OpenDataFile(dir, 1)
	assert.Nil(t, err)
	defer dataFile.Close()

	// 测试空数据
	emptyKv := NewKvPair([]byte{}, []byte{}, RecordPuted)
	emptyKv.KeySize = 0
	emptyKv.ValueSize = 0
	header1 := make([]byte, MaxKvPairHeaderSize)
	buffer1 := bytebufferpool.Get()
	encoded := emptyKv.EncodeKvPair(header1, buffer1)
	err = dataFile.Write(encoded)
	assert.Nil(t, err)
	err = dataFile.Sync()
	assert.Nil(t, err)

	readKv, err := dataFile.ReadKvPair(0)
	assert.Nil(t, err)
	assert.Empty(t, readKv.Key)
	assert.Empty(t, readKv.Value)

	// 测试大数据
	largeKey := make([]byte, 1024*1024)      // 1MB
	largeValue := make([]byte, 10*1024*1024) // 10MB
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	largeKv := NewKvPair(largeKey, largeValue, RecordPuted)
	largeKv.KeySize = uint32(len(largeKey))
	largeKv.ValueSize = uint32(len(largeValue))
	header2 := make([]byte, MaxKvPairHeaderSize)
	buffer2 := bytebufferpool.Get()
	encoded = largeKv.EncodeKvPair(header2, buffer2)

	err = dataFile.Write(encoded)
	assert.Nil(t, err)

	clear(header1)
	buffer1.Reset()
	readKv, err = dataFile.ReadKvPair(int64(len(emptyKv.EncodeKvPair(header1, buffer1))))
	assert.Nil(t, err)
	assert.Equal(t, largeKey, readKv.Key)
	assert.Equal(t, largeValue, readKv.Value)

	bytebufferpool.Put(buffer1)
	bytebufferpool.Put(buffer2)
}
