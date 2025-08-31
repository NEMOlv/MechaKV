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

package database

import (
	. "MechaKV/comment"
	"MechaKV/datafile"
	"MechaKV/index"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/bytebufferpool"
	"os"
	"strconv"
	"strings"
	"testing"
)

// TestGetNotMergeDatafileID 测试 getNotMergeDatafileID 函数
func TestGetNotMergeDatafileID(t *testing.T) {
	// 创建临时目录
	dir, err := os.MkdirTemp("../temp", "test_db")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	// 创建 MergeFinishedFile 并写入测试数据
	mergeFinishedFile, err := datafile.OpenMergeFinishedFile(dir)
	if err != nil {
		t.Fatalf("Failed to open MergeFinishedFile: %v", err)
	}

	if err = mergeFinishedFile.WriteMergeFinished(123); err != nil {
		return
	}

	if err = mergeFinishedFile.Close(); err != nil {
		return
	}

	db := &DB{
		Opts: Options{
			DirPath: dir,
		},
	}

	expectedID := uint32(123)
	actualID, err := db.getNotMergeDatafileID(dir)
	if err != nil {
		t.Fatalf("getNotMergeDatafileID returned an error: %v", err)
	}
	assert.Equal(t, expectedID, actualID, "getNotMergeDatafileID should return the correct file ID")
}

// TestLoadMergedFiles 测试 loadMergedFiles 函数
func TestLoadMergedFiles(t *testing.T) {
	// 创建临时目录
	dir, err := os.MkdirTemp("../temp", "test_db")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	// 模拟打开数据库
	db := &DB{
		Opts: Options{
			DirPath: dir,
		},
	}

	// 模拟数据库目录中的数据文件
	for i := 0; i < 80; i++ {
		file, err := datafile.OpenDataFile(dir, uint32(i))
		if err != nil {
			t.Fatalf("Failed to open datafile: %v", err)
		}
		if err := file.Close(); err != nil {
			return
		}
	}

	// 模拟合并目录
	if err := os.Mkdir(db.getMergePath(), DataFilePermision); err != nil {
		return
	}

	// 模拟合并目录中的数据文件
	for i := 100; i < 200; i++ {
		file, err := datafile.OpenDataFile(db.getMergePath(), uint32(i))
		if err != nil {
			t.Fatalf("Failed to open datafile: %v", err)
		}
		if err := file.Close(); err != nil {
			return
		}
	}

	// 创建 MergeFinishedFile 并写入测试数据
	mergeFinishedFile, err := datafile.OpenMergeFinishedFile(db.getMergePath())
	if err != nil {
		t.Fatalf("Failed to open MergeFinishedFile: %v", err)
	}

	if err = mergeFinishedFile.WriteMergeFinished(100); err != nil {
		return
	}

	if err = mergeFinishedFile.Close(); err != nil {
		return
	}

	// 数据库从合并目录中加载数据
	err = db.loadMergedFiles()
	assert.Nil(t, err, "loadMergedFiles should return nil when merge directory does not exist")

	// 从数据库目录中获取全部的文件
	dirFiles, err := os.ReadDir(dir)
	assert.Nil(t, err)

	// 依次检验是否和合并目录中的数据文件一致
	for i, entry := range dirFiles {
		if strings.HasSuffix(entry.Name(), datafile.DatafileSuffix) {
			fileName := strings.Split(entry.Name(), ".")[0]
			// 将字符串转为数字
			fileId, err := strconv.Atoi(fileName)
			assert.Nil(t, err)

			assert.Equal(t, i+100, fileId)
		}
	}

}

// TestLoadDataFiles 测试 loadDataFiles 函数
func TestLoadDataFiles(t *testing.T) {
	// 创建临时目录
	dir, err := os.MkdirTemp("../temp", "test_db")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	// 模拟数据库目录中的数据文件
	for i := 0; i < 100; i++ {
		file, err := datafile.OpenDataFile(dir, uint32(i))
		if err != nil {
			t.Fatalf("Failed to open datafile: %v", err)
		}
		if err := file.Close(); err != nil {
			return
		}
	}

	db := &DB{
		Opts: Options{
			DirPath: dir,
		},
		OldFiles: make(map[uint32]*datafile.DataFile),
	}

	err = db.loadDataFiles()
	if err != nil {
		t.Fatalf("loadDataFiles returned an error: %v", err)
	}

	for i := 0; i < 100; i++ {
		assert.Equal(t, i, int(db.fileIds[i]))
	}
}

// TestLoadIndexFromHintFile 测试 loadIndexFromHintFile 函数
func TestLoadIndexFromHintFile(t *testing.T) {
	// 创建临时目录
	dir, err := os.MkdirTemp("../temp", "test_db")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	// 创建 hint 文件并写入测试数据
	hintFile, err := datafile.OpenHintFile(dir)
	assert.Nil(t, err, "failed to open hint file")
	defer func() {
		err = hintFile.Close()
		assert.Nil(t, err, "failed to close hint file")
	}()
	var offset int64 = 0
	offsets := make([]int64, 100)
	for i := 0; i < 100; i++ {
		offsets[i] = offset
		testKey := []byte("test_key" + strconv.Itoa(i))
		testPos := datafile.NewKvPairPos(1, offset, 0)
		encodedPos := datafile.EncodeKvPairPos(&testPos)
		kvPair := &datafile.KvPair{
			Key:       testKey,
			Value:     encodedPos,
			KeySize:   uint32(len(testKey)),
			ValueSize: uint32(len(encodedPos)),
		}
		header := make([]byte, MaxKvPairHeaderSize)
		buffer := bytebufferpool.Get()
		encodeKvPair := kvPair.EncodeKvPair(header, buffer)
		if err := hintFile.Write(encodeKvPair); err != nil {
			t.Fatalf("Failed to write to hint file: %v", err)
		}
		if err := hintFile.Sync(); err != nil {
			t.Fatalf("Failed to sync to hint file: %v", err)
		}
		offset += int64(kvPair.Size())
		bytebufferpool.Put(buffer)
	}

	db := &DB{
		Opts: Options{
			DirPath: dir,
		},
		Index: index.NewBTree(),
	}

	err = db.loadIndexFromHintFile()
	assert.Nil(t, err, "loadIndexFromHintFile returned an error")

	for i := 0; i < 100; i++ {
		testKey := []byte("test_key" + strconv.Itoa(i))
		loadedPos := db.Index.Get(testKey)
		assert.Equal(t, uint32(1), loadedPos.Fid)
		assert.Equal(t, offsets[i], loadedPos.Offset)
	}
}

// TestLoadIndexFromDataFiles 测试 loadIndexFromDataFiles 函数
func TestLoadIndexFromDataFiles(t *testing.T) {
	// 创建临时目录
	dir, err := os.MkdirTemp("../temp", "test_db")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	// 创建一个测试数据文件
	FileID := uint32(1)
	dataFile, err := datafile.OpenDataFile(dir, FileID)
	if err != nil {
		t.Fatalf("Failed to open test data file: %v", err)
	}
	defer dataFile.Close()

	var offset int64 = 0
	offsets := make([]int64, 100)
	for i := 0; i < 100; i++ {
		offsets[i] = offset
		testKey := []byte("test_key" + strconv.Itoa(i))
		testValue := []byte("test_value" + strconv.Itoa(i))
		kvPair := &datafile.KvPair{
			TxId:      uint64(1),
			TxFinshed: TxFinished,
			Key:       testKey,
			Value:     testValue,
			KeySize:   uint32(len(testKey)),
			ValueSize: uint32(len(testValue)),
			Type:      RecordPuted,
		}
		header := make([]byte, MaxKvPairHeaderSize)
		buffer := bytebufferpool.Get()
		encodeKvPair := kvPair.EncodeKvPair(header, buffer)
		if err := dataFile.Write(encodeKvPair); err != nil {
			t.Fatalf("Failed to write to hint file: %v", err)
		}
		if err := dataFile.Sync(); err != nil {
			t.Fatalf("Failed to sync to hint file: %v", err)
		}
		offset += int64(kvPair.Size())
		bytebufferpool.Put(buffer)
	}

	db := &DB{
		Opts: Options{
			DirPath: dir,
		},
		fileIds:    []uint32{FileID},
		ActiveFile: dataFile,
		OldFiles:   make(map[uint32]*datafile.DataFile),
		Index:      index.NewBTree(),
	}

	err = db.loadIndexFromDataFiles()
	if err != nil {
		t.Fatalf("loadIndexFromDataFiles returned an error: %v", err)
	}
	for i := 0; i < 100; i++ {
		testKey := []byte("test_key" + strconv.Itoa(i))
		loadedPos := db.Index.Get(testKey)
		assert.Equal(t, uint32(1), loadedPos.Fid)
		assert.Equal(t, offsets[i], loadedPos.Offset)
	}

}

// TestSetActiveDataFile 测试 setActiveDataFile 函数
func TestSetActiveDataFile(t *testing.T) {
	// 创建临时目录
	dir, err := os.MkdirTemp("../temp", "test_db")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	db := &DB{
		Opts: Options{
			DirPath: dir,
		},
	}

	err = db.setActiveDataFile()
	if err != nil {
		t.Fatalf("setActiveDataFile returned an error: %v", err)
	}
	assert.NotNil(t, db.ActiveFile, "setActiveDataFile should set the active data file")
}

// TestAppendKvPair 测试 AppendKvPair 函数
func TestAppendKvPair(t *testing.T) {
	// 创建临时目录
	dir, err := os.MkdirTemp("../temp", "test_db")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	db := &DB{
		Opts: Options{
			DirPath:      dir,
			DataFileSize: 1024,
		},
		Index:        index.NewBTree(),
		KeySlots:     make(map[uint16]keyPointer),
		KvPairHeader: make([]byte, MaxKvPairHeaderSize),
	}

	testKey := []byte("test_key")
	testValue := []byte("test_value")
	kvPair := &datafile.KvPair{
		Key:       testKey,
		Value:     testValue,
		KeySize:   uint32(len(testKey)),
		ValueSize: uint32(len(testValue)),
		Type:      RecordPuted,
	}

	buffer := bytebufferpool.Get()
	pos, err := db.AppendKvPair(kvPair, db.KvPairHeader, buffer)
	if err != nil {
		t.Fatalf("AppendKvPair returned an error: %v", err)
	}
	assert.NotNil(t, pos, "AppendKvPair should return a valid position")
	bytebufferpool.Put(buffer)
}

// TestGetValueByPosition 测试 GetValueByPosition 函数
func TestGetValueByPosition(t *testing.T) {
	// 创建临时目录
	dir, err := os.MkdirTemp("../temp", "test_db")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	// 创建一个测试数据文件
	testFileID := uint32(1)
	testDataFile, err := datafile.OpenDataFile(dir, testFileID)
	if err != nil {
		t.Fatalf("Failed to open test data file: %v", err)
	}
	defer testDataFile.Close()

	testKey := []byte("test_key")
	testValue := []byte("test_value")
	kvPair := &datafile.KvPair{
		Key:       testKey,
		Value:     testValue,
		KeySize:   uint32(len(testKey)),
		ValueSize: uint32(len(testValue)),
		Type:      RecordPuted,
	}
	header := make([]byte, MaxKvPairHeaderSize)
	buffer := bytebufferpool.Get()
	encodeKvPair := kvPair.EncodeKvPair(header, buffer)
	if err := testDataFile.Write(encodeKvPair); err != nil {
		t.Fatalf("Failed to write to test data file: %v", err)
	}
	if err := testDataFile.Sync(); err != nil {
		t.Fatalf("Failed to sync to test data file: %v", err)
	}
	bytebufferpool.Put(buffer)

	db := &DB{
		Opts: Options{
			DirPath: dir,
		},
		fileIds:    []uint32{testFileID},
		ActiveFile: testDataFile,
		OldFiles:   make(map[uint32]*datafile.DataFile),
		Index:      index.NewBTree(),
	}

	pos := datafile.NewKvPairPos(testFileID, 0, uint32(len(encodeKvPair)))
	value, err := db.GetValueByPosition(&pos)
	if err != nil {
		t.Fatalf("GetValueByPosition returned an error: %v", err)
	}
	assert.Equal(t, testValue, value, "GetValueByPosition should return the correct value")

}

// TestGetKvPairByPosition 测试 GetKvPairByPosition 函数
func TestGetKvPairByPosition(t *testing.T) {
	// 创建临时目录
	dir, err := os.MkdirTemp("../temp", "test_db")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	// 创建一个测试数据文件
	testFileID := uint32(1)
	testDataFile, err := datafile.OpenDataFile(dir, testFileID)
	if err != nil {
		t.Fatalf("Failed to open test data file: %v", err)
	}
	defer testDataFile.Close()

	testKey := []byte("test_key")
	testValue := []byte("test_value")
	kvPair := &datafile.KvPair{
		Key:       testKey,
		Value:     testValue,
		KeySize:   uint32(len(testKey)),
		ValueSize: uint32(len(testValue)),
		Type:      RecordPuted,
	}

	header := make([]byte, MaxKvPairHeaderSize)
	buffer := bytebufferpool.Get()
	encodeKvPair := kvPair.EncodeKvPair(header, buffer)

	if err := testDataFile.Write(encodeKvPair); err != nil {
		t.Fatalf("Failed to write to test data file: %v", err)
	}
	if err := testDataFile.Sync(); err != nil {
		t.Fatalf("Failed to sync to test data file: %v", err)
	}

	bytebufferpool.Put(buffer)

	db := &DB{
		Opts: Options{
			DirPath: dir,
		},
		fileIds:    []uint32{testFileID},
		ActiveFile: testDataFile,
		OldFiles:   make(map[uint32]*datafile.DataFile),
		Index:      index.NewBTree(),
	}

	pos := datafile.NewKvPairPos(testFileID, 0, uint32(len(encodeKvPair)))
	loadedKvPair, err := db.GetKvPairByPosition(&pos)
	if err != nil {
		t.Fatalf("GetKvPairByPosition returned an error: %v", err)
	}
	assert.Equal(t, testKey, loadedKvPair.Key, "GetKvPairByPosition should return the correct key")
	assert.Equal(t, testValue, loadedKvPair.Value, "GetKvPairByPosition should return the correct value")
}
