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
	"bytes"
	"fmt"
	"github.com/bwmarrin/snowflake"
	"github.com/gofrs/flock"
	"github.com/sigurn/crc16"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

type (
	keyPointer []*[]byte
	KvPair     = datafile.KvPair
	KvPairPos  = datafile.KvPairPos
	DataFile   = datafile.DataFile
)

type MergeManager struct {
	// 引入cron，考虑移除
	//// 合并开始通道
	//startCh chan struct{}
	//// 合并结束通道
	//endCh chan error
	//// 合并工作通道
	//closeCh chan struct{}
	// 合并的 cron 调度程序
	cronScheduler *cron.Cron
	// 表示是否正在进行合并
	isMerging bool
}

type DB struct {
	// 用户传入选项
	Opts Options
	// 读写锁保证多进程之间互斥执行读写操作
	DBLock *sync.RWMutex
	// 文件锁保证多进程之间的互斥打开同一数据库
	fileLock *flock.Flock
	// 文件ID切片
	fileIds []uint32
	// 内存索引
	Index index.Indexer
	// 活跃数据文件：用于读写
	ActiveFile *DataFile
	// 旧数据文件：只用于读
	OlderFiles map[uint32]*DataFile
	// 表示有多少数据是无效的
	ReclaimSize int64
	// 合并管理器
	mergeManager MergeManager
	// 过期器
	expiryMonitor *ExpiryMonitor
	// 表示数据库是否初始化
	isInitial bool
	// 累计写入N字节数据
	bytesWrite uint
	//
	KeySlots map[uint16]keyPointer
	//
	TxIdGenerater *snowflake.Node
}

// getNotMergeDatafileID 获取没有进行合并的DatafileID
func (db *DB) getNotMergeDatafileID(dirPath string) (uint32, error) {
	mergeFinishedFile, err := datafile.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	var notMergeDatafileID []byte
	notMergeDatafileID, err = mergeFinishedFile.ReadMergeFinished()
	if err != nil {
		return 0, err
	}
	nonMergeFiledID, err := strconv.Atoi(string(notMergeDatafileID))
	if err != nil {
		return 0, err
	}
	if err = mergeFinishedFile.Close(); err != nil {
		return 0, err
	}
	return uint32(nonMergeFiledID), nil
}

// 获取合并目录
func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.Opts.DirPath))
	base := path.Base(db.Opts.DirPath)
	return filepath.Join(dir, base+MergeDirName)
}

// 从合并目录加载数据文件
func (db *DB) loadMergedFiles() error {
	// 获取mergePath的名称
	// 合并目录名称 = 数据目录名称 + MergeDirName
	mergePath := db.getMergePath()
	// 合并目录不存在则直接返回，代表没有进行合并操作
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}

	// 获取合并目录中的所有文件
	dirFiles, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 查找标识merge完成的文件，判断merge是否处理完成
	var mergeFinished bool
	var mergeFileNames []string
	for _, file := range dirFiles {
		if file.Name() == datafile.MergeFinishedFileName {
			mergeFinished = true
		}

		// 跳过文件锁
		if file.Name() == FileLockName {
			continue
		}
		mergeFileNames = append(mergeFileNames, file.Name())
	}

	// 没有merge标识，直接返回
	if !mergeFinished {
		return nil
	}

	// 获取尚未进行合并的数据文件ID
	NotMergeDatafileID, err := db.getNotMergeDatafileID(mergePath)
	if err != nil {
		return err
	}

	// 删除数据目录中的已合并的数据文件
	var fileID uint32 = 0
	for ; fileID < NotMergeDatafileID; fileID++ {
		fileName := datafile.GetDatafileName(db.Opts.DirPath, fileID)
		// 检查文件是否存在
		_, err := os.Stat(fileName)
		if err != nil {
			// 明确判断是否为“文件不存在”，其他错误需要记录
			if !os.IsNotExist(err) {
				fmt.Printf("检查文件 %s 时出错: %v\n", fileName, err)
			}
			continue
		}

		if err := os.Remove(fileName); err != nil {
			fmt.Printf("删除文件 %s 失败: %v\n", fileName, err)
		}
	}

	// 将合并后的数据文件移动到数据目录中
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(db.Opts.DirPath, fileName)
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}

	return nil
}

// 从数据库目录加载数据文件
func (db *DB) loadDataFiles() error {
	// 获取数据目录中的所有文件
	dirFiles, err := os.ReadDir(db.Opts.DirPath)
	if err != nil {
		return err
	}

	// 遍历数据目录中的所有文件，找到所有以.data结尾的文件
	var fileIds []int
	for _, entry := range dirFiles {
		if strings.HasSuffix(entry.Name(), datafile.DatafileSuffix) {
			fileName := strings.Split(entry.Name(), ".")[0]
			// 将字符串转为数字
			fileId, err := strconv.Atoi(fileName)
			if err != nil {
				return ErrDataDirectoryCorrupt
			}

			fileIds = append(fileIds, fileId)
		}
	}

	// 对数据文件id进行排序，从小到大依次加载
	sort.Ints(fileIds)
	fileIdsUint32 := make([]uint32, len(fileIds))

	// 遍历每个数据文件id，打开对应的数据文件
	for i, fileId := range fileIds {
		fileId := uint32(fileId)
		fileIdsUint32[i] = fileId
		dataFile, err := datafile.OpenDataFile(db.Opts.DirPath, fileId)
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			db.ActiveFile = dataFile
		} else {
			db.OlderFiles[fileId] = dataFile
		}
	}

	// 存储fileIds
	db.fileIds = fileIdsUint32

	return nil
}

// 从hint文件加载索引
func (db *DB) loadIndexFromHintFile() error {
	// 查看hint索引文件是否存在
	hintFileName := filepath.Join(db.Opts.DirPath, datafile.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	// 打开hint文件
	hintFile, err := datafile.OpenHintFile(db.Opts.DirPath)
	if err != nil {
		return err
	}

	// 读取文件中的索引
	var offset int64 = 0
	for {
		kvPair, err := hintFile.ReadKvPair(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// 解码拿到实际的位置索引
		kvPairPos := datafile.DecodeKvPairPos(kvPair.Value)
		db.Index.Put(kvPair.Key, kvPairPos)
		offset += int64(kvPair.Size())
	}

	return nil
}

// 从数据文件中加载索引
func (db *DB) loadIndexFromDataFiles() error {
	// 没有文件，说明数据库是空的，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	hasMerge := false
	notMergeDatafileID := uint32(0)

	// 先执行loadMergedFiles后，这里hasMerge只可能为false
	mergePath := filepath.Join(db.Opts.DirPath, datafile.MergeFinishedFileName)
	if _, err := os.Stat(mergePath); err == nil {
		// 后清理：将合并目录及其数据删除
		defer func() {
			_ = os.RemoveAll(mergePath)
		}()
		fid, err := db.getNotMergeDatafileID(db.Opts.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		notMergeDatafileID = fid
	}

	// 暂存事务数据
	TxMap := make(map[uint64][]*KvPairPos)

	//	遍历所有的文件id，处理文件中的记录

	for i, fileId := range db.fileIds {
		// 已经构建过的内存索引，不重复构建
		if hasMerge && fileId < notMergeDatafileID {
			continue
		}

		var dataFile *DataFile
		var offset int64 = 0

		if fileId == db.ActiveFile.FileID {
			dataFile = db.ActiveFile
		} else {
			dataFile = db.OlderFiles[fileId]
		}

		for {
			// 从数据文件中读数据
			kvPair, err := dataFile.ReadKvPair(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			size := kvPair.Size()
			// 构造内存索引并保存
			kvPairPos := &KvPairPos{
				Key:    kvPair.Key,
				Fid:    fileId,
				Offset: offset,
				Size:   size,
				Type:   kvPair.Type,
			}

			// 重构事务
			TxMap[kvPair.TxId] = append(TxMap[kvPair.TxId], kvPairPos)
			// 事务完成
			if kvPair.TxFinshed == TxFinished {
				for _, kvPairdPos := range TxMap[kvPair.TxId] {
					var oldPos *KvPairPos
					if kvPairdPos.Type == KvPairDeleted {
						oldPos, _ = db.Index.Delete(kvPairdPos.Key)
						db.ReclaimSize += int64(kvPairdPos.Size)
					} else {
						oldPos = db.Index.Put(kvPairdPos.Key, kvPairdPos)
					}
					if oldPos != nil {
						db.ReclaimSize += int64(oldPos.Size)
					}
				}
				delete(TxMap, kvPair.TxId)
			}

			offset += int64(size)
		}
		// 如果当前是活跃文件，更新这个文件的Writeoff
		if i == len(db.fileIds)-1 {
			db.ActiveFile.WriteOffset = offset
		}
	}

	return nil
}

// 设置当前活跃文件
func (db *DB) setActiveDataFile() error {
	var initFileID uint32 = 0
	// 如果活跃文件不为空，则新活跃文件ID为上一活跃文件ID+1
	if db.ActiveFile != nil {
		initFileID = db.ActiveFile.FileID + 1
	}

	daatFile, err := datafile.OpenDataFile(db.Opts.DirPath, initFileID)
	if err != nil {
		return err
	}

	db.ActiveFile = daatFile

	return nil
}

func (db *DB) AppendKvPair(kvPair *KvPair) (*KvPairPos, error) {
	// 判断当前活跃数据文件是否存在
	// 如果为空则初始化数据文件
	if db.ActiveFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 对写入数据编码
	encodeKvPair := kvPair.EncodeKvPair()
	size := kvPair.Size()
	// 如果写入数据已经达到活跃文件的阈值，关闭活跃文件，打开新文件
	if db.ActiveFile.WriteOffset+int64(size) > db.Opts.DataFileSize {
		//	先持久化数据文件，保证已有的数据持久到磁盘中
		if err := db.ActiveFile.Sync(); err != nil {
			return nil, err
		}

		// 将当前活跃文件转换为旧的数据文件
		db.OlderFiles[db.ActiveFile.FileID] = db.ActiveFile

		// 打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 要在写入前记录WriteOffset
	pos := datafile.NewKvPairPos(db.ActiveFile.FileID, db.ActiveFile.WriteOffset, uint32(size))

	if err := db.ActiveFile.Write(encodeKvPair); err != nil {
		return nil, err
	}

	db.bytesWrite += uint(size)
	//根据用户配置决定是否持久化
	needSync := db.Opts.SyncWrites
	if !needSync && db.Opts.BytesPerSync > 0 && db.bytesWrite > db.Opts.BytesPerSync {
		needSync = true
	}
	if needSync {
		if err := db.ActiveFile.Sync(); err != nil {
			return nil, err
		}
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}

	table := crc16.MakeTable(crc16.CRC16_MODBUS)
	// 计算key的crc，将其映射至不同的slot里
	keyCrc := crc16.Checksum(kvPair.Key, table)
	slot := keyCrc % 16384
	if kvPair.Type == KvPairPuted {
		db.KeySlots[slot] = append(db.KeySlots[slot], &kvPair.Key)
	} else if kvPair.Type == KvPairDeleted {
		for idx, deleteKey := range db.KeySlots[slot] {
			if bytes.Equal(kvPair.Key, *deleteKey) {
				db.KeySlots[slot] = append(db.KeySlots[slot][:idx], db.KeySlots[slot][idx+1:]...)
				break
			}
		}
	}

	return &pos, nil
}

// GetValueByPosition
// 从db的数据文件中通过Position寻找value
// 应该属于是db的操作，因为是db负责存储数据文件
func (db *DB) GetValueByPosition(kvPairPos *KvPairPos) ([]byte, error) {

	kvPair, err := db.GetKvPairByPosition(kvPairPos)
	if err != nil {
		return nil, err
	}

	if kvPair.IsExpired() {
		expireTime := time.UnixMilli(int64(kvPair.Timestamp))
		expireTime = expireTime.Add(time.Duration(kvPair.TTL) * time.Second)
		db.Index.Delete(kvPair.Key)
		return nil, ErrKeyNotFound
	}

	return kvPair.Value, nil
}

func (db *DB) GetKvPairByPosition(kvPairPos *KvPairPos) (*KvPair, error) {
	// 根据文件id找到对应的数据文件
	var dataFile *DataFile
	if db.ActiveFile.FileID == kvPairPos.Fid {
		dataFile = db.ActiveFile
	} else {
		dataFile = db.OlderFiles[kvPairPos.Fid]
	}

	// 数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移读取对应的数据
	kvPair, err := dataFile.ReadKvPair(kvPairPos.Offset)
	if err != nil {
		return nil, err
	}

	if kvPair.Type == KvPairDeleted {
		return nil, ErrKeyNotFound
	}

	return kvPair, nil
}
