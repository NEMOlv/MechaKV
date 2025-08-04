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
	"MechaKV/utils"
	"fmt"
	"github.com/bwmarrin/snowflake"
	"github.com/gofrs/flock"
	"github.com/robfig/cron/v3"
	"github.com/valyala/bytebufferpool"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

func Open(options Options) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 判断当前数据目录是否存在，如果不存在，则创建这个目录
	var isInit bool
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInit = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 判断当前数据目录是否正在使用
	fileLock := flock.New(filepath.Join(options.DirPath, FileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	// 如果无法持有锁，证明别的进程在使用该目录
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	// 有目录无文件也视作初始化
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInit = true
	}

	// 初始化DB实例结构体
	mergeManager := MergeManager{isMerging: false}
	expiryMonitor := NewExpiryMonitor()
	snowflake.Epoch = options.TxEpoch
	TxIdGenerater, err := snowflake.NewNode(0)
	if err != nil {
		return nil, err
	}

	db := &DB{
		// 配置项
		Opts: options,
		// 数据库锁
		DBLock:        new(sync.RWMutex),
		OlderFiles:    make(map[uint32]*datafile.DataFile),
		Index:         index.NewIndexer(),
		KeySlots:      make(map[uint16]keyPointer, 16384),
		isInitial:     isInit,
		fileLock:      fileLock,
		mergeManager:  mergeManager,
		expiryMonitor: expiryMonitor,
		TxIdGenerater: TxIdGenerater,
		KvPairHeader:  make([]byte, MaxKvPairHeaderSize),
	}

	// 加载merge目录中的数据文件
	if err := db.loadMergedFiles(); err != nil {
		return nil, err
	}

	// 加载data目录中的数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 从 hint 索引文件中加载索引
	if err := db.loadIndexFromHintFile(); err != nil {
		return nil, err
	}

	// 从数据文件中加载索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	if db.ActiveFile != nil {
		size, err := db.ActiveFile.IOManager.Size()
		if err != nil {
			return nil, err
		}
		db.ActiveFile.WriteOffset = size
	}

	// 自动合并任务
	err = db.StartMergeScheduler()
	if err != nil {
		return nil, err
	}

	// 自动过期任务
	go db.StartExpiryMonitor()

	return db, nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	// 关闭文件锁
	defer func() {
		db.CloseExpiryMonitor()
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock the directory: %v", err))
		}
	}()

	// 如果没有当前活跃文件，说明数据库无数据，直接返回
	if db.ActiveFile == nil {
		return nil
	}

	// 数据库加锁，准备进行关闭操作
	db.DBLock.Lock()
	defer db.DBLock.Unlock()

	// 关闭索引
	if err := db.Index.Close(); err != nil {
		return err
	}

	// 关闭当前活跃文件
	if err := db.ActiveFile.Close(); err != nil {
		return err
	}
	// 关闭历史文件
	for _, file := range db.OlderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync 持久化当前活跃文件
// 历史文件不需要持久化，因为历史文件已经进行过持久化
func (db *DB) Sync() error {
	// 如果没有当前活跃文件，说明数据库无数据，直接返回
	if db.ActiveFile == nil {
		return nil
	}

	// 加锁
	db.DBLock.Lock()
	defer db.DBLock.Unlock()
	return db.ActiveFile.Sync()
}

// Backup 备份数据库，将数据文件拷贝到新的目录中
func (db *DB) Backup(dest string) error {
	// 如果没有当前活跃文件，说明数据库无数据，直接返回
	if db.ActiveFile == nil {
		return nil
	}

	db.DBLock.RLock()
	defer db.DBLock.RUnlock()
	return utils.CopyData(db.Opts.DirPath, dest, []string{FileLockName})
}

// Merge 对外暴露的手动合并函数
func (db *DB) Merge() error {
	return db.merge()
}

func (db *DB) merge() error {
	// ===============merge预备工作开始===========================
	// 如果数据库为空，直接返回
	if db.ActiveFile == nil {
		return nil
	}
	// 加锁
	db.DBLock.Lock()

	// 如果merge正在进行当中，则直接返回
	if db.mergeManager.isMerging {
		db.DBLock.Unlock()
		return ErrMergeIsProgress
	}

	// 查看可以 merge 的数据量是否达到了阈值
	totalSize, err := utils.GetDiskSize(db.Opts.DirPath)
	if err != nil {
		db.DBLock.Unlock()
		return err
	}
	if float32(db.ReclaimSize)/float32(totalSize) < db.Opts.DataFileMergeRatio {
		db.DBLock.Unlock()
		return ErrMergeRatioUnreached
	}

	// 查看剩余的空间容量是否可以容纳 merge 之后的数据量
	availableDiskSize, err := utils.GetAvailableDiskSize()
	if err != nil {
		db.DBLock.Unlock()
		return err
	}
	if uint64(totalSize-db.ReclaimSize) >= availableDiskSize {
		db.DBLock.Unlock()
		return ErrNoEnoughSpaceForMerge
	}

	// 标注merge标识，并在函数执行结束后修改
	db.mergeManager.isMerging = true
	defer func() {
		db.mergeManager.isMerging = false
	}()

	// 持久化当前活跃文件
	if err := db.ActiveFile.Sync(); err != nil {
		db.DBLock.Unlock()
		return err
	}
	// 将当前活跃文件转为历史数据文件
	db.OlderFiles[db.ActiveFile.FileID] = db.ActiveFile
	// 打开新的活跃文件
	// 新活跃文件不参与merge
	if err := db.setActiveDataFile(); err != nil {
		db.DBLock.Unlock()
		return err
	}
	// 记录最近没有参与merge的文件id
	nonMergeFileID := db.ActiveFile.FileID

	// 取出所有需要merge的文件
	var mergeFiles []*datafile.DataFile
	for _, file := range db.OlderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	// 释放锁，允许数据库执行其他操作
	db.DBLock.Unlock()

	// 将要merge的文件从小到大排序，依次合并
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileID < mergeFiles[j].FileID
	})

	mergePath := db.getMergePath()

	// 如果merge目录存在，说明发生过失败的merge，对merge目录进行清理
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	// ===============merge预备工作结束===========================

	// 打开一个新的临时数据库(bitcask)实例
	mergeOptions := db.Opts
	mergeOptions.DirPath = mergePath
	// merge文件要全部历史记录成功才算成功，因此没必要每一次都落盘
	mergeOptions.SyncWrites = false
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}

	// 打开 hint 文件存储索引
	hintFile, err := datafile.OpenHintFile(mergePath)
	if err != nil {
		return err
	}
	var KvPairHeader []byte
	var KvPairBuffers []*bytebufferpool.ByteBuffer

	// 遍历处理每个数据文件
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			kvPair, err := dataFile.ReadKvPair(offset)
			if err != nil {
				// 文件读取完毕，跳出循环
				if err == io.EOF {
					break
				}
				return err
			}

			size := kvPair.Size()

			// 获取内存索引中的数据
			kvPairPos := db.Index.Get(kvPair.Key)
			// 判断数据是否有效
			// 举例该数据已经被删除，那么数据文件中至少存在一条新增记录和一条删除记录
			// 而内存索引中，被删除的数据不存在对应记录
			// 因此，用数据文件中的记录与内存索引中的记录进行比对，如果不对应，则该数据被视作删除，不写入到merge文件中
			if kvPairPos != nil &&
				kvPairPos.Fid == dataFile.FileID &&
				kvPairPos.Offset == offset {
				// 清除事务标记
				KvPairBuffer := bytebufferpool.Get()
				KvPairBuffers = append(KvPairBuffers, KvPairBuffer)
				kvPairPos, err = mergeDB.AppendKvPair(kvPair, KvPairHeader, KvPairBuffer)
				if err != nil {
					return err
				}
				KvPairBuffer.Reset()
				// 将当前位置索引写道 Hint 文件中
				if err = hintFile.WriteHintKvPair(kvPair.Key, kvPairPos, KvPairHeader, KvPairBuffer); err != nil {
					return err
				}
			}
			offset += int64(size)
		}
		for _, KvPairBuffer := range KvPairBuffers {
			bytebufferpool.Put(KvPairBuffer)
		}
		clear(KvPairBuffers)
	}

	// Sync 保持持久化
	if err = hintFile.Sync(); err != nil {
		return err
	}
	if err = mergeDB.Sync(); err != nil {
		return err
	}

	// 写标识merge完成的文件
	mergeFinishedFile, err := datafile.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	if err = mergeFinishedFile.WriteMergeFinished(int(nonMergeFileID)); err != nil {
		return err
	}

	return nil
}

func (db *DB) StartMergeScheduler() (err error) {
	if len(db.Opts.MergeCronExpr) > 0 || db.Opts.MergeInterval > 0 {
		db.mergeManager.cronScheduler = cron.New(
			cron.WithParser(
				cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour |
					cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
			),
		)

		if len(db.Opts.MergeCronExpr) > 0 {
			_, err = db.mergeManager.cronScheduler.AddFunc(db.Opts.MergeCronExpr, func() {
				_ = db.merge()
			})
		}

		mergeIntervalExpr := fmt.Sprintf("@every %s", db.Opts.MergeInterval.String())
		if db.Opts.MergeInterval > 0 {
			_, err = db.mergeManager.cronScheduler.AddFunc(mergeIntervalExpr, func() {
				_ = db.merge()
			})
		}

		if err != nil {
			return err
		}

		db.mergeManager.cronScheduler.Start()
	}
	return
}

// 引入corn，考虑移除
//func (database *DB) mergeWorker() {
//	var ticker *time.Ticker
//
//	if database.Opts.MergeInterval != 0 {
//		ticker = time.NewTicker(database.Opts.MergeInterval)
//	} else {
//		ticker = time.NewTicker(math.MaxInt)
//		ticker.Stop()
//	}
//
//	for {
//		select {
//		case <-database.mergeManager.startCh:
//			database.mergeManager.endCh <- database.merge()
//			if database.Opts.MergeInterval != 0 {
//				ticker.Reset(database.Opts.MergeInterval)
//			}
//		case <-ticker.C:
//			err := database.merge()
//			if err != nil {
//				println(err)
//			}
//			//if err != nil {log.Fatal(err)}
//		case <-database.mergeManager.closeCh:
//			return
//		}
//	}
//}
