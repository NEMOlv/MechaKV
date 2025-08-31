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
	"os"
	"time"
)

type IteratorOptions struct {
	// 遍历前缀位指定值的 Key, 默认为空
	Prefix []byte
	// 是否反向遍历，默认false
	Reverse bool
}

type Options struct {
	// 数据库目录
	DirPath    string
	DataPath   string
	BucketPath string
	// 数据文件大小
	DataFileSize int64
	// 每次写入数据是否持久化
	SyncWrites bool
	// 累计写到多少字节后进行持久化
	BytesPerSync uint
	// 可以同时实现定时任务与固定时间间隔任务
	// 合并操作的定时表达式
	MergeCronExpr string
	// 合并操作的时间间隔
	MergeInterval time.Duration
	// 合并操作需达到的比率
	DataFileMergeRatio float32
	// MachaKv的首次启动时间，一旦设定无法更改
	// 如果要更改，无法保证历史数据事务ID的准确性
	TxEpoch      int64
	IteratorOpts IteratorOptions
}

var DefaultOptions = Options{
	DirPath: os.TempDir(),
	// 数据文件大小：默认256MB
	DataFileSize: 256 * 1024 * 1024,
	// 每条数据持久化：默认不开启
	SyncWrites: false,
	// N字节数据持久化：默认不开启
	BytesPerSync: 0,
	// 合并的时间间隔
	MergeInterval: 0 * time.Hour,
	//	数据文件合并的阈值
	DataFileMergeRatio: 0.5,
	IteratorOpts: IteratorOptions{
		Reverse: false,
		Prefix:  []byte(""),
	},
}

func checkOptions(opts Options) error {
	if opts.DirPath == "" {
		return ErrDirPathIsEmpty
	}
	if opts.DataFileSize <= 0 {
		return ErrFileSizeIsLessThanZero
	}
	if opts.DataFileMergeRatio < 0 || opts.DataFileMergeRatio > 1 {
		return ErrInvalidMergeRatio
	}
	return nil
}

// WriteBatchOptions 批量写配置项
type WriteBatchOptions struct {
	// 一个批次当中最大的数据量
	MaxBatchNum uint

	// 提交时是否 sync 持久化
	SyncWrites bool
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}

type IndexerType = int8
