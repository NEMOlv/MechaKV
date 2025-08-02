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

package transaction

import "time"

// TxOptions 事务写配置项
type TxOptions struct {
	// 一个批次当中最大的数据量
	MaxBatchNum uint
	// 提交时是否 sync 持久化
	SyncWrites bool
	Timeout    time.Duration
}

var DefaultTxOptions = &TxOptions{
	// 默认一个事务能提交1w条记录
	MaxBatchNum: 10000,
	// 默认不开启同步磁盘
	SyncWrites: false,
	// 默认不开启事务超时管理
	Timeout: -1,
}

type TxOptionFunc func(*TxOptions) error

func WithMaxBatchNum(MaxBatchNum uint) TxOptionFunc {
	return func(TxOp *TxOptions) error {
		TxOp.MaxBatchNum = MaxBatchNum
		return nil
	}
}

func WithSyncWrites(SyncWrites bool) TxOptionFunc {
	return func(TxOp *TxOptions) error {
		TxOp.SyncWrites = SyncWrites
		return nil
	}
}

func WithTimeout(Timeout time.Duration) TxOptionFunc {
	return func(TxOp *TxOptions) error {
		TxOp.Timeout = Timeout
		return nil
	}
}

// NewTxOption TxOptions 的构造函数
func NewTxOptions(OptionFuncs ...TxOptionFunc) (*TxOptions, error) {
	var opts = &TxOptions{
		MaxBatchNum: DefaultTxOptions.MaxBatchNum,
		SyncWrites:  DefaultTxOptions.SyncWrites,
	}

	for _, OptionFunc := range OptionFuncs {
		if err := OptionFunc(opts); err != nil {
			return nil, err
		}
	}

	return opts, nil
}
