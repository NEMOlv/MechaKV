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

package comment

import (
	"encoding/binary"
	"math"
)

const MaxKvPairHeaderSize int64 = 4 + 1 + 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32*3

const (
	MergeDirName     = "-merge"
	MergeFinishedKey = "merge-finished"
)

const PERSISTENT = 0

type PutCondition uint8

const (
	PUT_NORMAL PutCondition = iota
	PUT_IF_NOT_EXISTS
	PUT_IF_EXISTS
	PUT_AND_RETURN_OLD_VALUE
	UPDATE_TTL
	APPEND_VALUE
)

type GetCondition uint8

const (
	FileLockName = "flock"
)

//const NonTransactionSeqNo uint64 = 0

type TransactionStatus int

const (
	// TransactionRunning 代表事务正在执行
	TransactionRunning TransactionStatus = iota
	// TransactionCommitting 代表事务正在提交
	TransactionCommitting
	// TransactionClosed 代表事务已关闭，可能是提交结束也可能是回滚
	TransactionClosed
)

type KvPairType = uint8

const (
	KvPairPuted KvPairType = iota
	KvPairDeleted
	TxFinished
)

const DataFilePermision = 0644

const (
	MaxMetadataSize   = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	ExtraListMetaSize = binary.MaxVarintLen64 * 2

	InitialListMark = math.MaxUint64 / 2
)

type RedisDataType = byte

const (
	Set RedisDataType = iota
	Hash
	List
	ZSet
)

type Aggregate = byte

const (
	Sum Aggregate = iota
	Max
	Min
)

type IterateType = byte

const (
	Ascend IterateType = iota
	Descend
)
