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

package index

import (
	. "MechaKV/comment"
	"MechaKV/datafile"
	"bytes"
	"github.com/google/btree"
)

type (
	KvPairPos = datafile.KvPairPos
)

type Indexer interface {
	Put(bucketID uint64, key []byte, pos *KvPairPos) *KvPairPos
	Get(bucketID uint64, key []byte) *KvPairPos
	Delete(bucketID uint64, key []byte) (*KvPairPos, bool)
	AddBucketBTree(bucketID uint64)
	Iterator(bucketID uint64, reverse bool) IndexIterator
	// 索引中的数据量
	Size(bucketID uint64) int
	// Close 关闭索引
	Close() error
	Iterate(iterateType IterateType, bucketID uint64, startKey, endKey []byte, handleFn func(key []byte, pos *KvPairPos) (bool, error)) []*KvPairPos
	Ascend(bucketID uint64, handleFn func(key []byte, pos *KvPairPos) (bool, error)) []*KvPairPos
	Descend(bucketID uint64, handleFn func(key []byte, pos *KvPairPos) (bool, error)) []*KvPairPos
	AscendRange(bucketID uint64, startKey, endKey []byte, handleFn func(key []byte, pos *KvPairPos) (bool, error)) []*KvPairPos
	DescendRange(bucketID uint64, startKey, endKey []byte, handleFn func(key []byte, pos *KvPairPos) (bool, error)) []*KvPairPos
	AscendGreaterOrEqual(bucketID uint64, startKey []byte, handleFn func(key []byte, pos *KvPairPos) (bool, error)) []*KvPairPos
	DescendLessOrEqual(bucketID uint64, startKey []byte, handleFn func(key []byte, pos *KvPairPos) (bool, error)) []*KvPairPos
}

type Item struct {
	Key []byte
	Pos *KvPairPos
}

func (elem1 *Item) Less(elem2 btree.Item) bool {
	return bytes.Compare(elem1.Key, elem2.(*Item).Key) == -1
}

func NewIndexer(bucketIDToName map[uint64]string) Indexer {
	return NewBTree(bucketIDToName)
}
