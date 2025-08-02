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

package iterator

import (
	"MechaKV/database"
	"MechaKV/index"
	"bytes"
)

type (
	DB            = database.DB
	IndexIterator = index.IndexIterator
)

type Iterator struct {
	db      *DB
	idxIter IndexIterator
}

func NewIterator(db *DB) *Iterator {
	return &Iterator{
		db:      db,
		idxIter: db.Index.Iterator(db.Opts.IteratorOpts.Reverse),
	}
}

// Rwind 重新回到迭代器的起点
func (it *Iterator) Rwind() {
	it.idxIter.Rewind()
	it.skipToNext()
}

// Seek 根据传入的key查找到第一个大于（或小于）等于的目标key，从这个key开始遍历
func (it *Iterator) Seek(key []byte) {
	it.idxIter.Seek(key)
	it.skipToNext()
}

// Next 跳转到下一个 key
func (it *Iterator) Next() {
	it.idxIter.Next()
	it.skipToNext()
}

// Valid 是否有效，即是否已经遍历完了所有的key，用于退出遍历
func (it *Iterator) Valid() bool {
	return it.idxIter.Valid()
}

// Key 当前遍历位置的Key数据
func (it *Iterator) Key() []byte {
	return it.idxIter.Key()
}

// Value 当前遍历位置的Value数据
func (it *Iterator) Value() ([]byte, error) {
	kvPairPos := it.idxIter.Value()
	it.db.DBLock.RLock()
	defer it.db.DBLock.RUnlock()
	return it.db.GetValueByPosition(kvPairPos)
}

// Close 关闭迭代器，释放相应资源
func (it *Iterator) Close() {
	it.idxIter.Close()
}

func (it *Iterator) Iterate() ([]byte, []byte, bool) {
	key, kvPairPos, ok := it.idxIter.Iterate()
	if !ok {
		return nil, nil, false
	}
	it.db.DBLock.RLock()
	defer it.db.DBLock.RUnlock()
	value, err := it.db.GetValueByPosition(kvPairPos)
	if err != nil {
		return nil, nil, false
	}
	return key, value, true
}

func (it *Iterator) skipToNext() {
	prefixLen := len(it.db.Opts.IteratorOpts.Prefix)
	if prefixLen == 0 {
		return
	}

	for ; it.idxIter.Valid(); it.idxIter.Next() {
		key := it.idxIter.Key()
		if prefixLen <= len(key) && bytes.Compare(it.db.Opts.IteratorOpts.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
