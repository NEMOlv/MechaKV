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
	"bytes"
	"github.com/google/btree"
	"sort"
	"sync"
)

// BTree
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(64),
		lock: new(sync.RWMutex),
	}
}

func (btree *BTree) Put(key []byte, pos *KvPairPos) *KvPairPos {
	btree.lock.Lock()
	item := &Item{Key: key, Pos: pos}
	oldItem := btree.tree.ReplaceOrInsert(item)
	btree.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).Pos
}

func (btree *BTree) Get(key []byte) *KvPairPos {
	inItem := &Item{Key: key}
	outItem := btree.tree.Get(inItem)
	// 判空操作，因为nil值无法进行强转
	if outItem == nil {
		return nil
	}
	return outItem.(*Item).Pos
}

func (btree *BTree) Delete(key []byte) (*KvPairPos, bool) {
	btree.lock.Lock()
	item := &Item{Key: key}
	oldItem := btree.tree.Delete(item)
	btree.lock.Unlock()
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).Pos, true
}

func (btree *BTree) Iterator(reverse bool) IndexIterator {
	if btree.tree == nil {
		return nil
	}
	btree.lock.RLock()
	defer btree.lock.RUnlock()

	return newBTreeIterator(btree.tree, reverse)
}

func (btree *BTree) Size() int {
	return btree.tree.Len()
}

func (btree *BTree) Close() error {
	return nil
}

// btreeIterator
type btreeIterator struct {
	// 当前遍历的下标位置
	currentIndex int
	// 是否反向遍历
	reverse bool
	// key+pos位置索引信息
	values []*Item
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx = 0
	values := make([]*Item, tree.Len())
	saveVlues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}

	if reverse {
		tree.Descend(saveVlues)
	} else {
		tree.Ascend(saveVlues)
	}

	bti := &btreeIterator{
		currentIndex: 0,
		reverse:      reverse,
		values:       values,
	}

	return bti
}

// Rwind 重新回到迭代器的起点
func (bti *btreeIterator) Rewind() {
	bti.currentIndex = 0
}

// Seek 根据传入的key查找到第一个大于（或小于）等于的目标key，从这个key开始遍历
func (bti *btreeIterator) Seek(key []byte) {
	if bti.reverse {
		// 倒序排序：从大到小
		// 查找到第一个小于等于的目标key
		bti.currentIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(key, bti.values[i].Key) >= 0
		})
	} else {
		// 正序排序：从小到大
		// 查找到第一个大于等于的目标key
		bti.currentIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(key, bti.values[i].Key) <= 0
		})
		println(bti.currentIndex)
	}
}

// Next 跳转到下一个 key
func (bti *btreeIterator) Next() {
	bti.currentIndex++
}

// Valid 是否有效，即是否已经遍历完了所有的key，用于退出遍历
func (bti *btreeIterator) Valid() bool {
	return bti.currentIndex < len(bti.values)
}

// Key 当前遍历位置的Key数据
func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.currentIndex].Key
}

// Value 当前遍历位置的Value数据
func (bti *btreeIterator) Value() *KvPairPos {
	return bti.values[bti.currentIndex].Pos
}

// Close 关闭迭代器，释放相应资源
func (bti *btreeIterator) Close() {
	bti.values = nil
}

func (bti *btreeIterator) Iterate() ([]byte, *KvPairPos, bool) {
	if !bti.Valid() {
		return nil, nil, false
	}
	defer bti.Next()

	return bti.Key(), bti.Value(), true
}
