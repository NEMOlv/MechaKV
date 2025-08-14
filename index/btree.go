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
	"bytes"
	"github.com/google/btree"
	"sort"
	"sync"
)

// BTree 结构体
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

// NewBTree 新建BTree
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(64),
		lock: new(sync.RWMutex),
	}
}

// Put 添加索引
func (bt *BTree) Put(key []byte, pos *KvPairPos) *KvPairPos {
	bt.lock.Lock()
	item := &Item{Key: key, Pos: pos}
	oldItem := bt.tree.ReplaceOrInsert(item)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).Pos
}

// Get 获取索引
func (bt *BTree) Get(key []byte) *KvPairPos {
	inItem := &Item{Key: key}
	outItem := bt.tree.Get(inItem)
	// 判空操作，因为nil值无法进行强转
	if outItem == nil {
		return nil
	}
	return outItem.(*Item).Pos
}

// Delete 删除索引
func (bt *BTree) Delete(key []byte) (*KvPairPos, bool) {
	bt.lock.Lock()
	item := &Item{Key: key}
	oldItem := bt.tree.Delete(item)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).Pos, true
}

// Iterator 生成一个迭代器
func (bt *BTree) Iterator(reverse bool) IndexIterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()

	return newBTreeIterator(bt.tree, reverse)
}

// Size 返回BTree大小
func (bt *BTree) Size() int {
	return bt.tree.Len()
}

// Close 关闭BTree
func (bt *BTree) Close() error {
	return nil
}

// Ascend 全局升序遍历
func (bt *BTree) Ascend(handleFn func(key []byte, pos *KvPairPos) (bool, error)) []*KvPairPos {
	return bt.Iterate(Ascend, nil, nil, handleFn)
}

// Descend 全局降序遍历
func (bt *BTree) Descend(handleFn func(key []byte, pos *KvPairPos) (bool, error)) []*KvPairPos {
	return bt.Iterate(Descend, nil, nil, handleFn)
}

// AscendRange 范围升序遍历
func (bt *BTree) AscendRange(startKey, endKey []byte, handleFn func(key []byte, pos *KvPairPos) (bool, error)) []*KvPairPos {
	return bt.Iterate(Ascend, startKey, endKey, handleFn)
}

// DescendRange 范围降序遍历
func (bt *BTree) DescendRange(startKey, endKey []byte, handleFn func(key []byte, pos *KvPairPos) (bool, error)) []*KvPairPos {
	return bt.Iterate(Descend, startKey, endKey, handleFn)
}

// AscendGreaterOrEqual 大于等于某个key的升序遍历
func (bt *BTree) AscendGreaterOrEqual(startKey []byte, handleFn func(key []byte, pos *KvPairPos) (bool, error)) []*KvPairPos {
	return bt.Iterate(Ascend, startKey, nil, handleFn)
}

// DescendLessOrEqual 小于等于某个key的降序遍历
func (bt *BTree) DescendLessOrEqual(startKey []byte, handleFn func(key []byte, pos *KvPairPos) (bool, error)) []*KvPairPos {
	return bt.Iterate(Descend, startKey, nil, handleFn)
}

func (bt *BTree) Iterate(iterateType IterateType, startKey, endKey []byte, handleFn func(key []byte, pos *KvPairPos) (bool, error)) []*KvPairPos {
	bt.lock.RLock()
	defer bt.lock.RUnlock()

	var KvPairPosSlice []*KvPairPos

	internalHandleFn := func(item btree.Item) bool {
		ok, err := handleFn(item.(*Item).Key, item.(*Item).Pos)
		if err != nil {
			return false
		}

		if ok {
			kvPairPos := item.(*Item).Pos
			kvPairPos.Key = item.(*Item).Key
			KvPairPosSlice = append(KvPairPosSlice, kvPairPos)
		}
		return ok
	}

	if iterateType == Ascend && startKey != nil && endKey == nil {
		bt.tree.AscendGreaterOrEqual(&Item{Key: startKey}, internalHandleFn)
	} else if iterateType == Ascend && startKey != nil && endKey != nil {
		bt.tree.AscendRange(&Item{Key: startKey}, &Item{Key: endKey}, internalHandleFn)
	} else if iterateType == Ascend && startKey == nil && endKey == nil {
		bt.tree.Ascend(internalHandleFn)
	} else if iterateType == Descend && startKey != nil && endKey == nil {
		bt.tree.DescendLessOrEqual(&Item{Key: startKey}, internalHandleFn)
	} else if iterateType == Descend && startKey != nil && endKey != nil {
		bt.tree.DescendRange(&Item{Key: startKey}, &Item{Key: endKey}, internalHandleFn)
	} else if iterateType == Descend && startKey == nil && endKey == nil {
		bt.tree.Descend(internalHandleFn)
	}
	return KvPairPosSlice
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
