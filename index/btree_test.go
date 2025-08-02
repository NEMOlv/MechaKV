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
	"MechaKV/datafile"
	"github.com/stretchr/testify/assert"
	"strconv"
	"sync"
	"testing"
)

// 测试BTree的Put和Get方法
func TestBTree_PutAndGet(t *testing.T) {
	btree := NewBTree()
	defer func() {
		assert.Nil(t, btree.Close())
	}()

	// 测试正常插入和获取
	key1 := []byte("test_key_1")
	pos1 := &datafile.KvPairPos{
		Fid:    1,
		Offset: 100,
		Size:   20,
	}
	oldPos := btree.Put(key1, pos1)
	assert.Nil(t, oldPos)

	getPos := btree.Get(key1)
	assert.NotNil(t, getPos)
	assert.Equal(t, pos1.Fid, getPos.Fid)
	assert.Equal(t, pos1.Offset, getPos.Offset)
	assert.Equal(t, pos1.Size, getPos.Size)

	// 测试覆盖插入
	pos2 := &datafile.KvPairPos{
		Fid:    2,
		Offset: 200,
		Size:   30,
	}
	oldPos = btree.Put(key1, pos2)
	assert.NotNil(t, oldPos)
	assert.Equal(t, pos1.Fid, oldPos.Fid)

	getPos = btree.Get(key1)
	assert.Equal(t, pos2.Fid, getPos.Fid)
	assert.Equal(t, pos2.Offset, getPos.Offset)
}

// 测试BTree的Delete方法
func TestBTree_Delete(t *testing.T) {
	btree := NewBTree()
	defer func() {
		assert.Nil(t, btree.Close())
	}()

	key := []byte("test_key")
	pos := &datafile.KvPairPos{Fid: 1, Offset: 100}

	// 删除不存在的键
	oldPos, ok := btree.Delete(key)
	assert.Nil(t, oldPos)
	assert.False(t, ok)

	// 插入后删除
	btree.Put(key, pos)
	oldPos, ok = btree.Delete(key)
	assert.True(t, ok)
	assert.Equal(t, pos.Fid, oldPos.Fid)
	assert.Equal(t, pos.Offset, oldPos.Offset)

	// 确认已删除
	getPos := btree.Get(key)
	assert.Nil(t, getPos)
}

// 测试BTree的Size方法
func TestBTree_Size(t *testing.T) {
	btree := NewBTree()
	defer func() {
		assert.Nil(t, btree.Close())
	}()

	assert.Equal(t, 0, btree.Size())

	// 插入10个元素
	for i := 0; i < 10; i++ {
		key := []byte("test_key_" + strconv.Itoa(i))
		println(string(key))
		pos := &datafile.KvPairPos{Fid: uint32(i)}
		btree.Put(key, pos)
	}
	assert.Equal(t, 10, btree.Size())

	// 删除3个元素
	for i := 0; i < 3; i++ {
		key := []byte("test_key_" + strconv.Itoa(i))
		btree.Delete(key)
	}
	assert.Equal(t, 7, btree.Size())

	// 覆盖插入不改变数量
	key := []byte("test_key_3")
	btree.Put(key, &datafile.KvPairPos{Fid: 100})
	assert.Equal(t, 7, btree.Size())
}

// 测试BTree迭代器（正向和反向）
func TestBTree_Iterator(t *testing.T) {
	btree := NewBTree()
	defer func() {
		assert.Nil(t, btree.Close())
	}()

	// 插入有序键
	keys := [][]byte{
		[]byte("a"),
		[]byte("b"),
		[]byte("c"),
		[]byte("d"),
		[]byte("e"),
	}
	for i, key := range keys {
		btree.Put(key, &datafile.KvPairPos{Fid: uint32(i)})
	}

	// 测试正向迭代
	iter := btree.Iterator(false)
	assert.NotNil(t, iter)
	defer iter.Close()

	count := 0
	iter.Rewind()
	for iter.Valid() {
		assert.Equal(t, keys[count], iter.Key())
		assert.Equal(t, uint32(count), iter.Value().Fid)
		iter.Next()
		count++
	}
	assert.Equal(t, 5, count)

	// 测试反向迭代
	iter = btree.Iterator(true)
	assert.NotNil(t, iter)
	defer iter.Close()

	count = 4
	iter.Rewind()
	for iter.Valid() {
		assert.Equal(t, keys[count], iter.Key())
		assert.Equal(t, uint32(count), iter.Value().Fid)
		iter.Next()
		count--
	}
	assert.Equal(t, -1, count)
}

// 测试迭代器的Seek方法
func TestBTree_Iterator_Seek(t *testing.T) {
	btree := NewBTree()
	defer func() {
		assert.Nil(t, btree.Close())
	}()

	// 插入测试数据
	keys := [][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("cherry"),
		[]byte("date"),
		[]byte("elderberry"),
	}
	for i, key := range keys {
		btree.Put(key, &datafile.KvPairPos{Fid: uint32(i)})
	}

	// 正向Seek
	iter := btree.Iterator(false)
	iter.Seek([]byte("cherry"))
	assert.True(t, iter.Valid())
	println(string(iter.Key()))
	assert.Equal(t, []byte("cherry"), iter.Key())
	assert.Equal(t, uint32(2), iter.Value().Fid)

	// 继续遍历剩余元素
	iter.Next()
	assert.Equal(t, []byte("date"), iter.Key())
	iter.Next()
	assert.Equal(t, []byte("elderberry"), iter.Key())

	// 反向Seek
	iter = btree.Iterator(true)
	iter.Seek([]byte("cherry"))
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("cherry"), iter.Key())

	// 继续遍历剩余元素
	iter.Next()
	assert.Equal(t, []byte("banana"), iter.Key())
	iter.Next()
	assert.Equal(t, []byte("apple"), iter.Key())
}

// 测试迭代器的Iterate方法
func TestBTree_Iterator_Iterate(t *testing.T) {
	btree := NewBTree()
	defer func() {
		assert.Nil(t, btree.Close())
	}()

	// 插入测试数据
	keys := [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")}
	for i, key := range keys {
		btree.Put(key, &datafile.KvPairPos{Fid: uint32(i)})
	}

	iter := btree.Iterator(false)
	defer iter.Close()

	// 测试Iterate方法
	resultKeys := make([][]byte, 0)
	for {
		key, _, ok := iter.Iterate()
		if !ok {
			break
		}
		resultKeys = append(resultKeys, key)
	}
	assert.Equal(t, keys, resultKeys)
}

// 测试并发操作
func TestBTree_ConcurrentOperations(t *testing.T) {
	btree := NewBTree()
	defer func() {
		assert.Nil(t, btree.Close())
	}()

	const concurrency = 10
	const operations = 1000
	var wg sync.WaitGroup

	// 并发写入
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < operations; j++ {
				key := []byte("key_" + string(rune(goroutineID)) + "_" + string(rune(j)))
				pos := &datafile.KvPairPos{
					Fid:    uint32(goroutineID),
					Offset: int64(j),
				}
				btree.Put(key, pos)
			}
		}(i)
	}
	wg.Wait()

	// 验证总数
	assert.Equal(t, concurrency*operations, btree.Size())

	// 并发读取
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < operations; j++ {
				key := []byte("key_" + string(rune(goroutineID)) + "_" + string(rune(j)))
				pos := btree.Get(key)
				assert.NotNil(t, pos)
				assert.Equal(t, uint32(goroutineID), pos.Fid)
				assert.Equal(t, int64(j), pos.Offset)
			}
		}(i)
	}
	wg.Wait()

	// 并发删除
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < operations; j++ {
				key := []byte("key_" + string(rune(goroutineID)) + "_" + string(rune(j)))
				_, ok := btree.Delete(key)
				assert.True(t, ok)
			}
		}(i)
	}
	wg.Wait()

	// 验证全部删除
	assert.Equal(t, 0, btree.Size())
}
