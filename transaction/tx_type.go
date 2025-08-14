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

import (
	. "MechaKV/comment"
	"MechaKV/utils"
	"bytes"
	"encoding/binary"
	"errors"
	"sort"
	"time"
)

// 遍历
// AscendGreaterOrEqual 遍历事务中所有大于等于指定key的键值对（升序）
// 结合事务内pendingWrites和底层索引，保证事务隔离性

// string
func (tx *Transaction) put(key, value []byte, ttl uint32, timestamp uint64) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	kvPair := tx.tm.KvPairPool.Get().(*KvPair)
	kvPair.TxId, kvPair.Type = tx.id, KvPairPuted
	kvPair.Key, kvPair.Value = key, value
	kvPair.KeySize, kvPair.ValueSize = uint32(len(key)), uint32(len(value))
	kvPair.Timestamp, kvPair.TTL = timestamp, ttl

	tx.pendingWrites[string(key)] = kvPair
	return nil
}

func (tx *Transaction) delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// key不存在则直接返回
	// 查找索引
	if tx.tm.db.Index.Get(key) == nil {
		// 查找待写数组
		if tx.pendingWrites[string(key)] == nil {
			return ErrKeyNotFound
		}
	}

	kvPair := &KvPair{
		Type: KvPairDeleted,
		TxId: tx.id,
		Key:  key,
	}

	tx.pendingWrites[string(key)] = kvPair

	// 将删除的数据写入待写数组
	return nil
}

func (tx *Transaction) get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从当前事务的pendingWrites中查找
	// 对一个事务而言，可以看见自己所做的“未提交”操作
	kvPair := tx.pendingWrites[string(key)]
	if kvPair != nil {
		if kvPair.Type == KvPairPuted {
			return kvPair.Value, nil
		} else if kvPair.Type == KvPairDeleted {
			return nil, ErrKeyNotFound
		}
	}

	// 从内存数据结构中取出 key 对应的索引信息
	kvPairPos := tx.tm.db.Index.Get(key)
	// 如果key不在内存索引中，说明 key 不存在
	if kvPairPos == nil {
		return nil, ErrKeyNotFound
	}

	// 从数据库中查找
	return tx.tm.db.GetValueByPosition(kvPairPos)
}

func (tx *Transaction) getKvPair(key []byte) (*KvPair, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从当前事务的pendingWrites中查找
	// 对一个事务而言，可以看见自己所做的“未提交”操作
	kvPair := tx.pendingWrites[string(key)]
	if kvPair != nil {
		if kvPair.Type == KvPairPuted {
			return kvPair, nil
		} else if kvPair.Type == KvPairDeleted {
			return nil, ErrKeyNotFound
		}
	}

	// 从内存数据结构中取出 key 对应的索引信息
	kvPairPos := tx.tm.db.Index.Get(key)
	// 如果key不在内存索引中，说明 key 不存在
	if kvPairPos == nil {
		return nil, ErrKeyNotFound
	}

	// 从数据库中查找
	return tx.tm.db.GetKvPairByPosition(kvPairPos)
}

func (tx *Transaction) Put(key, value []byte, ttl uint32, timestamp uint64, condition PutCondition) (oldValue []byte, err error) {
	if condition == PUT_NORMAL {
		PutNormal := func() error {
			return tx.put(key, value, ttl, timestamp)
		}

		err = tx.managed(PutNormal)
	} else if condition == PUT_IF_NOT_EXISTS {
		PutIfNotExists := func() error {
			getValue, getErr := tx.get(key)
			if getErr != nil && !errors.Is(getErr, ErrKeyNotFound) {
				return getErr
			}
			// 如果Key已经存在，则直接返回nil
			if getValue != nil {
				return nil
			}
			return tx.put(key, value, ttl, timestamp)
		}

		err = tx.managed(PutIfNotExists)
	} else if condition == PUT_IF_EXISTS {
		PutIfNotExists := func() error {
			_, getErr := tx.get(key)
			if getErr != nil {
				return getErr
			}
			return tx.put(key, value, ttl, timestamp)
		}

		err = tx.managed(PutIfNotExists)
	} else if condition == PUT_AND_RETURN_OLD_VALUE {
		PutAndReturnOldValue := func() error {
			var getErr error
			oldValue, getErr = tx.get(key)
			if getErr != nil && !errors.Is(getErr, ErrKeyNotFound) {
				return getErr
			}
			return tx.put(key, value, ttl, timestamp)
		}

		err = tx.managed(PutAndReturnOldValue)
	} else if condition == APPEND_VALUE {
		AppendValue := func() error {
			var getErr error
			oldValue, getErr = tx.get(key)
			if getErr != nil && !errors.Is(getErr, ErrKeyNotFound) {
				return getErr
			}
			if oldValue != nil {
				value = append(oldValue, value...)
			}

			return tx.put(key, value, ttl, timestamp)
		}
		err = tx.managed(AppendValue)
	} else if condition == UPDATE_TTL {
		UpdateTTL := func() error {
			var getErr error
			oldValue, getErr = tx.get(key)
			if getErr != nil {
				return getErr
			}
			return tx.put(key, oldValue, ttl, timestamp)
		}
		err = tx.managed(UpdateTTL)
	}

	if err != nil {
		return nil, err
	}
	return
}

func (tx *Transaction) BatchPut(key, value [][]byte, ttl uint32, timestamp uint64, condition PutCondition) (oldValue [][]byte, err error) {
	if condition == PUT_NORMAL {
		BatchPut := func() (err error) {
			for i := 0; i < len(key); i++ {
				if err = tx.put(key[i], value[i], ttl, timestamp); err != nil {
					return
				}
			}
			return
		}

		err = tx.managed(BatchPut)
	} else if condition == PUT_IF_NOT_EXISTS {
		BatchPutIfNotExists := func() (err error) {
			for i := 0; i < len(key); i++ {
				getValue, getErr := tx.get(key[i])
				if getErr != nil && !errors.Is(getErr, ErrKeyNotFound) {
					return getErr
				}
				// 如果Key已经存在，则直接返回nil
				if getValue != nil {
					return
				}
				if err = tx.put(key[i], value[i], ttl, timestamp); err != nil {
					return
				}
			}
			return
		}

		err = tx.managed(BatchPutIfNotExists)
	} else if condition == PUT_IF_EXISTS {
		BatchPutIfExists := func() (err error) {
			for i := 0; i < len(key); i++ {
				_, getErr := tx.get(key[i])
				if getErr != nil {
					return getErr
				}
				if err = tx.put(key[i], value[i], ttl, timestamp); err != nil {
					return
				}
			}
			return
		}

		err = tx.managed(BatchPutIfExists)
	} else if condition == PUT_AND_RETURN_OLD_VALUE {
		BatchPutAndReturnOldValue := func() (err error) {
			var getErr error
			var tempValue []byte
			for i := 0; i < len(key); i++ {
				tempValue, getErr = tx.get(key[i])
				if getErr != nil && !errors.Is(getErr, ErrKeyNotFound) {
					return getErr
				}
				if err = tx.put(key[i], value[i], ttl, timestamp); err != nil {
					oldValue = nil
					return
				}
				oldValue = append(oldValue, tempValue)
			}
			return
		}

		err = tx.managed(BatchPutAndReturnOldValue)
	} else if condition == UPDATE_TTL {
		BatchUpdateTTL := func() (err error) {
			var getErr error
			var tempValue []byte
			for i := 0; i < len(key); i++ {
				tempValue, getErr = tx.get(key[i])
				if getErr != nil {
					return getErr
				}
				if err = tx.put(key[i], tempValue, ttl, timestamp); err != nil {
					return
				}
			}
			return
		}
		err = tx.managed(BatchUpdateTTL)
	}

	if err != nil {
		return nil, err
	}

	return
}

func (tx *Transaction) Delete(key []byte) (err error) {
	err = tx.managed(func() (err error) {
		err = tx.delete(key)
		return err
	})
	return
}

func (tx *Transaction) BatchDelete(key [][]byte) (err error) {
	err = tx.managed(func() (err error) {
		for i := 0; i < len(key); i++ {
			if err = tx.delete(key[i]); err != nil {
				return
			}
		}
		return
	})
	return
}

func (tx *Transaction) Get(key []byte) (value []byte, err error) {
	err = tx.managed(func() (err error) {
		value, err = tx.get(key)
		return
	})
	return
}

func (tx *Transaction) GetKvPair(key []byte) (kvPair *KvPair, err error) {
	err = tx.managed(func() (err error) {
		kvPair, err = tx.getKvPair(key)
		return
	})
	return
}

func (tx *Transaction) BatchGet(key [][]byte) (value [][]byte, err error) {
	err = tx.managed(func() (err error) {
		var tempValue []byte
		for i := 0; i < len(key); i++ {
			if tempValue, err = tx.get(key[i]); err != nil {
				return
			}
			value = append(value, tempValue)
		}
		return
	})
	return
}

// Set
func (tx *Transaction) SAdd(key, member []byte) (exist bool, err error) {
	SAdd := func() error {
		meta, err := tx.findMetadata(key, Set)
		if err != nil {
			return err
		}

		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(meta.version))
		encSetKey := bytes.Join([][]byte{key, version, member}, nil)

		exist = true
		_, err = tx.get(encSetKey)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return err
		}
		if err != nil && errors.Is(err, ErrKeyNotFound) {
			exist = false
			meta.size++
			err = tx.put(key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()))
			if err != nil {
				return err
			}
			err = tx.put(encSetKey, nil, PERSISTENT, uint64(time.Now().UnixMilli()))
			if err != nil {
				return err
			}
		}
		return nil
	}

	err = tx.managed(SAdd)
	return
}

func (tx *Transaction) SIsMember(key, member []byte) (exist bool, err error) {
	SIsMember := func() error {
		meta, err := tx.findMetadata(key, Set)
		if err != nil {
			return err
		}
		if meta.size == 0 {
			return nil
		}
		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(meta.version))
		encSetKey := bytes.Join([][]byte{key, version, member}, nil)

		exist = true
		_, err = tx.get(encSetKey)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return err
		}
		if err != nil && errors.Is(err, ErrKeyNotFound) {
			exist = false
		}
		return nil
	}

	err = tx.managed(SIsMember)

	return
}

func (tx *Transaction) SRem(key, member []byte) (exist bool, err error) {
	SRem := func() error {
		meta, err := tx.findMetadata(key, Set)
		if err != nil {
			return err
		}
		if meta.size == 0 {
			return nil
		}
		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(meta.version))
		encSetKey := bytes.Join([][]byte{key, version, member}, nil)

		exist = true
		_, err = tx.get(encSetKey)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return err
		}
		if err != nil && errors.Is(err, ErrKeyNotFound) {
			exist = false
			return nil
		}

		meta.size--
		err = tx.put(key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()))
		if err != nil {
			return err
		}
		err = tx.delete(encSetKey)
		if err != nil {
			return err
		}

		return nil
	}

	err = tx.managed(SRem)

	return
}

// Hash
func (tx *Transaction) HSet(key, field, value []byte) (notExist bool, err error) {
	HSet := func() error {
		// 从Value中查找元数据
		meta, err := tx.findMetadata(key, Hash)
		if err != nil {
			return err
		}
		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(meta.version))
		// 构造 Hash的 SubKey
		encHashSubKey := bytes.Join([][]byte{key, version, field}, nil)

		if _, err = tx.get(encHashSubKey); errors.Is(err, ErrKeyNotFound) {
			notExist = true
		}

		// 不存在则更新元数据
		if notExist {
			meta.size++
			err = tx.put(key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()))
			if err != nil {
				return err
			}
		}
		err = tx.put(encHashSubKey, value, PERSISTENT, uint64(time.Now().UnixMilli()))
		if err != nil {
			return err
		}

		return nil
	}

	err = tx.managed(HSet)

	return
}

func (tx *Transaction) HGet(key, field []byte) (value []byte, err error) {
	HGet := func() error {
		meta, err := tx.findMetadata(key, Hash)
		if err != nil {
			return err
		}

		// 这里应该返回一个ErrKeyNotFound
		if meta.size == 0 {
			return nil
		}
		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(meta.version))
		// 构造 Hash的 SubKey
		encHashSubKey := bytes.Join([][]byte{key, version, field}, nil)

		value, err = tx.get(encHashSubKey)
		if err != nil {
			return err
		}

		return nil
	}

	err = tx.managed(HGet)

	return
}

func (tx *Transaction) HDel(key, field []byte) (exist bool, err error) {
	HDel := func() error {
		meta, err := tx.findMetadata(key, Hash)
		if err != nil {
			return err
		}
		if meta.size == 0 {
			return nil
		}

		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(meta.version))
		// 构造 Hash的 SubKey
		encHashSubKey := bytes.Join([][]byte{key, version, field}, nil)

		// 先查看是否存在
		exist = true
		_, err = tx.get(encHashSubKey)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return err
		}
		if err != nil && errors.Is(err, ErrKeyNotFound) {
			exist = false
			return nil
		}

		meta.size--
		err = tx.put(key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()))
		if err != nil {
			return err
		}
		err = tx.delete(encHashSubKey)
		if err != nil {
			return err
		}

		return nil
	}

	err = tx.managed(HDel)

	return
}

// List
func (tx *Transaction) push(key, value []byte, isLeft bool) (uint32, error) {
	// 查找元数据
	meta, err := tx.findMetadata(key, List)
	if err != nil {
		return 0, err
	}

	var index uint64
	if isLeft {
		index = meta.head - 1
	} else {
		index = meta.tail
	}

	byteBuf := make([]byte, 16)
	binary.LittleEndian.PutUint64(byteBuf, uint64(meta.version))
	binary.LittleEndian.PutUint64(byteBuf, index)
	encListKey := bytes.Join([][]byte{key, byteBuf}, nil)

	// 更新元数据和数据部分
	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}
	err = tx.put(key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()))
	if err != nil {
		return 0, err
	}
	err = tx.put(encListKey, value, PERSISTENT, uint64(time.Now().UnixMilli()))
	if err != nil {
		return 0, err
	}

	return meta.size, nil
}

func (tx *Transaction) pop(key []byte, isLeft bool) ([]byte, error) {
	// 查找元数据
	meta, err := tx.findMetadata(key, List)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	var index uint64
	if isLeft {
		index = meta.head
	} else {
		index = meta.tail - 1
	}

	byteBuf := make([]byte, 16)
	binary.LittleEndian.PutUint64(byteBuf, uint64(meta.version))
	binary.LittleEndian.PutUint64(byteBuf, index)
	encListKey := bytes.Join([][]byte{key, byteBuf}, nil)

	value, err := tx.get(encListKey)
	if err != nil {
		return nil, err
	}

	// 更新元数据
	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}

	err = tx.put(key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()))
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (tx *Transaction) LPush(key, element []byte) (size uint32, err error) {
	LPush := func() error {
		size, err = tx.push(key, element, true)
		if err != nil {
			return err
		}
		return nil
	}
	err = tx.managed(LPush)
	return
}

func (tx *Transaction) RPush(key, element []byte) (size uint32, err error) {
	RPush := func() error {
		size, err = tx.push(key, element, false)
		if err != nil {
			return err
		}
		return nil
	}
	err = tx.managed(RPush)
	return
}

func (tx *Transaction) LPop(key []byte) (value []byte, err error) {
	LPop := func() error {
		value, err = tx.pop(key, true)
		if err != nil {
			return err
		}
		return nil
	}
	err = tx.managed(LPop)
	return
}

func (tx *Transaction) RPop(key []byte) (value []byte, err error) {
	RPop := func() error {
		value, err = tx.pop(key, false)
		if err != nil {
			return err
		}
		return nil
	}
	err = tx.managed(RPop)
	return
}

// ZSet
func (tx *Transaction) ZAdd(key []byte, score float64, member []byte) (notExist bool, err error) {
	ZAdd := func() error {
		meta, err := tx.findMetadata(key, ZSet)
		if err != nil {
			return err
		}

		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(meta.version))
		encZSetKey := bytes.Join([][]byte{key, version, member}, nil)
		// 查看是否已经存在
		value, err := tx.get(encZSetKey)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return err
		}
		if errors.Is(err, ErrKeyNotFound) {
			notExist = true
		}
		if !notExist {
			if score == utils.BytesToFloat64(value) {
				return nil
			}
		}

		if notExist {
			meta.size++
			err := tx.put(key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()))
			if err != nil {
				return err
			}
		}
		if !notExist {
			err = tx.delete(encZSetKey)
			if err != nil {
				return err
			}
		}
		err = tx.put(encZSetKey, utils.Float64ToBytes(score), PERSISTENT, uint64(time.Now().UnixMilli()))
		if err != nil {
			return err
		}

		err = tx.put(encZSetKey, value, PERSISTENT, uint64(time.Now().UnixMilli()))
		encZSetKeyWithScore := bytes.Join([][]byte{key, version, value, member}, nil)
		_ = tx.put(encZSetKeyWithScore, nil, PERSISTENT, uint64(time.Now().UnixMilli()))

		return nil
	}

	err = tx.managed(ZAdd)

	return
}

func (tx *Transaction) BatchZAdd(key [][]byte, score []float64, member [][]byte) (notExist bool, err error) {
	if len(key) != len(score) || len(key) != len(member) {
		return false, ErrLengthNotMatch
	}

	BatchZAdd := func() error {
		for i := 0; i < len(key); i++ {
			meta, err := tx.findMetadata(key[i], ZSet)
			if err != nil {
				return err
			}

			version := make([]byte, 8)
			binary.LittleEndian.PutUint64(version, uint64(meta.version))
			encZSetKey := bytes.Join([][]byte{key[i], version, member[i]}, nil)
			// 查看是否已经存在
			value, err := tx.get(encZSetKey)
			if err != nil && !errors.Is(err, ErrKeyNotFound) {
				return err
			}
			if errors.Is(err, ErrKeyNotFound) {
				notExist = true
			}
			if !notExist {
				if score[i] == utils.BytesToFloat64(value) {
					return nil
				}
			}

			if notExist {
				meta.size++
				err := tx.put(key[i], encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()))
				if err != nil {
					return err
				}
			}
			if !notExist {
				err = tx.delete(encZSetKey)
				if err != nil {
					return err
				}
			}
			err = tx.put(encZSetKey, utils.Float64ToBytes(score[i]), PERSISTENT, uint64(time.Now().UnixMilli()))
			if err != nil {
				return err
			}

			err = tx.put(encZSetKey, value, PERSISTENT, uint64(time.Now().UnixMilli()))
			encZSetKeyWithScore := bytes.Join([][]byte{key[i], version, value, member[i]}, nil)
			_ = tx.put(encZSetKeyWithScore, nil, PERSISTENT, uint64(time.Now().UnixMilli()))
		}

		return nil
	}

	err = tx.managed(BatchZAdd)

	return
}

func (tx *Transaction) ZScore(key []byte, member []byte) (score float64, err error) {
	score = -1
	ZScore := func() error {
		meta, err := tx.findMetadata(key, ZSet)
		if err != nil {
			return err
		}
		if meta.size == 0 {
			return nil
		}

		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(meta.version))
		encZSetKey := bytes.Join([][]byte{key, version, member}, nil)
		value, err := tx.get(encZSetKey)
		if err != nil {
			return err
		}
		score = utils.BytesToFloat64(value)
		return nil
	}

	err = tx.managed(ZScore)

	return
}

func (tx *Transaction) ZCard(key []byte) (uint32, error) {
	meta, err := tx.findMetadata(key, ZSet)
	if err != nil {
		return 0, err
	}
	return meta.size, nil
}

// 遍历
// 这段代码可以交给外部的handleFn实现
func hasPrefix(key, value []byte) (bool, error) {
	prefix := []byte("test")
	_ = value
	if !bytes.HasPrefix(prefix, key) {
		return false, nil
	}
	return true, nil
}

// Ascend 升序全局遍历
func (tx *Transaction) Ascend(handleFn func(key, value []byte) (bool, error)) ([]*KvPair, error) {
	return tx.Iterate(Ascend, nil, nil, handleFn)
}

// Descend 降序全局遍历
func (tx *Transaction) Descend(handleFn func(key, value []byte) (bool, error)) ([]*KvPair, error) {
	return tx.Iterate(Descend, nil, nil, handleFn)
}

// AscendRange 升序范围遍历
func (tx *Transaction) AscendRange(startKey, endKey []byte, handleFn func(key, value []byte) (bool, error)) ([]*KvPair, error) {
	return tx.Iterate(Ascend, startKey, endKey, handleFn)
}

// DescendRange 降序范围遍历
func (tx *Transaction) DescendRange(startKey, endKey []byte, handleFn func(key, value []byte) (bool, error)) ([]*KvPair, error) {
	return tx.Iterate(Descend, startKey, endKey, handleFn)
}

// AscendGreaterOrEqual 大于等于某个指定值升序遍历
func (tx *Transaction) AscendGreaterOrEqual(startKey []byte, handleFn func(key []byte, value []byte) (bool, error)) ([]*KvPair, error) {
	return tx.Iterate(Ascend, startKey, nil, handleFn)
}

// DescendLessOrEqual 小于等于某个指定值降序遍历
func (tx *Transaction) DescendLessOrEqual(startKey []byte, handleFn func(key, value []byte) (bool, error)) ([]*KvPair, error) {
	return tx.Iterate(Descend, startKey, nil, handleFn)
}

// 返回的是无序数据
// 存储开销低，执行开销低
func (tx *Transaction) IterateUnordered(startKey, endKey []byte, handleFn func(key, value []byte) (bool, error)) ([]*KvPair, error) {
	if tx.isClosed() {
		return nil, ErrTxClosed
	}

	// 处理事务中新加入的待写KvPair，保证事务内部的一致性
	KvPairInPendingWrites := make(map[string][]byte)
	for key, pendingKvPair := range tx.pendingWrites {
		// 筛选出大于等于目标key且未被删除的键
		if bytes.Compare([]byte(key), startKey) >= 0 && pendingKvPair.Type != KvPairDeleted {
			KvPairInPendingWrites[key] = pendingKvPair.Value
		}
	}

	var KvPairs []*KvPair
	internalHandleFn := func(currentKey []byte, pos *KvPairPos) (bool, error) {
		var currentValue []byte
		var err error
		// 如果PendingWrites中存在，则直接取PendingWrites中的值
		// 否则取数据库中的值
		if pendingWriteValue, exist := KvPairInPendingWrites[string(currentKey)]; exist {
			currentValue = pendingWriteValue
			delete(KvPairInPendingWrites, string(currentKey))
		} else {
			currentValue, err = tx.tm.db.GetValueByPosition(pos)
			if err != nil {
				return false, err
			}
		}

		ok, err := handleFn(currentKey, currentValue)
		if err != nil {
			return false, err
		}
		if ok {
			kvPair := &KvPair{
				Key:   currentKey,
				Value: currentValue,
			}
			KvPairs = append(KvPairs, kvPair)
		}
		return ok, nil
	}

	tx.tm.db.Index.Iterate(Ascend, startKey, endKey, internalHandleFn)

	for key, value := range KvPairInPendingWrites {
		ok, err := handleFn([]byte(key), value)
		if err != nil {
			return nil, err
		}
		if ok {
			kvPair := &KvPair{
				Key:   []byte(key),
				Value: value,
			}
			KvPairs = append(KvPairs, kvPair)
		}
	}

	return KvPairs, nil
}

// 返回有序数据
// 存储开销低，执行开销高
func (tx *Transaction) Iterate(iterateType IterateType, startKey, endKey []byte, handleFn func(key, value []byte) (bool, error)) ([]*KvPair, error) {
	if tx.isClosed() {
		return nil, ErrTxClosed
	}

	// 处理事务中新加入的待写KvPair，保证事务内部的一致性
	KvPairInPendingWrites := make(map[string][]byte)
	if iterateType == Ascend {
		for key, pendingKvPair := range tx.pendingWrites {
			// 筛选出大于等于目标key且未被删除的键
			if bytes.Compare([]byte(key), startKey) >= 0 && pendingKvPair.Type != KvPairDeleted {
				KvPairInPendingWrites[key] = pendingKvPair.Value
			}
		}
	} else if iterateType == Descend {
		for key, pendingKvPair := range tx.pendingWrites {
			// 筛选出大于等于目标key且未被删除的键
			if bytes.Compare([]byte(key), startKey) <= 0 && pendingKvPair.Type != KvPairDeleted {
				KvPairInPendingWrites[key] = pendingKvPair.Value
			}
		}
	}

	var KvPairs []*KvPair
	internalHandleFn := func(currentKey []byte, pos *KvPairPos) (bool, error) {
		var currentValue []byte
		var err error
		// 如果PendingWrites中存在，则直接取PendingWrites中的值
		// 否则取数据库中的值
		if pendingWriteValue, exist := KvPairInPendingWrites[string(currentKey)]; exist {
			currentValue = pendingWriteValue
			delete(KvPairInPendingWrites, string(currentKey))
		} else {
			currentValue, err = tx.tm.db.GetValueByPosition(pos)
			if err != nil {
				return false, err
			}
		}

		ok, err := handleFn(currentKey, currentValue)
		if err != nil {
			return false, err
		}
		if ok {
			kvPair := &KvPair{
				Key:   currentKey,
				Value: currentValue,
			}
			KvPairs = append(KvPairs, kvPair)
		}
		return ok, nil
	}

	tx.tm.db.Index.Iterate(Ascend, startKey, endKey, internalHandleFn)

	for key, value := range KvPairInPendingWrites {
		key := []byte(key)
		ok, err := handleFn(key, value)
		if err != nil {
			return nil, err
		}
		if ok {
			kvPair := &KvPair{
				Key:   key,
				Value: value,
			}
			KvPairs = append(KvPairs, kvPair)
		}
	}

	if iterateType == Ascend {
		sort.Slice(KvPairs, func(i, j int) bool {
			return bytes.Compare(KvPairs[i].Key, KvPairs[j].Key) < 0
		})
	} else if iterateType == Descend {
		sort.Slice(KvPairs, func(i, j int) bool {
			return bytes.Compare(KvPairs[i].Key, KvPairs[j].Key) > 0
		})
	}

	return KvPairs, nil
}
