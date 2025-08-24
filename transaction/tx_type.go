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
	"regexp"
	"sort"
	"time"
)

// string
func (tx *Transaction) put(bucketID uint64, key, value []byte, ttl uint32, timestamp uint64) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	kvPair := tx.tm.KvPairPool.Get().(*KvPair)
	kvPair.TxId, kvPair.Type = tx.id, KvPairPuted
	kvPair.BucketID, kvPair.Key, kvPair.Value = bucketID, key, value
	kvPair.KeySize, kvPair.ValueSize = uint32(len(key)), uint32(len(value))
	kvPair.Timestamp, kvPair.TTL = timestamp, ttl

	tx.pendingWrites[string(key)] = kvPair
	return nil
}

func (tx *Transaction) delete(bucketID uint64, key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// key不存在则直接返回
	// 查找索引
	if tx.tm.db.Index.Get(bucketID, key) == nil {
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

func (tx *Transaction) get(bucketID uint64, key []byte) ([]byte, error) {
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
	kvPairPos := tx.tm.db.Index.Get(bucketID, key)
	// 如果key不在内存索引中，说明 key 不存在
	if kvPairPos == nil {
		return nil, ErrKeyNotFound
	}

	// 从数据库中查找
	return tx.tm.db.GetValueByPosition(kvPairPos)
}

func (tx *Transaction) getKvPair(bucketID uint64, key []byte) (*KvPair, error) {
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
	kvPairPos := tx.tm.db.Index.Get(bucketID, key)
	// 如果key不在内存索引中，说明 key 不存在
	if kvPairPos == nil {
		return nil, ErrKeyNotFound
	}

	// 从数据库中查找
	return tx.tm.db.GetKvPairByPosition(kvPairPos)
}

func (tx *Transaction) Put(bucketName string, key, value []byte, ttl uint32, timestamp uint64, condition PutCondition) (oldValue []byte, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, err
	}

	if condition == PUT_NORMAL {
		PutNormal := func() error {
			return tx.put(bucketID, key, value, ttl, timestamp)
		}

		err = tx.managed(PutNormal)
	} else if condition == PUT_IF_NOT_EXISTS {
		PutIfNotExists := func() error {
			getValue, getErr := tx.get(bucketID, key)
			if getErr != nil && !errors.Is(getErr, ErrKeyNotFound) {
				return getErr
			}
			// 如果Key已经存在，则直接返回nil
			if getValue != nil {
				return nil
			}
			return tx.put(bucketID, key, value, ttl, timestamp)
		}

		err = tx.managed(PutIfNotExists)
	} else if condition == PUT_IF_EXISTS {
		PutIfNotExists := func() error {
			_, getErr := tx.get(bucketID, key)
			if getErr != nil {
				return getErr
			}
			return tx.put(bucketID, key, value, ttl, timestamp)
		}

		err = tx.managed(PutIfNotExists)
	} else if condition == PUT_AND_RETURN_OLD_VALUE {
		PutAndReturnOldValue := func() error {
			var getErr error
			oldValue, getErr = tx.get(bucketID, key)
			if getErr != nil && !errors.Is(getErr, ErrKeyNotFound) {
				return getErr
			}
			return tx.put(bucketID, key, value, ttl, timestamp)
		}

		err = tx.managed(PutAndReturnOldValue)
	} else if condition == APPEND_VALUE {
		AppendValue := func() error {
			var getErr error
			oldValue, getErr = tx.get(bucketID, key)
			if getErr != nil && !errors.Is(getErr, ErrKeyNotFound) {
				return getErr
			}
			if oldValue != nil {
				value = append(oldValue, value...)
			}

			return tx.put(bucketID, key, value, ttl, timestamp)
		}
		err = tx.managed(AppendValue)
	} else if condition == UPDATE_TTL {
		UpdateTTL := func() error {
			var getErr error
			oldValue, getErr = tx.get(bucketID, key)
			if getErr != nil {
				return getErr
			}
			return tx.put(bucketID, key, oldValue, ttl, timestamp)
		}
		err = tx.managed(UpdateTTL)
	}

	if err != nil {
		return nil, err
	}
	return
}

func (tx *Transaction) BatchPut(bucketName string, key, value [][]byte, ttl uint32, timestamp uint64, condition PutCondition) (oldValue [][]byte, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, err
	}

	if condition == PUT_NORMAL {
		BatchPut := func() (err error) {
			for i := 0; i < len(key); i++ {
				if err = tx.put(bucketID, key[i], value[i], ttl, timestamp); err != nil {
					return
				}
			}
			return
		}

		err = tx.managed(BatchPut)
	} else if condition == PUT_IF_NOT_EXISTS {
		BatchPutIfNotExists := func() (err error) {
			for i := 0; i < len(key); i++ {
				getValue, getErr := tx.get(bucketID, key[i])
				if getErr != nil && !errors.Is(getErr, ErrKeyNotFound) {
					return getErr
				}
				// 如果Key已经存在，则直接返回nil
				if getValue != nil {
					return
				}
				if err = tx.put(bucketID, key[i], value[i], ttl, timestamp); err != nil {
					return
				}
			}
			return
		}

		err = tx.managed(BatchPutIfNotExists)
	} else if condition == PUT_IF_EXISTS {
		BatchPutIfExists := func() (err error) {
			for i := 0; i < len(key); i++ {
				_, getErr := tx.get(bucketID, key[i])
				if getErr != nil {
					return getErr
				}
				if err = tx.put(bucketID, key[i], value[i], ttl, timestamp); err != nil {
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
				tempValue, getErr = tx.get(bucketID, key[i])
				if getErr != nil && !errors.Is(getErr, ErrKeyNotFound) {
					return getErr
				}
				if err = tx.put(bucketID, key[i], value[i], ttl, timestamp); err != nil {
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
				tempValue, getErr = tx.get(bucketID, key[i])
				if getErr != nil {
					return getErr
				}
				if err = tx.put(bucketID, key[i], tempValue, ttl, timestamp); err != nil {
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

func (tx *Transaction) Delete(bucketName string, key []byte) (err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return err
	}

	err = tx.managed(func() (err error) {
		err = tx.delete(bucketID, key)
		return err
	})
	return
}

func (tx *Transaction) BatchDelete(bucketName string, key [][]byte) (err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return err
	}

	err = tx.managed(func() (err error) {
		for i := 0; i < len(key); i++ {
			if err = tx.delete(bucketID, key[i]); err != nil {
				return
			}
		}
		return
	})
	return
}

func (tx *Transaction) Get(bucketName string, key []byte) (value []byte, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, err
	}

	err = tx.managed(func() (err error) {
		value, err = tx.get(bucketID, key)
		return
	})
	return
}

func (tx *Transaction) GetKvPair(bucketName string, key []byte) (kvPair *KvPair, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, err
	}

	err = tx.managed(func() (err error) {
		kvPair, err = tx.getKvPair(bucketID, key)
		return
	})
	return
}

func (tx *Transaction) BatchGet(bucketName string, key [][]byte) (value [][]byte, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, err
	}

	err = tx.managed(func() (err error) {
		var tempValue []byte
		for i := 0; i < len(key); i++ {
			if tempValue, err = tx.get(bucketID, key[i]); err != nil {
				return
			}
			value = append(value, tempValue)
		}
		return
	})
	return
}

// Set
//+--------------------------------------+---------------+
//|           Key                        |     Value     |
//+--------------------------------------+---------------+
//|  SetName|version|member|memberSize   |     NULL      |
//|       Nbyte+8byte+Nbyte+8byte)       |    (0 byte)   |
//+--------------------------------------+---------------+

// Set basedata
// SCARD 获取集合的成员数
func (tx *Transaction) SCARD(bucketName string, key []byte) (size uint32, err error) {
	SCARD := func() error {
		meta, err := tx.findMetadata(bucketName, key, Set)
		if err != nil {
			return err
		}

		// 这里应该返回一个ErrKeyNotFound
		if meta.size == 0 {
			return nil
		}

		size = meta.size

		return nil
	}

	err = tx.managed(SCARD)

	return
}

// Set put
func (tx *Transaction) SAdd(bucketName string, key, member []byte) (exist bool, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return false, err
	}

	SAdd := func() error {
		meta, err := tx.findMetadata(bucketName, key, Set)
		if err != nil {
			return err
		}

		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(meta.version))
		memberSize := make([]byte, 8)
		binary.LittleEndian.PutUint64(memberSize, uint64(len(member)))
		encSetKey := bytes.Join([][]byte{key, version, member, memberSize}, nil)

		exist = true
		_, err = tx.get(bucketID, encSetKey)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return err
		}
		if err != nil && errors.Is(err, ErrKeyNotFound) {
			exist = false
			meta.size++
			err = tx.put(bucketID, key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()))
			if err != nil {
				return err
			}
			err = tx.put(bucketID, encSetKey, nil, PERSISTENT, uint64(time.Now().UnixMilli()))
			if err != nil {
				return err
			}
		}
		return nil
	}

	err = tx.managed(SAdd)
	return
}

func (tx *Transaction) SBatchAdd(bucketName string, key []byte, members [][]byte) (exist bool, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return false, err
	}

	SAdd := func() error {
		meta, err := tx.findMetadata(bucketName, key, Set)
		if err != nil {
			return err
		}

		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(meta.version))
		memberSize := make([]byte, 8)
		for _, member := range members {
			clear(memberSize)
			binary.LittleEndian.PutUint64(memberSize, uint64(len(member)))
			encSetKey := bytes.Join([][]byte{key, version, member, memberSize}, nil)
			exist = true
			_, err = tx.get(bucketID, encSetKey)
			if err != nil && !errors.Is(err, ErrKeyNotFound) {
				return err
			}
			if err != nil && errors.Is(err, ErrKeyNotFound) {
				exist = false
				meta.size++
				err = tx.put(bucketID, key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()))
				if err != nil {
					return err
				}
				err = tx.put(bucketID, encSetKey, nil, PERSISTENT, uint64(time.Now().UnixMilli()))
				if err != nil {
					return err
				}
			}
		}

		return nil
	}

	err = tx.managed(SAdd)
	return
}

// Set get
func (tx *Transaction) SIsMember(bucketName string, key, member []byte) (exist bool, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return false, err
	}

	SIsMember := func() error {
		meta, err := tx.findMetadata(bucketName, key, Set)
		if err != nil {
			return err
		}
		if meta.size == 0 {
			return nil
		}
		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(meta.version))
		memberSize := make([]byte, 8)
		binary.LittleEndian.PutUint64(memberSize, uint64(len(member)))
		encSetKey := bytes.Join([][]byte{key, version, member, memberSize}, nil)

		exist = true
		_, err = tx.get(bucketID, encSetKey)
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

func (tx *Transaction) SMembers(bucketName string, key []byte) (members [][]byte, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, err
	}

	SMembers := func() error {
		meta, err := tx.findMetadata(bucketName, key, Set)
		if err != nil {
			return err
		}

		// 这里应该返回一个ErrKeyNotFound
		if meta.size == 0 {
			return nil
		}

		prefix := key
		hasPrefix := func(key, value []byte) (bool, error) {
			_ = value
			if !bytes.HasPrefix(prefix, key) {
				return false, nil
			}
			return true, nil
		}

		kvPairs, err := tx.AscendGreaterOrEqual(bucketID, key, hasPrefix)
		// kvPairs[0]是元数据，需要排除掉
		var memberSize int
		var member []byte
		for _, kvPair := range kvPairs[1:] {
			memberSize = int(binary.LittleEndian.Uint64(kvPair.Key[len(kvPair.Key)-8:]))
			member = kvPair.Key[len(kvPair.Key)-8-memberSize : len(kvPair.Key)-8]
			members = append(members, member)
		}

		return nil
	}

	err = tx.managed(SMembers)

	return
}

func (tx *Transaction) SRandMember(bucketName string, key []byte, count uint32) (members [][]byte, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, err
	}

	SRandMember := func() error {
		meta, err := tx.findMetadata(bucketName, key, Set)
		if err != nil {
			return err
		}

		if meta.size == 0 {
			return nil
		}

		prefix := key
		hasPrefix := func(key, value []byte) (bool, error) {
			_ = value
			if !bytes.HasPrefix(prefix, key) {
				return false, nil
			}
			return true, nil
		}

		var (
			memberSize int
			member     []byte
		)
		k, err := utils.RandomK(int(meta.size), int(count))
		if err != nil {
			return err
		}
		kvPairs, err := tx.AscendGreaterOrEqual(bucketID, key, hasPrefix)
		if err != nil {
			return err
		}
		for _, i := range k {
			memberSize = int(binary.LittleEndian.Uint64(kvPairs[i].Key[len(kvPairs[i].Key)-8:]))
			member = kvPairs[i].Key[len(kvPairs[i].Key)-8-memberSize : len(kvPairs[i].Key)-8]
			members = append(members, member)
		}
		return nil
	}

	err = tx.managed(SRandMember)
	return
}

func (tx *Transaction) SPop(bucketName string, key []byte, count uint32) (members [][]byte, err error) {
	var bucketID uint64
	bucketID, err = tx.getBucketID(bucketName)
	if err != nil {
		return nil, err
	}

	SPop := func() error {
		var meta *metadata
		meta, err = tx.findMetadata(bucketName, key, Set)
		if err != nil {
			return err
		}

		if meta.size == 0 {
			return nil
		}

		prefix := key
		hasPrefix := func(key, value []byte) (bool, error) {
			_ = value
			if !bytes.HasPrefix(prefix, key) {
				return false, nil
			}
			return true, nil
		}

		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(meta.version))
		var (
			memberSize int
			member     []byte
			k          []int
			kvPairs    []*KvPair
		)
		k, err = utils.RandomK(int(meta.size), int(count))
		if err != nil {
			return err
		}
		kvPairs, err = tx.AscendGreaterOrEqual(bucketID, key, hasPrefix)
		if err != nil {
			return err
		}
		for _, i := range k {
			memberSize = int(binary.LittleEndian.Uint64(kvPairs[i].Key[len(kvPairs[i].Key)-8:]))
			member = kvPairs[i].Key[len(kvPairs[i].Key)-8-memberSize : len(kvPairs[i].Key)-8]
			members = append(members, member)
			err = tx.delete(bucketID, kvPairs[i].Key)
			if err != nil {
				return err
			}
			meta.size--
		}

		if meta.size == 0 {
			err = tx.delete(bucketID, key)
			if err != nil {
				return err
			}
		} else {
			err = tx.put(bucketID, key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()))
			if err != nil {
				return err
			}
		}
		return nil
	}

	err = tx.managed(SPop)
	return
}

func (tx *Transaction) SRem(bucketName string, key, member []byte) (ok bool, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return false, err
	}

	SRem := func() error {
		meta, err := tx.findMetadata(bucketName, key, Set)
		if err != nil {
			return err
		}
		if meta.size == 0 {
			return nil
		}
		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(meta.version))
		memberSize := make([]byte, 8)
		binary.LittleEndian.PutUint64(memberSize, uint64(len(member)))
		encSetKey := bytes.Join([][]byte{key, version, member, memberSize}, nil)

		_, err = tx.get(bucketID, encSetKey)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {

			return err
		}
		if err != nil && errors.Is(err, ErrKeyNotFound) {
			return nil
		}

		err = tx.delete(bucketID, encSetKey)
		if err != nil {
			return err
		}

		meta.size--
		err = tx.put(bucketID, key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()))
		if err != nil {
			return err
		}

		ok = true

		return nil
	}

	err = tx.managed(SRem)

	return
}

func (tx *Transaction) SDiff(bucketName string, source []byte, others [][]byte) (diffSet [][]byte, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, err
	}

	SDiff := func() error {
		sourceMeta, err := tx.findMetadata(bucketName, source, Set)
		if err != nil {
			return err
		}
		// 这里应该返回一个ErrKeyNotFound
		if sourceMeta.size == 0 {
			return nil
		}
		otherMetas := make([]*metadata, len(others))
		for i, otherSet := range others {
			otherMeta, err := tx.findMetadata(bucketName, otherSet, Set)
			if err != nil {
				return err
			}
			// 这里应该返回一个ErrKeyNotFound
			if otherMeta.size == 0 {
				return nil
			}
			otherMetas[i] = otherMeta
		}

		prefix := source
		hasPrefix := func(key, value []byte) (bool, error) {
			_ = value
			if !bytes.HasPrefix(prefix, key) {
				return false, nil
			}
			return true, nil
		}

		kvPairs, err := tx.AscendGreaterOrEqual(bucketID, source, hasPrefix)
		var memberSizeInt int
		memberSizeByte := make([]byte, 8)
		var member []byte
		version := make([]byte, 8)
		var isDiff bool
		for _, kvPair := range kvPairs[1:] {
			isDiff = true
			memberSizeByte = kvPair.Key[len(kvPair.Key)-8:]
			memberSizeInt = int(binary.LittleEndian.Uint64(memberSizeByte))
			member = kvPair.Key[len(kvPair.Key)-8-memberSizeInt : len(kvPair.Key)-8]
			for i, otherSet := range others {
				clear(version)
				binary.LittleEndian.PutUint64(version, uint64(otherMetas[i].version))
				encSetKey := bytes.Join([][]byte{otherSet, version, member, memberSizeByte}, nil)
				_, err = tx.get(bucketID, encSetKey)
				if err != nil && !errors.Is(err, ErrKeyNotFound) {
					return err
				}
				if err == nil {
					isDiff = false
					break
				}
			}
			if isDiff {
				diffSet = append(diffSet, member)
			}
		}
		return nil
	}

	err = tx.managed(SDiff)
	return
}

func (tx *Transaction) SDiffStore(bucketName string, destination, source []byte, others [][]byte) (diffSet [][]byte, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, err
	}

	SDiffStore := func() error {
		destinationMeta, err := tx.findMetadata(bucketName, destination, Set)
		if err != nil {
			return err
		}
		destinationVersion := make([]byte, 8)
		binary.LittleEndian.PutUint64(destinationVersion, uint64(destinationMeta.version))

		sourceMeta, err := tx.findMetadata(bucketName, source, Set)
		if err != nil {
			return err
		}
		// 这里应该返回一个ErrKeyNotFound
		if sourceMeta.size == 0 {
			return nil
		}
		otherMetas := make([]*metadata, len(others))
		for i, otherSet := range others {
			otherMeta, err := tx.findMetadata(bucketName, otherSet, Set)
			if err != nil {
				return err
			}
			// 这里应该返回一个ErrKeyNotFound
			if otherMeta.size == 0 {
				return nil
			}
			otherMetas[i] = otherMeta
		}

		prefix := source
		hasPrefix := func(key, value []byte) (bool, error) {
			_ = value
			if !bytes.HasPrefix(prefix, key) {
				return false, nil
			}
			return true, nil
		}

		kvPairs, err := tx.AscendGreaterOrEqual(bucketID, source, hasPrefix)
		var memberSizeInt int
		memberSizeByte := make([]byte, 8)
		var member []byte
		version := make([]byte, 8)
		var isDiff bool
		for _, kvPair := range kvPairs[1:] {
			isDiff = true
			memberSizeByte = kvPair.Key[len(kvPair.Key)-8:]
			memberSizeInt = int(binary.LittleEndian.Uint64(memberSizeByte))
			member = kvPair.Key[len(kvPair.Key)-8-memberSizeInt : len(kvPair.Key)-8]
			for i, otherSet := range others {
				clear(version)
				binary.LittleEndian.PutUint64(version, uint64(otherMetas[i].version))
				encSetKey := bytes.Join([][]byte{otherSet, version, member, memberSizeByte}, nil)
				_, err = tx.get(bucketID, encSetKey)
				if err != nil && !errors.Is(err, ErrKeyNotFound) {
					return err
				}
				if err == nil {
					isDiff = false
					break
				}
			}
			if isDiff {
				diffSet = append(diffSet, member)
				encSetKey := bytes.Join([][]byte{destination, destinationVersion, member, memberSizeByte}, nil)
				err = tx.put(bucketID, encSetKey, nil, PERSISTENT, uint64(time.Now().UnixMilli()))
				if err != nil {
					return err
				}
				destinationMeta.size++
			}
		}
		err = tx.put(bucketID, encodeMetadata(destinationMeta), nil, PERSISTENT, uint64(time.Now().UnixMilli()))
		if err != nil {
			return err
		}
		return nil
	}

	err = tx.managed(SDiffStore)
	return
}

func (tx *Transaction) SInter(bucketName string, source []byte, others [][]byte) (interSet [][]byte, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, err
	}

	SInter := func() error {
		sourceMeta, err := tx.findMetadata(bucketName, source, Set)
		if err != nil {
			return err
		}
		// 这里应该返回一个ErrKeyNotFound
		if sourceMeta.size == 0 {
			return nil
		}
		otherMetas := make([]*metadata, len(others))
		for i, otherSet := range others {
			otherMeta, err := tx.findMetadata(bucketName, otherSet, Set)
			if err != nil {
				return err
			}
			// 这里应该返回一个ErrKeyNotFound
			if otherMeta.size == 0 {
				return nil
			}
			otherMetas[i] = otherMeta
		}

		prefix := source
		hasPrefix := func(key, value []byte) (bool, error) {
			_ = value
			if !bytes.HasPrefix(prefix, key) {
				return false, nil
			}
			return true, nil
		}

		kvPairs, err := tx.AscendGreaterOrEqual(bucketID, source, hasPrefix)
		var memberSizeInt int
		memberSizeByte := make([]byte, 8)
		var member []byte
		version := make([]byte, 8)
		var isInter bool
		for _, kvPair := range kvPairs[1:] {
			isInter = true
			memberSizeByte = kvPair.Key[len(kvPair.Key)-8:]
			memberSizeInt = int(binary.LittleEndian.Uint64(memberSizeByte))
			member = kvPair.Key[len(kvPair.Key)-8-memberSizeInt : len(kvPair.Key)-8]
			for i, otherSet := range others {
				clear(version)
				binary.LittleEndian.PutUint64(version, uint64(otherMetas[i].version))
				encSetKey := bytes.Join([][]byte{otherSet, version, member, memberSizeByte}, nil)
				_, err = tx.get(bucketID, encSetKey)
				if err != nil && !errors.Is(err, ErrKeyNotFound) {
					return err
				}
				if err != nil && errors.Is(err, ErrKeyNotFound) {
					isInter = false
					break
				}
			}
			if isInter {
				interSet = append(interSet, member)
			}
		}
		return nil
	}

	err = tx.managed(SInter)
	return
}

func (tx *Transaction) SInterStore(bucketName string, destination, source []byte, others [][]byte) (interSet [][]byte, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, err
	}

	SInterStore := func() error {
		destinationMeta, err := tx.findMetadata(bucketName, destination, Set)
		if err != nil {
			return err
		}
		destinationVersion := make([]byte, 8)
		binary.LittleEndian.PutUint64(destinationVersion, uint64(destinationMeta.version))

		sourceMeta, err := tx.findMetadata(bucketName, source, Set)
		if err != nil {
			return err
		}
		// 这里应该返回一个ErrKeyNotFound
		if sourceMeta.size == 0 {
			return nil
		}
		otherMetas := make([]*metadata, len(others))
		for i, otherSet := range others {
			otherMeta, err := tx.findMetadata(bucketName, otherSet, Set)
			if err != nil {
				return err
			}
			// 这里应该返回一个ErrKeyNotFound
			if otherMeta.size == 0 {
				return nil
			}
			otherMetas[i] = otherMeta
		}

		prefix := source
		hasPrefix := func(key, value []byte) (bool, error) {
			_ = value
			if !bytes.HasPrefix(prefix, key) {
				return false, nil
			}
			return true, nil
		}

		kvPairs, err := tx.AscendGreaterOrEqual(bucketID, source, hasPrefix)
		var memberSizeInt int
		memberSizeByte := make([]byte, 8)
		var member []byte
		version := make([]byte, 8)
		var isInter bool
		for _, kvPair := range kvPairs[1:] {
			isInter = true
			memberSizeByte = kvPair.Key[len(kvPair.Key)-8:]
			memberSizeInt = int(binary.LittleEndian.Uint64(memberSizeByte))
			member = kvPair.Key[len(kvPair.Key)-8-memberSizeInt : len(kvPair.Key)-8]
			for i, otherSet := range others {
				clear(version)
				binary.LittleEndian.PutUint64(version, uint64(otherMetas[i].version))
				encSetKey := bytes.Join([][]byte{otherSet, version, member, memberSizeByte}, nil)
				_, err = tx.get(bucketID, encSetKey)
				if err != nil && !errors.Is(err, ErrKeyNotFound) {
					return err
				}
				if err == nil && errors.Is(err, ErrKeyNotFound) {
					isInter = false
					break
				}
			}
			if isInter {
				interSet = append(interSet, member)
				encSetKey := bytes.Join([][]byte{destination, destinationVersion, member, memberSizeByte}, nil)
				err = tx.put(bucketID, encSetKey, nil, PERSISTENT, uint64(time.Now().UnixMilli()))
				if err != nil {
					return err
				}
				destinationMeta.size++
			}
		}
		err = tx.put(bucketID, encodeMetadata(destinationMeta), nil, PERSISTENT, uint64(time.Now().UnixMilli()))
		if err != nil {
			return err
		}
		return nil
	}

	err = tx.managed(SInterStore)
	return
}

func (tx *Transaction) SUnion(bucketName string, sets [][]byte) (unionSet [][]byte, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, err
	}

	SUnion := func() error {
		setMetas := make([]*metadata, len(sets))
		for i, set := range sets {
			setMeta, err := tx.findMetadata(bucketName, set, Set)
			if err != nil {
				return err
			}
			// 这里应该返回一个ErrKeyNotFound
			if setMeta.size == 0 {
				return nil
			}
			setMetas[i] = setMeta

			prefix := set
			hasPrefix := func(key, value []byte) (bool, error) {
				_ = value
				if !bytes.HasPrefix(prefix, key) {
					return false, nil
				}
				return true, nil
			}

			kvPairs, err := tx.AscendGreaterOrEqual(bucketID, set, hasPrefix)
			var memberSizeInt int
			memberSizeByte := make([]byte, 8)
			var member []byte
			for _, kvPair := range kvPairs[1:] {
				memberSizeByte = kvPair.Key[len(kvPair.Key)-8:]
				memberSizeInt = int(binary.LittleEndian.Uint64(memberSizeByte))
				member = kvPair.Key[len(kvPair.Key)-8-memberSizeInt : len(kvPair.Key)-8]
				unionSet = append(unionSet, member)
			}
		}
		return nil
	}

	err = tx.managed(SUnion)
	return
}

func (tx *Transaction) SUnionStore(bucketName string, destination []byte, sets [][]byte) (unionSet [][]byte, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, err
	}

	SUnion := func() error {
		destinationMeta, err := tx.findMetadata(bucketName, destination, Set)
		if err != nil {
			return err
		}
		destinationVersion := make([]byte, 8)
		binary.LittleEndian.PutUint64(destinationVersion, uint64(destinationMeta.version))

		setMetas := make([]*metadata, len(sets))
		temp := make(map[string]int)
		for i, set := range sets {
			setMeta, err := tx.findMetadata(bucketName, set, Set)
			if err != nil {
				return err
			}
			// 这里应该返回一个ErrKeyNotFound
			if setMeta.size == 0 {
				return nil
			}
			setMetas[i] = setMeta

			prefix := set
			hasPrefix := func(key, value []byte) (bool, error) {
				_ = value
				if !bytes.HasPrefix(prefix, key) {
					return false, nil
				}
				return true, nil
			}

			kvPairs, err := tx.AscendGreaterOrEqual(bucketID, set, hasPrefix)
			var memberSizeInt int
			memberSizeByte := make([]byte, 8)
			var member []byte
			for _, kvPair := range kvPairs[1:] {
				memberSizeByte = kvPair.Key[len(kvPair.Key)-8:]
				memberSizeInt = int(binary.LittleEndian.Uint64(memberSizeByte))
				member = kvPair.Key[len(kvPair.Key)-8-memberSizeInt : len(kvPair.Key)-8]
				if temp[string(member)] == 0 {
					unionSet = append(unionSet, member)
					encSetKey := bytes.Join([][]byte{destination, destinationVersion, member, memberSizeByte}, nil)
					err = tx.put(bucketID, encSetKey, nil, PERSISTENT, uint64(time.Now().UnixMilli()))
					if err != nil {
						return err
					}
					destinationMeta.size++
				}
				temp[string(member)]++
			}
		}

		err = tx.put(bucketID, destination, encodeMetadata(destinationMeta), PERSISTENT, uint64(time.Now().UnixMilli()))
		if err != nil {
			return err
		}
		return nil
	}

	err = tx.managed(SUnion)
	return
}

func (tx *Transaction) SMove(bucketName string, source, destination []byte) (err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return err
	}

	SMove := func() error {
		sourceMeta, err := tx.findMetadata(bucketName, source, Set)
		if err != nil {
			return err
		}
		// 这里应该返回一个ErrKeyNotFound
		if sourceMeta.size == 0 {
			return nil
		}

		destinationMeta, err := tx.findMetadata(bucketName, destination, Set)
		if err != nil {
			return err
		}

		prefix := source
		hasPrefix := func(key, value []byte) (bool, error) {
			_ = value
			if !bytes.HasPrefix(prefix, key) {
				return false, nil
			}
			return true, nil
		}

		kvPairs, err := tx.AscendGreaterOrEqual(bucketID, source, hasPrefix)
		// kvPairs[0]是元数据，需要排除掉
		var memberSizeInt int
		memberSizeByte := make([]byte, 8)
		var member []byte
		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(destinationMeta.version))

		for _, kvPair := range kvPairs[1:] {
			memberSizeByte = kvPair.Key[len(kvPair.Key)-8:]
			memberSizeInt = int(binary.LittleEndian.Uint64(memberSizeByte))
			member = kvPair.Key[len(kvPair.Key)-8-memberSizeInt : len(kvPair.Key)-8]
			encSetKey := bytes.Join([][]byte{destination, version, member, memberSizeByte}, nil)

			_, err = tx.get(bucketID, encSetKey)
			if err != nil && !errors.Is(err, ErrKeyNotFound) {
				return err
			}

			if err != nil && errors.Is(err, ErrKeyNotFound) {
				destinationMeta.size++
				err = tx.put(bucketID, encSetKey, nil, PERSISTENT, uint64(time.Now().UnixMilli()))
				if err != nil {
					return err
				}
			}

			err = tx.delete(bucketID, kvPair.Key)
			if err != nil {
				return err
			}
		}
		err = tx.delete(bucketID, source)
		if err != nil {
			return err
		}
		err = tx.put(bucketID, destination, encodeMetadata(destinationMeta), PERSISTENT, uint64(time.Now().UnixMilli()))
		if err != nil {
			return err
		}
		return nil
	}

	err = tx.managed(SMove)
	return
}

func (tx *Transaction) SScan(bucketName string, key []byte, cursor uint32, pattern string, count uint32) (members [][]byte, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, err
	}

	SScan := func() error {
		meta, err := tx.findMetadata(bucketName, key, Set)
		if err != nil {
			return err
		}

		// 这里应该返回一个ErrKeyNotFound
		if meta.size == 0 {
			return nil
		}

		prefix := key
		hasPrefix := func(key, value []byte) (bool, error) {
			_ = value
			if !bytes.HasPrefix(prefix, key) {
				return false, nil
			}
			return true, nil
		}

		kvPairs, err := tx.AscendGreaterOrEqual(bucketID, key, hasPrefix)
		kvPairs = kvPairs[1:]
		re := regexp.MustCompile(pattern)
		var memberSize int
		// kvPairs[0]是元数据，需要排除掉
		for _, kvPair := range kvPairs[cursor : cursor+count] {
			if re.MatchString(string(kvPair.Key)) {
				memberSize = int(binary.LittleEndian.Uint64(kvPair.Key[len(kvPair.Key)-8:]))
				member := kvPair.Key[len(kvPair.Key)-8-memberSize : len(kvPair.Key)-8]
				members = append(members, member)
			}
		}

		return nil
	}

	err = tx.managed(SScan)

	return
}

// Hash
//+--------------------------------------+---------------+
//|           Key                        |     Value     |
//+--------------------------------------+---------------+
//|  HashName|version|filed|filedSize    |     value     |
//|      (Nbyte+8byte+Nbyte+8byte)       |    (N byte)   |
//+--------------------------------------+---------------+

// hash basedata
func (tx *Transaction) HLen(bucketName string, key []byte) (size uint32, err error) {
	HLen := func() error {
		meta, err := tx.findMetadata(bucketName, key, Hash)
		if err != nil {
			return err
		}
		if meta.size == 0 {
			return nil
		}

		size = meta.size
		return nil
	}

	err = tx.managed(HLen)

	return
}

// hash set
func (tx *Transaction) HSet(bucketName string, key, field, value []byte) (notExist bool, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return false, err
	}

	HSet := func() error {
		// 从Value中查找元数据
		meta, err := tx.findMetadata(bucketName, key, Hash)
		if err != nil {
			return err
		}
		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(meta.version))
		// 构造 Hash的 SubKey
		fieldSize := make([]byte, 8)
		binary.LittleEndian.PutUint64(fieldSize, uint64(len(field)))
		// 构造 Hash的 SubKey
		encHashSubKey := bytes.Join([][]byte{key, version, field, fieldSize}, nil)

		if _, err = tx.get(bucketID, encHashSubKey); errors.Is(err, ErrKeyNotFound) {
			notExist = true
		}

		// 不存在则更新元数据
		if notExist {
			meta.size++
			err = tx.put(bucketID, key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()))
			if err != nil {
				return err
			}
		}
		err = tx.put(bucketID, encHashSubKey, value, PERSISTENT, uint64(time.Now().UnixMilli()))
		if err != nil {
			return err
		}

		return nil
	}

	err = tx.managed(HSet)

	return
}

func (tx *Transaction) HSetIfNotExist(bucketName string, key, field, value []byte) (notExist bool, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return false, err
	}

	HSet := func() error {
		// 从Value中查找元数据
		meta, err := tx.findMetadata(bucketName, key, Hash)
		if err != nil {
			return err
		}
		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(meta.version))
		fieldSize := make([]byte, 8)
		binary.LittleEndian.PutUint64(fieldSize, uint64(len(field)))
		// 构造 Hash的 SubKey
		encHashSubKey := bytes.Join([][]byte{key, version, field, fieldSize}, nil)

		if _, err = tx.get(bucketID, encHashSubKey); errors.Is(err, ErrKeyNotFound) {
			notExist = true
		}

		// 不存在则更新元数据
		if notExist {
			meta.size++
			err = tx.put(bucketID, key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()))
			if err != nil {
				return err
			}

			err = tx.put(bucketID, encHashSubKey, value, PERSISTENT, uint64(time.Now().UnixMilli()))
			if err != nil {
				return err
			}
		} else {
			return errors.New("key already exists")
		}

		return nil
	}

	err = tx.managed(HSet)

	return
}

func (tx *Transaction) HBatchSet(bucketName string, key []byte, fields, values [][]byte) (notExist bool, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return false, err
	}

	// 返回数组不匹配错
	if len(fields) != len(values) {
		return false, nil
	}
	HBatchSet := func() error {
		// 从Value中查找元数据
		meta, err := tx.findMetadata(bucketName, key, Hash)
		if err != nil {
			return err
		}
		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(meta.version))
		fieldSize := make([]byte, 8)
		// 构造 Hash的 SubKey
		for i := 0; i < len(fields); i++ {
			clear(fieldSize)
			binary.LittleEndian.PutUint64(fieldSize, uint64(len(fields[i])))
			// 构造 Hash的 SubKey
			encHashSubKey := bytes.Join([][]byte{key, version, fields[i], fieldSize}, nil)

			if _, err = tx.get(bucketID, encHashSubKey); errors.Is(err, ErrKeyNotFound) {
				notExist = true
			}

			// 不存在则更新元数据
			if notExist {
				meta.size++
				err = tx.put(bucketID, key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()))
				if err != nil {
					return err
				}
			}
			err = tx.put(bucketID, encHashSubKey, values[i], PERSISTENT, uint64(time.Now().UnixMilli()))
			if err != nil {
				return err
			}
		}

		return nil
	}

	err = tx.managed(HBatchSet)

	return
}

// hash get
func (tx *Transaction) HGet(bucketName string, key, field []byte) (value []byte, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, err
	}

	HGet := func() error {
		meta, err := tx.findMetadata(bucketName, key, Hash)
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

		value, err = tx.get(bucketID, encHashSubKey)
		if err != nil {
			return err
		}

		return nil
	}

	err = tx.managed(HGet)

	return
}

func (tx *Transaction) HBatchGet(bucketName string, key []byte, fields [][]byte) (values [][]byte, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, err
	}

	HBatchGet := func() error {
		meta, err := tx.findMetadata(bucketName, key, Hash)
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
		for _, field := range fields {
			encHashSubKey := bytes.Join([][]byte{key, version, field}, nil)

			value, err := tx.get(bucketID, encHashSubKey)
			if err != nil {
				return err
			}
			values = append(values, value)
		}

		return nil
	}

	err = tx.managed(HBatchGet)

	return
}

func (tx *Transaction) HGetAll(bucketName string, key []byte) (fields, values [][]byte, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, nil, err
	}

	HGetAll := func() error {
		meta, err := tx.findMetadata(bucketName, key, Hash)
		if err != nil {
			return err
		}

		// 这里应该返回一个ErrKeyNotFound
		if meta.size == 0 {
			return nil
		}

		prefix := key
		hasPrefix := func(key, value []byte) (bool, error) {
			_ = value
			if !bytes.HasPrefix(prefix, key) {
				return false, nil
			}
			return true, nil
		}

		kvPairs, err := tx.AscendGreaterOrEqual(bucketID, key, hasPrefix)
		var filedSize int
		// kvPairs[0]是元数据，需要排除掉
		for _, kvPair := range kvPairs[1:] {
			filedSize = int(binary.LittleEndian.Uint64(kvPair.Key[len(kvPair.Key)-8:]))
			filed := kvPair.Key[len(kvPair.Key)-8-filedSize : len(kvPair.Key)-8]
			fields = append(fields, filed)
			values = append(values, kvPair.Value)
		}

		return nil
	}

	err = tx.managed(HGetAll)

	return
}

func (tx *Transaction) HKeys(bucketName string, key []byte) (fields [][]byte, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, err
	}

	HKeys := func() error {
		meta, err := tx.findMetadata(bucketName, key, Hash)
		if err != nil {
			return err
		}

		// 这里应该返回一个ErrKeyNotFound
		if meta.size == 0 {
			return nil
		}

		prefix := key
		hasPrefix := func(key, value []byte) (bool, error) {
			_ = value
			if !bytes.HasPrefix(prefix, key) {
				return false, nil
			}
			return true, nil
		}

		kvPairs, err := tx.AscendGreaterOrEqual(bucketID, key, hasPrefix)
		var filedSize int
		// kvPairs[0]是元数据，需要排除掉
		for _, kvPair := range kvPairs[1:] {
			filedSize = int(binary.LittleEndian.Uint64(kvPair.Key[len(kvPair.Key)-8:]))
			filed := kvPair.Key[len(kvPair.Key)-8-filedSize : len(kvPair.Key)-8]
			fields = append(fields, filed)
		}

		return nil
	}

	err = tx.managed(HKeys)

	return
}

func (tx *Transaction) HValues(bucketName string, key []byte) (values [][]byte, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, err
	}

	HValues := func() error {
		meta, err := tx.findMetadata(bucketName, key, Hash)
		if err != nil {
			return err
		}

		// 这里应该返回一个ErrKeyNotFound
		if meta.size == 0 {
			return nil
		}

		prefix := key
		hasPrefix := func(key, value []byte) (bool, error) {
			_ = value
			if !bytes.HasPrefix(prefix, key) {
				return false, nil
			}
			return true, nil
		}

		kvPairs, err := tx.AscendGreaterOrEqual(bucketID, key, hasPrefix)
		// kvPairs[0]是元数据，需要排除掉
		for _, kvPair := range kvPairs[1:] {
			values = append(values, kvPair.Value)
		}

		return nil
	}

	err = tx.managed(HValues)

	return
}

func (tx *Transaction) HScan(bucketName string, key []byte, cursor uint32, pattern string, count uint32) (fields, values [][]byte, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, nil, err
	}

	HScan := func() error {
		meta, err := tx.findMetadata(bucketName, key, Hash)
		if err != nil {
			return err
		}

		// 这里应该返回一个ErrKeyNotFound
		if meta.size == 0 {
			return nil
		}

		prefix := key
		hasPrefix := func(key, value []byte) (bool, error) {
			_ = value
			if !bytes.HasPrefix(prefix, key) {
				return false, nil
			}
			return true, nil
		}

		kvPairs, err := tx.AscendGreaterOrEqual(bucketID, key, hasPrefix)
		kvPairs = kvPairs[1:]
		re := regexp.MustCompile(pattern)
		var filedSize int
		// kvPairs[0]是元数据，需要排除掉
		for _, kvPair := range kvPairs[cursor : cursor+count] {
			if re.MatchString(string(kvPair.Key)) {
				filedSize = int(binary.LittleEndian.Uint64(kvPair.Key[len(kvPair.Key)-8:]))
				filed := kvPair.Key[len(kvPair.Key)-8-filedSize : len(kvPair.Key)-8]
				fields = append(fields, filed)
				values = append(values, kvPair.Value)
			}
		}

		return nil
	}

	err = tx.managed(HScan)

	return
}

func (tx *Transaction) HExists(bucketName string, key, field []byte) (exists bool, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return false, err
	}

	HExists := func() error {
		meta, err := tx.findMetadata(bucketName, key, Hash)
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

		_, err = tx.get(bucketID, encHashSubKey)
		if err != nil {
			return err
		}

		exists = true

		return nil
	}

	err = tx.managed(HExists)

	return
}

// hash delete
func (tx *Transaction) HDel(bucketName string, key, field []byte) (exist bool, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return false, err
	}

	HDel := func() error {
		meta, err := tx.findMetadata(bucketName, key, Hash)
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
		_, err = tx.get(bucketID, encHashSubKey)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return err
		}
		if err != nil && errors.Is(err, ErrKeyNotFound) {
			exist = false
			return nil
		}

		meta.size--
		err = tx.put(bucketID, key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()))
		if err != nil {
			return err
		}
		err = tx.delete(bucketID, encHashSubKey)
		if err != nil {
			return err
		}

		return nil
	}

	err = tx.managed(HDel)

	return
}

//todo: HIncrby HIncrbyFloat

// List
// +--------------------------+---------------+
// |           Key            |     Value     |
// +--------------------------+---------------+
// | ListName|version|index   |     value     |
// |   (Nbyte+8byte+8byte)    |    (N byte)   |
// +--------------------------+---------------+
func (tx *Transaction) push(bucketName string, key, value []byte, isLeft bool) (uint32, error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return 0, err
	}

	// 查找元数据
	meta, err := tx.findMetadata(bucketName, key, List)
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
	err = tx.put(bucketID, key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()))
	if err != nil {
		return 0, err
	}
	err = tx.put(bucketID, encListKey, value, PERSISTENT, uint64(time.Now().UnixMilli()))
	if err != nil {
		return 0, err
	}

	return meta.size, nil
}

func (tx *Transaction) pop(bucketName string, key []byte, isLeft bool) ([]byte, error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, err
	}

	// 查找元数据
	meta, err := tx.findMetadata(bucketName, key, List)
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

	value, err := tx.get(bucketID, encListKey)
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

	err = tx.put(bucketID, key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()))
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (tx *Transaction) LPush(bucketName string, key, element []byte) (size uint32, err error) {
	LPush := func() error {
		size, err = tx.push(bucketName, key, element, true)
		if err != nil {
			return err
		}
		return nil
	}
	err = tx.managed(LPush)
	return
}

func (tx *Transaction) RPush(bucketName string, key, element []byte) (size uint32, err error) {
	RPush := func() error {
		size, err = tx.push(bucketName, key, element, false)
		if err != nil {
			return err
		}
		return nil
	}
	err = tx.managed(RPush)
	return
}

func (tx *Transaction) LPop(bucketName string, key []byte) (value []byte, err error) {
	LPop := func() error {
		value, err = tx.pop(bucketName, key, true)
		if err != nil {
			return err
		}
		return nil
	}
	err = tx.managed(LPop)
	return
}

func (tx *Transaction) RPop(bucketName string, key []byte) (value []byte, err error) {
	RPop := func() error {
		value, err = tx.pop(bucketName, key, false)
		if err != nil {
			return err
		}
		return nil
	}
	err = tx.managed(RPop)
	return
}

// ZSet
// ZSet basedata
// ZCard 获取有序集合的成员数
func (tx *Transaction) ZCard(bucketName string, key []byte) (size uint32, err error) {
	ZCard := func() error {
		meta, err := tx.findMetadata(bucketName, key, ZSet)
		if err != nil {
			return err
		}
		size = meta.size
		return nil
	}

	err = tx.managed(ZCard)

	return
}

// ZCOUNT 计算在有序集合中指定区间分数的成员数
func (tx *Transaction) ZCOUNT(bucketName string, key []byte, min_score, max_score float64) (size uint32, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return 0, err
	}

	ZCOUNT := func() error {
		_, err = tx.findMetadata(bucketName, key, ZSet)
		if err != nil {
			return err
		}

		prefix := key
		hasPrefix := func(key, value []byte) (bool, error) {
			_ = value
			if !bytes.HasPrefix(prefix, key) {
				return false, nil
			}
			return true, nil
		}

		kvPairs, err := tx.AscendGreaterOrEqual(bucketID, key, hasPrefix)
		if err != nil {
			return err
		}
		var score float64
		for _, kvPair := range kvPairs[1:] {
			score = utils.BytesToFloat64(kvPair.Value)
			if score >= min_score && score <= max_score {
				size++
			}
		}

		return nil
	}

	err = tx.managed(ZCOUNT)

	return size, nil
}

// ZSet set
// ZAdd 向有序集合添加一个成员，或者更新已存在成员的分数
func (tx *Transaction) ZAdd(bucketName string, key []byte, score float64, member []byte) (notExist bool, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return false, err
	}

	ZAdd := func() error {
		meta, err := tx.findMetadata(bucketName, key, ZSet)
		if err != nil {
			return err
		}

		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(meta.version))
		memberSize := make([]byte, 8)
		binary.LittleEndian.PutUint64(memberSize, uint64(len(member)))
		encZSetKey := bytes.Join([][]byte{key, version, member, memberSize}, nil)
		// 查看是否已经存在
		value, err := tx.get(bucketID, encZSetKey)
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
			err := tx.put(bucketID, key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()))
			if err != nil {
				return err
			}
		}
		if !notExist {
			err = tx.delete(bucketID, encZSetKey)
			if err != nil {
				return err
			}
		}
		err = tx.put(bucketID, encZSetKey, utils.Float64ToBytes(score), PERSISTENT, uint64(time.Now().UnixMilli()))
		if err != nil {
			return err
		}

		err = tx.put(bucketID, encZSetKey, value, PERSISTENT, uint64(time.Now().UnixMilli()))
		encZSetKeyWithScore := bytes.Join([][]byte{key, version, value, member}, nil)
		_ = tx.put(bucketID, encZSetKeyWithScore, nil, PERSISTENT, uint64(time.Now().UnixMilli()))

		return nil
	}

	err = tx.managed(ZAdd)

	return
}

// ZBatchAdd 向有序集合添加多个成员，或者更新已存在成员的分数
func (tx *Transaction) ZBatchAdd(bucketName string, key [][]byte, score []float64, member [][]byte) (notExist bool, err error) {
	if len(key) != len(score) || len(key) != len(member) {
		return false, ErrLengthNotMatch
	}

	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return false, err
	}

	BatchZAdd := func() error {
		for i := 0; i < len(key); i++ {
			meta, err := tx.findMetadata(bucketName, key[i], ZSet)
			if err != nil {
				return err
			}

			version := make([]byte, 8)
			binary.LittleEndian.PutUint64(version, uint64(meta.version))
			memberSize := make([]byte, 8)
			binary.LittleEndian.PutUint64(memberSize, uint64(len(member[i])))
			encZSetKey := bytes.Join([][]byte{key[i], version, member[i], memberSize}, nil)
			// 查看是否已经存在
			value, err := tx.get(bucketID, encZSetKey)
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
				err := tx.put(bucketID, key[i], encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()))
				if err != nil {
					return err
				}
			}
			if !notExist {
				err = tx.delete(bucketID, encZSetKey)
				if err != nil {
					return err
				}
			}
			err = tx.put(bucketID, encZSetKey, utils.Float64ToBytes(score[i]), PERSISTENT, uint64(time.Now().UnixMilli()))
			if err != nil {
				return err
			}

			err = tx.put(bucketID, encZSetKey, value, PERSISTENT, uint64(time.Now().UnixMilli()))
			encZSetKeyWithScore := bytes.Join([][]byte{key[i], version, value, member[i]}, nil)
			_ = tx.put(bucketID, encZSetKeyWithScore, nil, PERSISTENT, uint64(time.Now().UnixMilli()))
		}

		return nil
	}

	err = tx.managed(BatchZAdd)

	return
}

func (tx *Transaction) ZScore(bucketName string, key []byte, member []byte) (score float64, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return 0, err
	}

	score = -1
	ZScore := func() error {
		meta, err := tx.findMetadata(bucketName, key, ZSet)
		if err != nil {
			return err
		}
		if meta.size == 0 {
			return nil
		}

		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(meta.version))
		memberSize := make([]byte, 8)
		binary.LittleEndian.PutUint64(memberSize, uint64(len(member)))
		encZSetKey := bytes.Join([][]byte{key, version, member, memberSize}, nil)
		value, err := tx.get(bucketID, encZSetKey)
		if err != nil {
			return err
		}
		score = utils.BytesToFloat64(value)
		return nil
	}

	err = tx.managed(ZScore)

	return
}

// ZRange 获取有序集合的成员数
func (tx *Transaction) ZRange(bucketName string, key []byte, start, stop uint32, isAscend bool) (members [][]byte, scores []float64, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, nil, err
	}

	ZRange := func() error {
		meta, err := tx.findMetadata(bucketName, key, ZSet)
		if err != nil {
			return err
		}

		prefix := key
		hasPrefix := func(key, value []byte) (bool, error) {
			_ = value
			if !bytes.HasPrefix(prefix, key) {
				return false, nil
			}
			return true, nil
		}

		var kvPairs []*KvPair
		if isAscend {
			kvPairs, err = tx.AscendGreaterOrEqual(bucketID, key, hasPrefix)
			if err != nil {
				return err
			}
			kvPairs = kvPairs[1:]
			sort.SliceStable(kvPairs, func(i, j int) bool {
				return utils.BytesToFloat64(kvPairs[i].Value) < utils.BytesToFloat64(kvPairs[j].Value)
			})
		} else {
			kvPairs, err = tx.DescendLessOrEqual(bucketID, key, hasPrefix)
			if err != nil {
				return err
			}
			kvPairs = kvPairs[:len(kvPairs)-1]
			sort.SliceStable(kvPairs, func(i, j int) bool {
				return utils.BytesToFloat64(kvPairs[i].Value) > utils.BytesToFloat64(kvPairs[j].Value)
			})
		}

		kvPairs = kvPairs[start:stop]
		var score float64
		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(meta.version))
		var memberSize int
		var member []byte
		for _, kvPair := range kvPairs {
			memberSize = int(binary.LittleEndian.Uint64(kvPair.Key[len(kvPair.Key)-8:]))
			member = kvPair.Key[len(kvPair.Key)-8-memberSize : len(kvPair.Key)-8]
			members = append(members, member)
			score = utils.BytesToFloat64(kvPair.Value)
			scores = append(scores, score)
		}

		return nil
	}

	err = tx.managed(ZRange)

	return
}

// ZRange 获取有序集合的成员数
func (tx *Transaction) ZRangeMember(bucketName string, key []byte, start, stop uint32, isAscend bool) (members [][]byte, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, err
	}

	ZRangeMember := func() error {
		meta, err := tx.findMetadata(bucketName, key, ZSet)
		if err != nil {
			return err
		}

		prefix := key
		hasPrefix := func(key, value []byte) (bool, error) {
			_ = value
			if !bytes.HasPrefix(prefix, key) {
				return false, nil
			}
			return true, nil
		}

		var kvPairs []*KvPair
		if isAscend {
			kvPairs, err = tx.AscendGreaterOrEqual(bucketID, key, hasPrefix)
			if err != nil {
				return err
			}
			kvPairs = kvPairs[1:]
			sort.SliceStable(kvPairs, func(i, j int) bool {
				return utils.BytesToFloat64(kvPairs[i].Value) < utils.BytesToFloat64(kvPairs[j].Value)
			})
		} else {
			kvPairs, err = tx.DescendLessOrEqual(bucketID, key, hasPrefix)
			if err != nil {
				return err
			}
			kvPairs = kvPairs[:len(kvPairs)-1]
			sort.SliceStable(kvPairs, func(i, j int) bool {
				return utils.BytesToFloat64(kvPairs[i].Value) > utils.BytesToFloat64(kvPairs[j].Value)
			})
		}

		kvPairs = kvPairs[start:stop]
		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(meta.version))
		var memberSize int
		var member []byte
		for _, kvPair := range kvPairs {
			memberSize = int(binary.LittleEndian.Uint64(kvPair.Key[len(kvPair.Key)-8:]))
			member = kvPair.Key[len(kvPair.Key)-8-memberSize : len(kvPair.Key)-8]
			members = append(members, member)
		}
		return nil
	}

	err = tx.managed(ZRangeMember)

	return
}

func (tx *Transaction) ZRangeByScore(bucketName string, key []byte, min_score, max_score float64, isAscend bool) (members [][]byte, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, err
	}

	ZRangeByScore := func() error {
		meta, err := tx.findMetadata(bucketName, key, ZSet)
		if err != nil {
			return err
		}

		prefix := key
		hasPrefix := func(key, value []byte) (bool, error) {
			_ = value
			if !bytes.HasPrefix(prefix, key) {
				return false, nil
			}
			return true, nil
		}

		var kvPairs []*KvPair
		if isAscend {
			kvPairs, err = tx.AscendGreaterOrEqual(bucketID, key, hasPrefix)
			if err != nil {
				return err
			}
			kvPairs = kvPairs[1:]
			sort.SliceStable(kvPairs, func(i, j int) bool {
				return utils.BytesToFloat64(kvPairs[i].Value) < utils.BytesToFloat64(kvPairs[j].Value)
			})
		} else {
			kvPairs, err = tx.DescendLessOrEqual(bucketID, key, hasPrefix)
			if err != nil {
				return err
			}
			kvPairs = kvPairs[:len(kvPairs)-1]
			sort.SliceStable(kvPairs, func(i, j int) bool {
				return utils.BytesToFloat64(kvPairs[i].Value) > utils.BytesToFloat64(kvPairs[j].Value)
			})
		}

		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(meta.version))
		var memberSize int
		var member []byte
		// 为什么不先过滤在排序？
		// 因为无法确定min和max所包含的元素数量，如果包含了全部的元素，过滤操作相当于白做
		// 而排序后剔除，本身数组是有序的，剔除元素不会有更多额外开销
		// 先过滤再排序，最坏情况下要多次一次for循环
		// 先排序再过滤，最坏情况下要对全部元素进行排序
		for _, kvPair := range kvPairs {
			memberSize = int(binary.LittleEndian.Uint64(kvPair.Key[len(kvPair.Key)-8:]))
			member = kvPair.Key[len(kvPair.Key)-8-memberSize : len(kvPair.Key)-8]
			if utils.BytesToFloat64(kvPair.Value) >= min_score && utils.BytesToFloat64(kvPair.Value) <= max_score {
				members = append(members, member)
			}
		}

		return nil
	}

	err = tx.managed(ZRangeByScore)

	return
}

// ZRank 返回有序集合中指定成员的索引
func (tx *Transaction) ZRank(bucketName string, key, compareMember []byte, isAscend bool) (rn int, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return 0, err
	}

	rn = -1
	ZRank := func() error {
		meta, err := tx.findMetadata(bucketName, key, ZSet)
		if err != nil {
			return err
		}

		prefix := key
		hasPrefix := func(key, value []byte) (bool, error) {
			_ = value
			if !bytes.HasPrefix(prefix, key) {
				return false, nil
			}
			return true, nil
		}

		var kvPairs []*KvPair
		if isAscend {
			kvPairs, err = tx.AscendGreaterOrEqual(bucketID, key, hasPrefix)
			if err != nil {
				return err
			}
			kvPairs = kvPairs[1:]
			sort.SliceStable(kvPairs, func(i, j int) bool {
				return utils.BytesToFloat64(kvPairs[i].Value) < utils.BytesToFloat64(kvPairs[j].Value)
			})
		} else {
			kvPairs, err = tx.DescendLessOrEqual(bucketID, key, hasPrefix)
			if err != nil {
				return err
			}
			kvPairs = kvPairs[:len(kvPairs)-1]
			sort.SliceStable(kvPairs, func(i, j int) bool {
				return utils.BytesToFloat64(kvPairs[i].Value) > utils.BytesToFloat64(kvPairs[j].Value)
			})
		}

		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(meta.version))
		var memberSize int
		var member []byte
		for i, kvPair := range kvPairs {
			memberSize = int(binary.LittleEndian.Uint64(kvPair.Key[len(kvPair.Key)-8:]))
			member = kvPair.Key[len(kvPair.Key)-8-memberSize : len(kvPair.Key)-8]
			if bytes.Compare(compareMember, member) == 0 {
				rn = i
				break
			}
		}

		return nil
	}

	err = tx.managed(ZRank)

	return
}

func (tx *Transaction) ZRem(bucketName string, key, member []byte) (ok bool, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return false, err
	}

	ZRem := func() error {
		meta, err := tx.findMetadata(bucketName, key, ZSet)
		if err != nil {
			return err
		}
		if meta.size == 0 {
			return nil
		}
		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(meta.version))
		memberSize := make([]byte, 8)
		binary.LittleEndian.PutUint64(memberSize, uint64(len(member)))
		encSetKey := bytes.Join([][]byte{key, version, member, memberSize}, nil)

		_, err = tx.get(bucketID, encSetKey)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return err
		}
		if err != nil && errors.Is(err, ErrKeyNotFound) {
			return nil
		}

		err = tx.delete(bucketID, encSetKey)
		if err != nil {
			return err
		}
		meta.size--
		err = tx.put(bucketID, key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()))
		if err != nil {
			return err
		}

		ok = true

		return nil
	}

	err = tx.managed(ZRem)

	return
}

func (tx *Transaction) ZBatchRem(bucketName string, key []byte, members [][]byte) (ok bool, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return false, err
	}

	ZBatchRem := func() error {
		meta, err := tx.findMetadata(bucketName, key, ZSet)
		if err != nil {
			return err
		}
		if meta.size == 0 {
			return nil
		}
		version := make([]byte, 8)
		binary.LittleEndian.PutUint64(version, uint64(meta.version))
		memberSize := make([]byte, 8)
		for _, member := range members {
			clear(memberSize)
			binary.LittleEndian.PutUint64(memberSize, uint64(len(member)))
			encSetKey := bytes.Join([][]byte{key, version, member, memberSize}, nil)
			_, err = tx.get(bucketID, encSetKey)
			if err != nil && !errors.Is(err, ErrKeyNotFound) {
				return err
			}
			if err != nil && errors.Is(err, ErrKeyNotFound) {
				return nil
			}

			err = tx.delete(bucketID, encSetKey)
			if err != nil {
				return err
			}

			meta.size--
		}

		err = tx.put(bucketID, key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()))
		if err != nil {
			return err
		}

		ok = true

		return nil
	}

	err = tx.managed(ZBatchRem)

	return
}

// ZRange 获取有序集合的成员数
func (tx *Transaction) ZRemRangeByRank(bucketName string, key []byte, start, stop int) (count int, err error) {
	count = -1
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return count, err
	}

	ZRemRangeByRank := func() error {
		meta, err := tx.findMetadata(bucketName, key, ZSet)
		if err != nil {
			return err
		}

		if meta.size == 0 {
			return nil
		}

		prefix := key
		hasPrefix := func(key, value []byte) (bool, error) {
			_ = value
			if !bytes.HasPrefix(prefix, key) {
				return false, nil
			}
			return true, nil
		}

		var kvPairs []*KvPair
		kvPairs, err = tx.AscendGreaterOrEqual(bucketID, key, hasPrefix)
		kvPairs = kvPairs[1:]

		sort.SliceStable(kvPairs, func(i, j int) bool {
			return utils.BytesToFloat64(kvPairs[i].Value) < utils.BytesToFloat64(kvPairs[j].Value)
		})

		for i, kvPair := range kvPairs {
			if i >= start && i <= stop {
				err = tx.delete(bucketID, kvPair.Key)
				if err != nil {
					return err
				}
				meta.size--
				count++
			}
		}

		err = tx.put(bucketID, key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()))
		if err != nil {
			return err
		}

		return nil
	}

	err = tx.managed(ZRemRangeByRank)

	return
}

// ZRange 获取有序集合的成员数
func (tx *Transaction) ZRemRangeByScore(bucketName string, key []byte, min_value, max_value float64) (count int, err error) {
	count = -1
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return count, err
	}

	ZRemRangeByScore := func() error {
		meta, err := tx.findMetadata(bucketName, key, ZSet)
		if err != nil {
			return err
		}

		if meta.size == 0 {
			return nil
		}

		prefix := key
		hasPrefix := func(key, value []byte) (bool, error) {
			_ = value
			if !bytes.HasPrefix(prefix, key) {
				return false, nil
			}
			return true, nil
		}

		kvPairs, err := tx.AscendGreaterOrEqual(bucketID, key, hasPrefix)
		kvPairs = kvPairs[1:]
		for _, kvPair := range kvPairs {
			if utils.BytesToFloat64(kvPair.Value) >= min_value && utils.BytesToFloat64(kvPair.Value) <= max_value {
				err = tx.delete(bucketID, kvPair.Key)
				if err != nil {
					return err
				}
				meta.size--
				count++
			}
		}

		err = tx.put(bucketID, key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()))
		if err != nil {
			return err
		}

		return nil
	}

	err = tx.managed(ZRemRangeByScore)

	return
}

func (tx *Transaction) ZScan(bucketName string, key []byte, cursor uint32, pattern string, count uint32) (members, values [][]byte, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, nil, err
	}

	ZScan := func() error {
		meta, err := tx.findMetadata(bucketName, key, ZSet)
		if err != nil {
			return err
		}

		// 这里应该返回一个ErrKeyNotFound
		if meta.size == 0 {
			return nil
		}

		prefix := key
		hasPrefix := func(key, value []byte) (bool, error) {
			_ = value
			if !bytes.HasPrefix(prefix, key) {
				return false, nil
			}
			return true, nil
		}

		kvPairs, err := tx.AscendGreaterOrEqual(bucketID, key, hasPrefix)
		kvPairs = kvPairs[1:]
		re := regexp.MustCompile(pattern)
		var memberSize int
		// kvPairs[0]是元数据，需要排除掉
		for _, kvPair := range kvPairs[cursor : cursor+count] {
			if re.MatchString(string(kvPair.Key)) {
				memberSize = int(binary.LittleEndian.Uint64(kvPair.Key[len(kvPair.Key)-8:]))
				member := kvPair.Key[len(kvPair.Key)-8-memberSize : len(kvPair.Key)-8]
				members = append(members, member)
				values = append(values, kvPair.Value)
			}
		}

		return nil
	}

	err = tx.managed(ZScan)

	return
}

func (tx *Transaction) ZUnionStore(bucketName string, destination []byte, sets [][]byte) (unionSet [][]byte, err error) {
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return nil, err
	}

	ZUnionStore := func() error {
		destinationMeta, err := tx.findMetadata(bucketName, destination, Set)
		if err != nil {
			return err
		}
		destinationVersion := make([]byte, 8)
		binary.LittleEndian.PutUint64(destinationVersion, uint64(destinationMeta.version))

		setMetas := make([]*metadata, len(sets))
		temp := make(map[string]float64)
		for i, set := range sets {
			setMeta, err := tx.findMetadata(bucketName, set, Set)
			if err != nil {
				return err
			}
			// 这里应该返回一个ErrKeyNotFound
			if setMeta.size == 0 {
				return nil
			}
			setMetas[i] = setMeta

			prefix := set
			hasPrefix := func(key, value []byte) (bool, error) {
				_ = value
				if !bytes.HasPrefix(prefix, key) {
					return false, nil
				}
				return true, nil
			}

			kvPairs, err := tx.AscendGreaterOrEqual(bucketID, set, hasPrefix)
			var memberSizeInt int
			memberSizeByte := make([]byte, 8)
			var member []byte
			for _, kvPair := range kvPairs[1:] {
				memberSizeByte = kvPair.Key[len(kvPair.Key)-8:]
				memberSizeInt = int(binary.LittleEndian.Uint64(memberSizeByte))
				member = kvPair.Key[len(kvPair.Key)-8-memberSizeInt : len(kvPair.Key)-8]
				temp[string(member)] += utils.BytesToFloat64(kvPair.Value)
				unionSet = append(unionSet, member)
				encSetKey := bytes.Join([][]byte{destination, destinationVersion, member, memberSizeByte}, nil)
				err = tx.put(bucketID, encSetKey, utils.Float64ToBytes(temp[string(member)]), PERSISTENT, uint64(time.Now().UnixMilli()))
				if err != nil {
					return err
				}
			}
		}

		destinationMeta.size = uint32(len(temp))
		err = tx.put(bucketID, destination, encodeMetadata(destinationMeta), PERSISTENT, uint64(time.Now().UnixMilli()))
		if err != nil {
			return err
		}

		return nil
	}

	err = tx.managed(ZUnionStore)
	return
}

// Ascend 升序全局遍历
func (tx *Transaction) Ascend(bucketID uint64, handleFn func(key, value []byte) (bool, error)) ([]*KvPair, error) {
	return tx.Iterate(Ascend, bucketID, nil, nil, handleFn)
}

// Descend 降序全局遍历
func (tx *Transaction) Descend(bucketID uint64, handleFn func(key, value []byte) (bool, error)) ([]*KvPair, error) {
	return tx.Iterate(Descend, bucketID, nil, nil, handleFn)
}

// AscendRange 升序范围遍历
func (tx *Transaction) AscendRange(bucketID uint64, startKey, endKey []byte, handleFn func(key, value []byte) (bool, error)) ([]*KvPair, error) {
	return tx.Iterate(Ascend, bucketID, startKey, endKey, handleFn)
}

// DescendRange 降序范围遍历
func (tx *Transaction) DescendRange(bucketID uint64, startKey, endKey []byte, handleFn func(key, value []byte) (bool, error)) ([]*KvPair, error) {
	return tx.Iterate(Descend, bucketID, startKey, endKey, handleFn)
}

// AscendGreaterOrEqual 大于等于某个指定值升序遍历
func (tx *Transaction) AscendGreaterOrEqual(bucketID uint64, startKey []byte, handleFn func(key []byte, value []byte) (bool, error)) ([]*KvPair, error) {
	return tx.Iterate(Ascend, bucketID, startKey, nil, handleFn)
}

// DescendLessOrEqual 小于等于某个指定值降序遍历
func (tx *Transaction) DescendLessOrEqual(bucketID uint64, startKey []byte, handleFn func(key, value []byte) (bool, error)) ([]*KvPair, error) {
	return tx.Iterate(Descend, bucketID, startKey, nil, handleFn)
}

// 返回的是无序数据
// 存储开销低，执行开销低
func (tx *Transaction) IterateUnordered(bucketID uint64, startKey, endKey []byte, handleFn func(key, value []byte) (bool, error)) ([]*KvPair, error) {
	if tx.isClosed() {
		return nil, ErrTxClosed
	}

	// 处理事务中新加入的待写KvPair，保证事务内部的一致性
	KvPairInPendingWrites := make(map[string][]byte)
	for key, pendingKvPair := range tx.pendingWrites {
		// 筛选出大于等于目标key且未被删除的键
		if startKey != nil && bytes.Compare([]byte(key), startKey) >= 0 && pendingKvPair.Type != KvPairDeleted {
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

	tx.tm.db.Index.Iterate(Ascend, bucketID, startKey, endKey, internalHandleFn)

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
func (tx *Transaction) Iterate(iterateType IterateType, bucketID uint64, startKey, endKey []byte, handleFn func(key, value []byte) (bool, error)) ([]*KvPair, error) {
	if tx.isClosed() {
		return nil, ErrTxClosed
	}

	// 处理事务中新加入的待写KvPair，保证事务内部的一致性
	KvPairInPendingWrites := make(map[string][]byte)
	if iterateType == Ascend {
		for key, pendingKvPair := range tx.pendingWrites {
			// 筛选出大于等于目标key且未被删除的键
			if startKey != nil && bytes.Compare([]byte(key), startKey) >= 0 && pendingKvPair.Type != KvPairDeleted {
				KvPairInPendingWrites[key] = pendingKvPair.Value
			}
		}
	} else if iterateType == Descend {
		for key, pendingKvPair := range tx.pendingWrites {
			// 筛选出大于等于目标key且未被删除的键
			if startKey != nil && bytes.Compare([]byte(key), startKey) <= 0 && pendingKvPair.Type != KvPairDeleted {
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

	tx.tm.db.Index.Iterate(Ascend, bucketID, startKey, endKey, internalHandleFn)

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
