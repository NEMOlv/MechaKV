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
	"bytes"
	"context"
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"
)

type ExpiryManager struct {
	expiryMonitor    *ExpiryMonitor
	KvPairExpirePool sync.Pool
	KeySlots         map[uint16][]*KvPairExpire
}

func NewExpiryManager() *ExpiryManager {
	expiryMonitor := NewExpiryMonitor()
	return &ExpiryManager{
		expiryMonitor:    expiryMonitor,
		KvPairExpirePool: sync.Pool{New: func() interface{} { return &KvPairExpire{} }},
		KeySlots:         make(map[uint16][]*KvPairExpire),
	}
}

type ExpiryMonitor struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func NewExpiryMonitor() *ExpiryMonitor {
	ctx, cancel := context.WithCancel(context.Background())

	return &ExpiryMonitor{
		ctx:    ctx,
		cancel: cancel,
	}
}

func (db *DB) CloseExpiryMonitor() {
	db.expiryManager.expiryMonitor.cancel()
	return
}

// CheckExpiredKeys 每100ms随机选择20个key进行过期检查
func (db *DB) StartExpiryMonitor() {
	// 使用带取消功能的上下文
	ctx, cancel := context.WithCancel(db.expiryManager.expiryMonitor.ctx)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			db.DBLock.Lock()
			keys, bukectIDs, slotNum := db.randomSelectKeys(20)
			if keys != nil {
				err := db.checkAndDeleteExpiredKey(bukectIDs, keys, slotNum)
				if err != nil {
					println(err)
					log.Fatal(err)
					return
				}
			}
			db.DBLock.Unlock()
		}
	}
}

// randomSelectKeys 从KeySlots中随机选择指定数量的key
func (db *DB) randomSelectKeys(keyNums int) ([][]byte, []uint64, uint16) {
	var randSlot []*KvPairExpire
	hasKeySlotNums := make([]uint16, 0)

	for i, KeySlot := range db.expiryManager.KeySlots {
		if len(KeySlot) > 0 {
			hasKeySlotNums = append(hasKeySlotNums, i)
		}
	}

	if len(hasKeySlotNums) > 0 {
		slotNum := hasKeySlotNums[rand.Intn(len(hasKeySlotNums))]
		randSlot = db.expiryManager.KeySlots[slotNum]

		if len(randSlot) <= 20 {
			keyNums = len(randSlot)
		}

		randKeys := make([][]byte, keyNums)
		randBucketIDs := make([]uint64, keyNums)

		for i := range keyNums {
			randKeys[i] = randSlot[i].Key
			randBucketIDs[i] = randSlot[i].BucketID
		}
		return randKeys, randBucketIDs, slotNum
	}

	return nil, nil, 0
}

// checkAndDeleteExpiredKey 检查key是否过期，如果过期则删除
func (db *DB) checkAndDeleteExpiredKey(bukectIDs []uint64, keys [][]byte, slotNum uint16) (err error) {
	// 这里需要实现检查key是否过期的逻辑
	// 假设我们有一个函数 GetTTL 用于获取key的TTL
	// 如果TTL小于等于0，则表示key已过期

	// 从内存数据结构中取出 key 对应的索引信息
	for i, key := range keys {
		kvPairPos := db.Index.Get(bukectIDs[i], key)

		// 从数据库中查找
		var kvPair *KvPair
		if kvPair, err = db.GetKvPairByPosition(kvPairPos); err != nil && !errors.Is(err, ErrKeyNotFound) {
			return err
		}

		// 过期key没必要在数据文件中追加一条delete记录
		// 因为不管是主动删除过期key还是被动删除过期key，都会在第一次的时候进行IsExpired判断
		// 如果key已过期是不会返回的，并且会将其从索引中删除
		// 到合并阶段可以同时比对tll进行清理
		if !errors.Is(err, ErrKeyNotFound) && kvPair.IsExpired() {
			db.Index.Delete(bukectIDs[i], key)
			for idx, deleteKvPair := range db.expiryManager.KeySlots[slotNum] {
				if bytes.Equal(kvPair.Key, deleteKvPair.Key) {
					db.expiryManager.KvPairExpirePool.Put(db.expiryManager.KeySlots[slotNum][idx])
					db.expiryManager.KeySlots[slotNum] = append(db.expiryManager.KeySlots[slotNum][:idx], db.expiryManager.KeySlots[slotNum][idx+1:]...)
					break
				}
			}
		}
	}
	return
}
