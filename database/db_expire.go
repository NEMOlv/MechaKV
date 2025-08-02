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
	"bytes"
	"context"
	"log"
	"math/rand"
	"time"
)

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
	if db.expiryMonitor.cancel != nil {
		db.expiryMonitor.cancel()
	}
	return
}

// CheckExpiredKeys 每100ms随机选择20个key进行过期检查
func (db *DB) StartExpiryMonitor() {
	// 使用带取消功能的上下文
	ctx, cancel := context.WithCancel(db.expiryMonitor.ctx)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			db.DBLock.Lock()
			keys, slotNum := db.randomSelectKeys(20)
			if keys != nil {
				err := db.checkAndDeleteExpiredKey(keys, slotNum)
				if err != nil {
					log.Fatal(err)
					return
				}
			}
			db.DBLock.Unlock()
		}
	}
}

// randomSelectKeys 从KeySlots中随机选择指定数量的key
func (db *DB) randomSelectKeys(keyNums int) ([][]byte, uint16) {
	var randKeys [][]byte
	var randSlot keyPointer

	hasKeySlotNums := make([]uint16, 0)

	for i, KeySlot := range db.KeySlots {
		if len(KeySlot) > 0 {
			hasKeySlotNums = append(hasKeySlotNums, i)
		}
	}

	if len(hasKeySlotNums) > 0 {
		slotNum := hasKeySlotNums[rand.Intn(len(hasKeySlotNums))]
		randSlot = db.KeySlots[slotNum]

		if len(randSlot) <= 20 {
			keyNums = len(randSlot)
		}

		for i := range keyNums {
			randKeys = append(randKeys, *randSlot[i])
		}
		return randKeys, slotNum
	}

	return nil, 0
}

// checkAndDeleteExpiredKey 检查key是否过期，如果过期则删除
func (db *DB) checkAndDeleteExpiredKey(keys [][]byte, slotNum uint16) (err error) {
	// 这里需要实现检查key是否过期的逻辑
	// 假设我们有一个函数 GetTTL 用于获取key的TTL
	// 如果TTL小于等于0，则表示key已过期

	// 从内存数据结构中取出 key 对应的索引信息
	for _, key := range keys {
		kvPairPos := db.Index.Get(key)

		// 从数据库中查找
		var kvPair *KvPair
		if kvPair, err = db.GetKvPairByPosition(kvPairPos); err != nil {
			return err
		}

		// 过期key没必要在数据文件中追加一条delete记录
		// 因为不管是主动删除过期key还是被动删除过期key，都会在第一次的时候进行IsExpired判断
		// 如果key已过期是不会返回的，并且会将其从索引中删除
		// 到合并阶段可以同时比对tll进行清理
		if kvPair.IsExpired() {
			db.Index.Delete(key)
			for idx, deleteKey := range db.KeySlots[slotNum] {
				if bytes.Equal(kvPair.Key, *deleteKey) {
					db.KeySlots[slotNum] = append(db.KeySlots[slotNum][:idx], db.KeySlots[slotNum][idx+1:]...)
					break
				}
			}
		}
	}
	return
}
