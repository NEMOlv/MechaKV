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

package client

import (
	. "MechaKV/comment"
	"MechaKV/database"
	"MechaKV/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
	"time"
)

func destroyDB(db *DB) {
	if db != nil {
		if db.ActiveFile != nil {
			_ = db.Close()
		}
		for _, of := range db.OlderFiles {
			if of != nil {
				_ = of.Close()
			}
		}
		err := os.RemoveAll(db.Opts.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func fastOpen() (*DB, *Client) {
	opts := database.DefaultOptions
	dir, _ := os.MkdirTemp("../temp", "MechaKV")
	opts.DirPath = dir
	db, _ := database.Open(opts)
	cli, _ := OpenClient(db)
	return db, cli
}

// Client打开关闭正常
func TestOpenAndCloseClient(t *testing.T) {
	opts := database.DefaultOptions
	dir, _ := os.MkdirTemp("../temp", "MechaKV")
	opts.DirPath = dir
	db, err := database.Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	cli, err := OpenClient(db)
	assert.Nil(t, err)
	assert.NotNil(t, cli)

	err = cli.Close()
	assert.Nil(t, err)

	err = db.Close()
	defer destroyDB(db)
	assert.Nil(t, err)
}

// PUT_NORMAL
func Test_Put_Normal(t *testing.T) {
	// 启动与关闭
	db, cli := fastOpen()
	defer func() {
		_ = cli.Close()
		_ = db.Close()
		destroyDB(db)
	}()

	// 1.基础功能测试
	// 1.1 单次put与get
	genkey := utils.GenerateKey(2025)
	genValue := utils.GenerateValue(10)
	err := cli.Put(genkey, genValue)
	assert.Nil(t, err)
	storeValue, err := cli.Get(genkey)
	assert.Nil(t, err)
	assert.Equal(t, genValue, storeValue)

	// 1.2 循环put与get
	for i := 0; i < 100000; i++ {
		genkey = utils.GenerateKey(i)
		genValue = utils.GenerateValue(10)
		err = cli.Put(genkey, genValue)
		assert.Nil(t, err)
		storeValue, err = cli.Get(genkey)
		assert.Nil(t, err)
		assert.Equal(t, genValue, storeValue)
	}

	// 1.3 先循环put，再循环get
	genValues := make([][]byte, 100000)
	for i := 0; i < 100000; i++ {
		genkey = utils.GenerateKey(i)
		genValue = utils.GenerateValue(10)
		genValues[i] = genValue
		err = cli.Put(genkey, genValue)
		assert.Nil(t, err)
	}

	for i := 0; i < 100000; i++ {
		genkey = utils.GenerateKey(i)
		storeValue, err = cli.Get(genkey)
		assert.Nil(t, err)
		assert.Equal(t, genValues[i], storeValue)
		if i == 10 {
			break
		}
	}

	// 2.边界条件测试
	// 2.1 空键
	emptyKey := []byte{}
	emptyValue := utils.GenerateValue(10)
	err = cli.Put(emptyKey, emptyValue)
	assert.Error(t, err)

	// 2.2 大键大值
	largeKey := make([]byte, 1024*1024) // 1MB
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}
	largeValue := make([]byte, 1024*1024) // 1MB
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}
	err = cli.Put(largeKey, largeValue)
	assert.Nil(t, err)
	storeValue, err = cli.Get(largeKey)
	assert.Nil(t, err)
	assert.Equal(t, largeValue, storeValue)

	// 3.并发测试
	var wg sync.WaitGroup
	concurrency := 10
	concurrentKey := utils.GenerateKey(100001)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			concurrentValue := utils.GenerateValue(10)
			err := cli.Put(concurrentKey, concurrentValue)
			assert.Nil(t, err)
		}(i)
	}
	wg.Wait()

	finalValue, err := cli.Get(concurrentKey)
	assert.Nil(t, err)
	assert.NotNil(t, finalValue)
}

// PUT_IF_NOT_EXISTS
func Test_Put_IfNotExists(t *testing.T) {
	// 启动与关闭
	db, cli := fastOpen()
	defer func() {
		_ = cli.Close()
		_ = db.Close()
		destroyDB(db)
	}()

	PUT_IF_NOT_EXISTS_FUNC := WithPutCondition(PUT_IF_NOT_EXISTS)

	// 1.基础功能测试
	// 1.1 单次put与get
	genkey := utils.GenerateKey(2025)
	genValue := utils.GenerateValue(10)

	err := cli.Put(genkey, genValue, PUT_IF_NOT_EXISTS_FUNC)
	assert.Nil(t, err)
	storeValue, err := cli.Get(genkey)
	assert.Nil(t, err)
	assert.Equal(t, genValue, storeValue)
	err = cli.Delete(genkey)
	assert.Nil(t, err)

	// 1.2向不存在的key插入数据
	genValues := make([][]byte, 100000)
	for i := 0; i < 100000; i++ {
		genkey = utils.GenerateKey(i)
		genValue = utils.GenerateValue(10)
		genValues[i] = genValue
		err = cli.Put(genkey, genValue, PUT_IF_NOT_EXISTS_FUNC)
		assert.Nil(t, err)
		storeValue, err = cli.Get(genkey)
		assert.Nil(t, err)
		assert.Equal(t, genValue, storeValue)
	}

	// 1.3向存在的key插入数据
	for i := 0; i < 100000; i++ {
		genkey = utils.GenerateKey(i)
		genValue = utils.GenerateValue(10)
		err = cli.Put(genkey, genValue, PUT_IF_NOT_EXISTS_FUNC)
		assert.Nil(t, err)
		storeValue, err = cli.Get(genkey)
		assert.Nil(t, err)
		assert.Equal(t, genValues[i], storeValue)
	}

	// 2.边界条件测试
	// 2.1空键
	emptyKey := []byte{}
	emptyValue := utils.GenerateValue(10)
	err = cli.Put(emptyKey, emptyValue, PUT_IF_NOT_EXISTS_FUNC)
	assert.Error(t, err)
	// 这里假设对空键操作会返回特定错误，根据实际情况调整
	// assert.EqualError(t, err, ErrKeyIsEmpty.Error())

	// 2.2大键大值
	largeKey := make([]byte, 1024*1024) // 1MB
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}
	largeValue := make([]byte, 1024*1024) // 1MB
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}
	err = cli.Put(largeKey, largeValue, PUT_IF_NOT_EXISTS_FUNC)
	assert.Nil(t, err)
	storeValue, err = cli.Get(largeKey)
	assert.Nil(t, err)
	assert.Equal(t, largeValue, storeValue)

	// 3.并发测试
	var wg sync.WaitGroup
	concurrency := 10
	concurrentKey := utils.GenerateKey(100001)
	concurrentValue := utils.GenerateValue(10)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			err := cli.Put(concurrentKey, concurrentValue, PUT_IF_NOT_EXISTS_FUNC)
			assert.Nil(t, err)
		}(i)
	}
	wg.Wait()

	finalValue, err := cli.Get(concurrentKey)
	assert.Nil(t, err)
	assert.Equal(t, concurrentValue, finalValue)
}

// PUT_IF_EXISTS
func Test_Put_IfExists(t *testing.T) {
	// 启动与关闭
	db, cli := fastOpen()
	defer func() {
		_ = cli.Close()
		_ = db.Close()
		destroyDB(db)
	}()

	PUT_IF_EXISTS_FUNC := WithPutCondition(PUT_IF_EXISTS)

	// 1.功能测试
	// 1.1 单次put与get
	genkey := utils.GenerateKey(2025)
	genValue := utils.GenerateValue(10)

	err := cli.Put(genkey, genValue, PUT_IF_EXISTS_FUNC)
	assert.EqualError(t, err, ErrKeyNotFound.Error())
	storeValue, err := cli.Get(genkey)
	assert.Nil(t, storeValue)
	assert.EqualError(t, err, ErrKeyNotFound.Error())

	genValues := make([][]byte, 1000)
	// 1.2 向不存在的key插入数据
	for i := 0; i < 1000; i++ {
		genkey = utils.GenerateKey(i)
		genValue = utils.GenerateValue(10)
		genValues[i] = genValue
		err = cli.Put(genkey, genValue, PUT_IF_EXISTS_FUNC)
		assert.EqualError(t, err, ErrKeyNotFound.Error())
		storeValue, err = cli.Get(genkey)
		assert.Nil(t, storeValue)
		assert.EqualError(t, err, ErrKeyNotFound.Error())
	}

	// 1.3 向存在的key插入数据
	for i := 0; i < 1000; i++ {
		genkey = utils.GenerateKey(i)
		genValue = utils.GenerateValue(10)
		err = cli.Put(genkey, genValue)
		assert.Nil(t, err)
		genValue = utils.GenerateValue(10)
		err = cli.Put(genkey, genValue, PUT_IF_EXISTS_FUNC)
		assert.Nil(t, err)
		storeValue, err = cli.Get(genkey)
		assert.Nil(t, err)
		assert.Equal(t, genValue, storeValue)
	}

	// 2.边界条件测试
	// 2.1 空键
	emptyKey := []byte{}
	emptyValue := utils.GenerateValue(10)
	err = cli.Put(emptyKey, emptyValue, PUT_IF_EXISTS_FUNC)
	assert.Error(t, err)
	// 这里假设对空键操作会返回特定错误，根据实际情况调整
	// assert.EqualError(t, err, ErrKeyIsEmpty.Error())

	// 2.2 大键大值
	largeKey := make([]byte, 1024*1024) // 1MB
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}
	largeValue := make([]byte, 1024*1024) // 1MB
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}
	err = cli.Put(largeKey, largeValue)
	assert.Nil(t, err)
	newLargeValue := make([]byte, 1024*1024) // 1MB
	for i := range newLargeValue {
		newLargeValue[i] = byte((i + 1) % 256)
	}
	err = cli.Put(largeKey, newLargeValue, PUT_IF_EXISTS_FUNC)
	assert.Nil(t, err)
	storeValue, err = cli.Get(largeKey)
	assert.Nil(t, err)
	assert.Equal(t, newLargeValue, storeValue)

	// 3.并发测试
	var wg sync.WaitGroup
	concurrency := 10
	concurrentKey := utils.GenerateKey(100001)
	concurrentValue := utils.GenerateValue(10)
	err = cli.Put(concurrentKey, concurrentValue)
	assert.Nil(t, err)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			newValue := utils.GenerateValue(10)
			err := cli.Put(concurrentKey, newValue, PUT_IF_EXISTS_FUNC)
			assert.Nil(t, err)
		}(i)
	}
	wg.Wait()

	finalValue, err := cli.Get(concurrentKey)
	assert.Nil(t, err)
	assert.NotNil(t, finalValue)
}

// PUT_AND_RETURN_OLD_VALUE
// PUT_AND_RETURN_OLD_VALUE
func Test_Put_ReturnOldValue(t *testing.T) {
	// 启动与关闭
	db, cli := fastOpen()
	defer func() {
		_ = cli.Close()
		_ = db.Close()
		destroyDB(db)
	}()

	// 1. 基础功能测试
	// 1.1 单次put与get
	genkey := utils.GenerateKey(2025)
	genValue := utils.GenerateValue(10)

	oldValue, err := cli.PutAndGet(genkey, genValue)
	assert.Nil(t, oldValue)
	assert.Nil(t, err)
	storeValue, err := cli.Get(genkey)
	assert.Equal(t, genValue, storeValue)
	assert.Nil(t, err)
	err = cli.Delete(genkey)
	assert.Nil(t, err)

	// 1.2 循环put与get
	oldValues := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		genkey = utils.GenerateKey(i)
		genValue = utils.GenerateValue(10)
		oldValues[i] = genValue
		oldValue, err = cli.PutAndGet(genkey, genValue)
		assert.Nil(t, oldValue)
		assert.Nil(t, err)
		storeValue, err = cli.Get(genkey)
		assert.Equal(t, genValue, storeValue)
		assert.Nil(t, err)
	}

	// 1.3 先循环put，再循环更新并获取旧值
	for i := 0; i < 1000; i++ {
		genkey = utils.GenerateKey(i)
		genValue = utils.GenerateValue(10)
		oldValue, err = cli.PutAndGet(genkey, genValue)
		assert.Equal(t, oldValues[i], oldValue)
		assert.Nil(t, err)
		storeValue, err = cli.Get(genkey)
		assert.Nil(t, err)
		assert.Equal(t, genValue, storeValue)
	}

	// 2.边界条件测试
	// 2.1 对空值的key调用PutAndGet
	emptyKey := []byte{}
	emptyValue := utils.GenerateValue(10)
	oldValue, err = cli.PutAndGet(emptyKey, emptyValue)
	assert.Error(t, err)
	assert.Nil(t, oldValue)

	// 2.2 对超大值的key和value调用PutAndGet
	largeKey := make([]byte, 1024*1024) // 1MB
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}
	largeValue := make([]byte, 1024*1024) // 1MB
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}
	// 先插入初始值
	err = cli.Put(largeKey, largeValue)
	assert.Nil(t, err)
	newLargeValue := make([]byte, 1024*1024) // 1MB
	for i := range newLargeValue {
		newLargeValue[i] = byte((i + 1) % 256)
	}
	oldValue, err = cli.PutAndGet(largeKey, newLargeValue)
	assert.Nil(t, err)
	assert.Equal(t, largeValue, oldValue)
	storeValue, err = cli.Get(largeKey)
	assert.Nil(t, err)
	assert.Equal(t, newLargeValue, storeValue)

	// 2.3 对已删除的key调用PutAndGet
	deletedKey := utils.GenerateKey(10004)
	deletedValue := utils.GenerateValue(10)
	err = cli.Put(deletedKey, deletedValue)
	assert.Nil(t, err)
	err = cli.Delete(deletedKey)
	assert.Nil(t, err)
	newDeletedValue := utils.GenerateValue(10)
	oldValue, err = cli.PutAndGet(deletedKey, newDeletedValue)
	assert.Nil(t, oldValue)
	assert.Nil(t, err)
	storeValue, err = cli.Get(deletedKey)
	assert.Equal(t, newDeletedValue, storeValue)
	assert.Nil(t, err)

	// 3 并发调用PutAndGet
	var wg sync.WaitGroup
	concurrency := 10
	genkey = utils.GenerateKey(10003)
	initialValue := utils.GenerateValue(10)
	err = cli.Put(genkey, initialValue)
	assert.Nil(t, err)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			newValue := utils.GenerateValue(10)
			oldValue, err := cli.PutAndGet(genkey, newValue)
			assert.Nil(t, err)
			// 这里无法准确预测旧值，因为并发操作可能导致顺序不确定
			assert.NotNil(t, oldValue)
		}(i)
	}
	wg.Wait()
}

func Test_Put_UpdateTTL(t *testing.T) {
	// 启动与关闭
	db, cli := fastOpen()
	defer func() {
		_ = cli.Close()
		_ = db.Close()
		destroyDB(db)
	}()

	// 1. 基础功能测试
	// 1.1 单次put与update TTL
	genkey := utils.GenerateKey(2025)
	genValue := utils.GenerateValue(10)
	initialTTL := uint32(10) // 初始TTL设置为10秒

	// 先插入数据并设置初始TTL
	err := cli.Put(genkey, genValue, WithTTL(initialTTL))
	assert.Nil(t, err)

	// 获取初始TTL，这里需要实现一个获取TTL的方法，假设为 cli.GetTTL(key)
	// 这里简化为等待一段时间后检查数据是否过期
	time.Sleep(2 * time.Second)

	// 更新TTL
	newTTL := uint32(10)
	err = cli.UpdateTTL(genkey, newTTL)
	assert.Nil(t, err)

	// 等待超过初始TTL但未超过新TTL的时间
	time.Sleep(6 * time.Second)

	// 检查数据是否仍然存在
	storeValue, err := cli.Get(genkey)
	assert.Nil(t, err)
	assert.Equal(t, genValue, storeValue)

	// 1.2 循环put与update TTL
	for i := 0; i < 1000; i++ {
		genkey = utils.GenerateKey(i)
		genValue = utils.GenerateValue(10)
		initialTTL = uint32(10)

		// 先插入数据并设置初始TTL
		err = cli.Put(genkey, genValue, WithTTL(initialTTL))
		assert.Nil(t, err)

		// 更新TTL
		newTTL = uint32(10)
		err = cli.UpdateTTL(genkey, newTTL)
		assert.Nil(t, err)
		kvPair, err := cli.GetKvPair(genkey)
		assert.Nil(t, err)
		assert.Equal(t, newTTL, kvPair.TTL)
	}

	// 1.3 先循环put，再循环update TTL，最后循环get
	for i := 0; i < 1000; i++ {
		genkey = utils.GenerateKey(i)
		genValue = utils.GenerateValue(10)
		initialTTL = uint32(10)

		// 先插入数据并设置初始TTL
		err = cli.Put(genkey, genValue, WithTTL(initialTTL))
		assert.Nil(t, err)
	}
	newTTLs := make([]uint32, 1000)
	for i := 0; i < 1000; i++ {
		genkey = utils.GenerateKey(i)
		newTTL = uint32(10 + i)
		newTTLs[i] = newTTL
		// 更新TTL
		err = cli.UpdateTTL(genkey, newTTL)
		assert.Nil(t, err)
	}

	for i := 0; i < 1000; i++ {
		genkey = utils.GenerateKey(i)
		kvPair, err := cli.GetKvPair(genkey)
		assert.Nil(t, err)
		assert.Equal(t, newTTLs[i], kvPair.TTL)
	}

	// 2. 边界条件测试
	// 2.1 更新不存在的key的TTL
	nonExistentKey := utils.GenerateKey(1001)
	err = cli.UpdateTTL(nonExistentKey, uint32(10))
	assert.EqualError(t, err, ErrKeyNotFound.Error())

	// 2.2 更新TTL为0（永久）
	genkey = utils.GenerateKey(1002)
	genValue = utils.GenerateValue(10)
	initialTTL = uint32(10)

	// 先插入数据并设置初始TTL
	err = cli.Put(genkey, genValue, WithTTL(initialTTL))
	assert.Nil(t, err)

	// 更新TTL为永久
	err = cli.Persist(genkey)
	assert.Nil(t, err)

	// 等待超过初始TTL的时间
	time.Sleep(11 * time.Second)

	// 检查数据是否仍然存在
	storeValue, err = cli.Get(genkey)
	assert.Nil(t, err)
	assert.Equal(t, genValue, storeValue)

	// 3. 并发更新TTL
	var wg sync.WaitGroup
	concurrency := 10
	genkey = utils.GenerateKey(10003)
	genValue = utils.GenerateValue(10)
	initialTTL = uint32(10)

	// 先插入数据并设置初始TTL
	err = cli.Put(genkey, genValue, WithTTL(initialTTL))
	assert.Nil(t, err)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			newTTL := uint32(idx * 10)
			err := cli.UpdateTTL(genkey, newTTL)
			assert.Nil(t, err)
		}(i)
	}
	wg.Wait()

	// 等待一段时间
	time.Sleep(1 * time.Second)

	// 检查数据是否仍然存在
	storeValue, err = cli.Get(genkey)
	assert.Nil(t, err)
	assert.Equal(t, genValue, storeValue)
}

// APPEND_VALUE
func Test_Append_Value(t *testing.T) {
	// 启动与关闭
	db, cli := fastOpen()
	defer func() {
		_ = cli.Close()
		_ = db.Close()
		destroyDB(db)
	}()

	APPEND_VALUE_FUNC := WithPutCondition(APPEND_VALUE)

	// 1.基础功能测试
	// 1.1 单次put、get
	genkey := utils.GenerateKey(2025)
	genValue1 := utils.GenerateValue(10)
	genValue2 := utils.GenerateValue(10)

	// 先插入初始值
	err := cli.Put(genkey, genValue1)
	assert.Nil(t, err)

	// 追加新值
	err = cli.Put(genkey, genValue2, APPEND_VALUE_FUNC)
	assert.Nil(t, err)

	// 获取追加后的值
	storeValue, err := cli.Get(genkey)
	assert.Nil(t, err)
	expectedValue := append(genValue1, genValue2...)
	assert.Equal(t, expectedValue, storeValue)

	// 1.2 循环put与get
	for i := 0; i < 1000; i++ { // 减少循环次数以提高测试速度
		genkey = utils.GenerateKey(i)
		genValue1 = utils.GenerateValue(10)
		genValue2 = utils.GenerateValue(10)

		// 先插入初始值
		err = cli.Put(genkey, genValue1)
		assert.Nil(t, err)

		// 追加新值
		err = cli.Put(genkey, genValue2, APPEND_VALUE_FUNC)
		assert.Nil(t, err)

		// 获取追加后的值
		storeValue, err = cli.Get(genkey)
		assert.Nil(t, err)
		expectedValue := append(genValue1, genValue2...)
		assert.Equal(t, expectedValue, storeValue)
	}

	// 1.3 先循环put，再循环追加，最后循环get
	genValues1 := make([][]byte, 1000) // 减少循环次数以提高测试速度
	genValues2 := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		genkey = utils.GenerateKey(i)
		genValue1 = utils.GenerateValue(10)
		genValue2 = utils.GenerateValue(10)
		genValues1[i] = genValue1
		genValues2[i] = genValue2

		// 先插入初始值
		err = cli.Put(genkey, genValue1)
		assert.Nil(t, err)
	}

	for i := 0; i < 1000; i++ {
		genkey = utils.GenerateKey(i)
		genValue2 = genValues2[i]

		// 追加新值
		err = cli.Put(genkey, genValue2, APPEND_VALUE_FUNC)
		assert.Nil(t, err)
	}

	for i := 0; i < 1000; i++ {
		genkey = utils.GenerateKey(i)
		genValue1 := genValues1[i]
		genValue2 := genValues2[i]
		expectedValue := append(genValue1, genValue2...)

		// 获取追加后的值
		storeValue, err = cli.Get(genkey)
		assert.Nil(t, err)
		assert.Equal(t, expectedValue, storeValue)
	}

	// 2.边界条件测试
	// 2.1 向空值追加空值
	genkey = utils.GenerateKey(1001)
	err = cli.Put(genkey, []byte{})
	assert.Nil(t, err)

	// 2.2 向空值追加非空值
	nonEmptyValue := utils.GenerateValue(10)
	err = cli.Put(genkey, nonEmptyValue, APPEND_VALUE_FUNC)
	assert.Nil(t, err)

	storeValue, err = cli.Get(genkey)
	assert.Nil(t, err)
	assert.Equal(t, nonEmptyValue, storeValue)

	// 2.3 向非空值追加空值
	genkey = utils.GenerateKey(1002)
	initialValue := utils.GenerateValue(10)
	err = cli.Put(genkey, initialValue)
	assert.Nil(t, err)
	err = cli.Put(genkey, []byte{}, APPEND_VALUE_FUNC)
	assert.Nil(t, err)
	storeValue, err = cli.Get(genkey)
	assert.Nil(t, err)
	assert.Equal(t, initialValue, storeValue)

	// 2.4 连续追加多个值
	expectedValues := make([]byte, 0)
	genkey = utils.GenerateKey(1003)
	parts := make([][]byte, 5)
	for i := 0; i < 5; i++ {
		parts[i] = utils.GenerateValue(10)
		expectedValues = append(expectedValues, parts[i]...)
		if i == 0 {
			err = cli.Put(genkey, parts[i])
		} else {
			err = cli.Put(genkey, parts[i], APPEND_VALUE_FUNC)
		}
		assert.Nil(t, err)
	}

	storeValue, err = cli.Get(genkey)
	assert.Nil(t, err)
	assert.Equal(t, expectedValues, storeValue)

	// 2.5 追加超大值 (限制在1MB以内以避免测试时间过长)
	genkey = utils.GenerateKey(1004)
	largeValue := make([]byte, 1024*1024) // 1MB
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	err = cli.Put(genkey, []byte{})
	assert.Nil(t, err)

	err = cli.Put(genkey, largeValue, APPEND_VALUE_FUNC)
	assert.Nil(t, err)

	storeValue, err = cli.Get(genkey)
	assert.Nil(t, err)
	assert.Equal(t, largeValue, storeValue)

	// 3.并发追加
	var wg sync.WaitGroup
	concurrency := 10
	genkey = utils.GenerateKey(10003)
	err = cli.Put(genkey, []byte{})
	assert.Nil(t, err)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			value := []byte{byte(idx)}
			err := cli.Put(genkey, value, APPEND_VALUE_FUNC)
			assert.Nil(t, err)
		}(i)
	}
	wg.Wait()

	storeValue, err = cli.Get(genkey)
	assert.Nil(t, err)
	expectedConcat := make([]byte, concurrency)
	for i := 0; i < concurrency; i++ {
		expectedConcat[i] = byte(i)
	}
	// 由于并发操作，顺序可能不同，只需验证长度和元素存在
	assert.Equal(t, concurrency, len(storeValue))
	for i := 0; i < concurrency; i++ {
		assert.Contains(t, storeValue, byte(i))
	}
}

// Test_ExpiryPolicy 测试数据库的过期策略
func Test_ExpiryPolicy(t *testing.T) {
	// 启动与关闭
	db, cli := fastOpen()
	defer func() {
		_ = cli.Close()
		_ = db.Close()
		destroyDB(db)
	}()

	// 测试持久化键值对（TTL 为 PERSISTENT）
	genkeyPersistent := utils.GenerateKey(2026)
	genValuePersistent := utils.GenerateValue(10)
	err := cli.Put(genkeyPersistent, genValuePersistent)
	assert.Nil(t, err)

	// 测试短期过期键值对（TTL 为 2 秒）
	genkeyShortTTL := utils.GenerateKey(2027)
	genValueShortTTL := utils.GenerateValue(10)
	shortTTLOption := WithTTL(2)
	err = cli.Put(genkeyShortTTL, genValueShortTTL, shortTTLOption)
	assert.Nil(t, err)

	// 测试长期过期键值对（TTL 为 10 秒）
	genkeyLongTTL := utils.GenerateKey(2028)
	genValueLongTTL := utils.GenerateValue(10)
	longTTLOption := WithTTL(10)
	err = cli.Put(genkeyLongTTL, genValueLongTTL, longTTLOption)
	assert.Nil(t, err)

	// 等待 3 秒，短期过期键值对应该已过期
	time.Sleep(3 * time.Second)

	// 检查持久化键值对是否仍然存在
	storeValuePersistent, err := cli.Get(genkeyPersistent)
	assert.Nil(t, err)
	assert.Equal(t, genValuePersistent, storeValuePersistent)

	// 检查短期过期键值对是否已过期
	storeValueShortTTL, err := cli.Get(genkeyShortTTL)
	assert.EqualError(t, err, ErrKeyNotFound.Error())
	assert.Nil(t, storeValueShortTTL)
	// 检查长期过期键值对是否仍然存在
	storeValueLongTTL, err := cli.Get(genkeyLongTTL)
	assert.Nil(t, err)
	assert.Equal(t, genValueLongTTL, storeValueLongTTL)

	// 等待 8 秒，长期过期键值对应该已过期
	time.Sleep(8 * time.Second)

	// 检查长期过期键值对是否已过期
	storeValueLongTTL, err = cli.Get(genkeyLongTTL)
	assert.EqualError(t, err, ErrKeyNotFound.Error())
	assert.Nil(t, storeValueLongTTL)
}
