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
	"MechaKV/iterator"
	"MechaKV/transaction"
	"time"
)

// 事务默认配置项
var DefaultTxOptions = transaction.DefaultTxOptions

type (
	KvPair             = database.KvPair
	Transaction        = transaction.Transaction
	TransactionManager = transaction.TransactionManager
	Iterator           = iterator.Iterator
	DB                 = database.DB
)

// Client 客户端结构体
type Client struct {
	db   *DB
	tm   *TransactionManager
	iter *Iterator
}

// OpenClient 打开客户端
func OpenClient(db *DB) (client *Client, err error) {
	// 打开事务管理器
	tm := transaction.NewTransactionManager(db)

	// 打开迭代器
	iter := iterator.NewIterator(db)

	// 构建客户端
	client = &Client{
		db:   db,
		tm:   tm,
		iter: iter,
	}

	return
}

// Close 关闭客户端
// 关闭客户端不代表关闭数据库，数据库需另外手动关闭
func (client *Client) Close() (err error) {
	client.tm.Close()
	client.iter.Close()
	client.db = nil
	client.tm = nil
	client.iter = nil
	return
}

// 添加数据
// Put 添加数据
func (client *Client) Put(key, value []byte, opOpts ...func(*PutOptions)) (err error) {
	// 1. 初始化默认选项
	var putOptions = &PutOptions{
		ttl:       DefaultPutOptions.ttl,          // 默认为"永久"值
		timestamp: uint64(time.Now().UnixMilli()), // 默认当前毫秒时间
		condition: DefaultPutOptions.condition,    // 默认为正常PUT
	}

	// 2. 应用用户传入的选项（覆盖默认值）
	for _, opOpt := range opOpts {
		opOpt(putOptions)
	}

	// 3.创建事务并提交
	var tx *Transaction
	if tx, err = client.tm.Begin(true, true, DefaultTxOptions); err != nil {
		return
	}
	_, err = tx.Put(key, value, putOptions.ttl, putOptions.timestamp, putOptions.condition)
	return
}

// 更新数据
// PutAndGet 更新数据的同时，返回旧值
func (client *Client) PutAndGet(key, value []byte, opOpts ...func(*PutOptions)) (oldValue []byte, err error) {
	// 1. 初始化默认选项
	var putOptions = &PutOptions{
		ttl:       DefaultPutOptions.ttl,          // 默认为"永久"值
		timestamp: uint64(time.Now().UnixMilli()), // 默认当前毫秒时间
		condition: DefaultPutOptions.condition,    // 默认为正常PUT
	}

	// 2. 应用用户传入的选项（覆盖默认值）
	for _, opOpt := range opOpts {
		opOpt(putOptions)
	}

	var tx *Transaction
	if tx, err = client.tm.Begin(true, true, DefaultTxOptions); err != nil {
		return
	}

	return tx.Put(key, value, putOptions.ttl, putOptions.timestamp, PUT_AND_RETURN_OLD_VALUE)
}

// UpdateTTL 更新TTL(生命周期)
func (client *Client) UpdateTTL(key []byte, ttl uint32) (err error) {
	var tx *Transaction
	if tx, err = client.tm.Begin(true, true, DefaultTxOptions); err != nil {
		return
	}
	_, err = tx.Put(key, nil, ttl, uint64(time.Now().UnixMilli()), UPDATE_TTL)
	return
}

// Persist 将TTL(生命周期)设置为永久
func (client *Client) Persist(key []byte) (err error) {
	return client.UpdateTTL(key, PERSISTENT)
}

// todo：Redis的高精度操作Incr、Decr、IncrBy、DecrBy，如果让用户自己实现，用户就需要自己考虑大数的高精度问题

// 获取数据
// Get 获取数据
func (client *Client) Get(key []byte) (value []byte, err error) {
	var tx *Transaction
	if tx, err = client.tm.Begin(false, true, DefaultTxOptions); err != nil {
		return
	}
	value, err = tx.Get(key)
	return
}

// GetKvPair 获取KvPair
func (client *Client) GetKvPair(key []byte) (kvPair *KvPair, err error) {
	var tx *Transaction
	if tx, err = client.tm.Begin(false, true, DefaultTxOptions); err != nil {
		return
	}
	kvPair, err = tx.GetKvPair(key)
	return
}

// 删除数据
// Delete 删除数据
func (client *Client) Delete(key []byte) (err error) {
	var tx *Transaction
	if tx, err = client.tm.Begin(true, true, DefaultTxOptions); err != nil {
		return
	}
	err = tx.Delete(key)
	return
}

// 批量操作
// BatchGet 批量获取数据
func (client *Client) BatchGet(key []byte) (value []byte, err error) {
	var tx *Transaction
	if tx, err = client.tm.Begin(false, true, DefaultTxOptions); err != nil {
		return
	} else {
		value, err = tx.Get(key)
		return
	}
}

// BatchPut 批量添加数据
func (client *Client) BatchPut(key, value [][]byte, opOpts ...func(*PutOptions)) (err error) {
	// 1. 初始化默认选项
	putOptions := &PutOptions{
		ttl:       PERSISTENT,                     // 默认为"永久"值
		timestamp: uint64(time.Now().UnixMilli()), // 默认当前毫秒时间
		condition: PUT_NORMAL,                     // 默认为正常PUT
	}

	// 2. 应用用户传入的选项（覆盖默认值）
	for _, opOpt := range opOpts {
		opOpt(putOptions)
	}

	var tx *Transaction
	if tx, err = client.tm.Begin(true, true, DefaultTxOptions); err != nil {
		return
	}
	_, err = tx.BatchPut(key, value, putOptions.ttl, putOptions.timestamp, putOptions.condition)
	return
}

// BatchPutAndGet 批量更新数据的同时，返回旧值
func (client *Client) BatchPutAndGet(key, value [][]byte, opOpts ...func(*PutOptions)) (oldValue [][]byte, err error) {
	// 1. 初始化默认选项
	putOptions := &PutOptions{
		ttl:       PERSISTENT,                     // 默认为"永久"值
		timestamp: uint64(time.Now().UnixMilli()), // 默认当前毫秒时间
	}

	// 2. 应用用户传入的选项（覆盖默认值）
	for _, opOpt := range opOpts {
		opOpt(putOptions)
	}

	var tx *Transaction
	if tx, err = client.tm.Begin(true, true, DefaultTxOptions); err != nil {
		return
	}

	return tx.BatchPut(key, value, putOptions.ttl, putOptions.timestamp, PUT_AND_RETURN_OLD_VALUE)
}

// BatchUpdateTTL 批量更新TTL(生命周期)
func (client *Client) BatchUpdateTTL(key [][]byte, ttl uint32) (err error) {
	var tx *Transaction
	if tx, err = client.tm.Begin(true, true, DefaultTxOptions); err != nil {
		return
	}
	_, err = tx.BatchPut(key, nil, ttl, uint64(time.Now().UnixMilli()), UPDATE_TTL)
	return
}

// BatchPersist 批量将TTL(生命周期)设置为永久
func (client *Client) BatchPersist(key [][]byte) (err error) {
	return client.BatchUpdateTTL(key, PERSISTENT)
}

// BatchDelete 批量删除数据
func (client *Client) BatchDelete(key [][]byte) (err error) {
	var tx *Transaction
	if tx, err = client.tm.Begin(true, true, DefaultTxOptions); err != nil {
		return
	} else {
		err = tx.BatchDelete(key)
		return
	}
}
