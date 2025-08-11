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
	"MechaKV/database"
	"context"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/valyala/bytebufferpool"
	"log"
	"strconv"
	"sync"
)

type KvPairBuffers = []*bytebufferpool.ByteBuffer

type TransactionManager struct {
	db         *database.DB
	KvPairPool sync.Pool
	TxPool     sync.Pool
	TxBuffers  map[uint64]KvPairBuffers
	// 可以使用一个 map 来管理所有活跃的事务
	ActiveTxs cmap.ConcurrentMap[string, *Transaction]
}

func NewTransactionManager(db *database.DB) *TransactionManager {
	tm := &TransactionManager{
		db:         db,
		KvPairPool: sync.Pool{New: func() interface{} { return &KvPair{} }},
		TxPool:     sync.Pool{New: func() interface{} { return &Transaction{} }},
		TxBuffers:  make(map[uint64]KvPairBuffers, 10000),
		ActiveTxs:  cmap.New[*Transaction](),
	}
	return tm
}

func (tm *TransactionManager) Close() {
	tm.db = nil
	// 优雅关闭
	for ActiveTx := range tm.ActiveTxs.IterBuffered() {
		if err := tm.Rollback(ActiveTx.Val); err != nil {
			// 强制关闭
			close(ActiveTx.Val.doneCh)
			log.Println("Rollback err:", err)
		}
	}
	// Clear之前要确保所有事务正确关闭
	tm.ActiveTxs.Clear()
}

func (tm *TransactionManager) Begin(isWrite, isAutoCommit bool, options *TxOptions) (*Transaction, error) {
	// 如果数据库为空，提前返回
	if tm.db == nil {
		// 返回nil,数据库关闭错
		return nil, ErrDBClosed
	}
	tx := tm.TxPool.Get().(*Transaction)
	// 传入事务管理器
	tx.tm = tm
	// 初始化待写数组
	tx.pendingWrites = make(map[string]*KvPair)
	// 配置当前事务是否为写事务和自动提交
	tx.isWrite, tx.isAutoCommit = isWrite, isAutoCommit
	// 配置事务的配置项
	tx.options = options
	// 初始化退出通知channel
	tx.doneCh = make(chan struct{})
	tm.lock(isWrite)
	// 雪花算法生成事务ID
	tx.id = uint64(tm.db.TxIdGenerater.Generate())
	tx.setStatusRunning()
	tm.ActiveTxs.Set(strconv.FormatUint(tx.id, 10), tx)

	// 每一个事务都要启动一个线程，如果短时间内启动非常多的事务，会导致性能影响
	// 默认不启用，如果启用超时管理，则启动超时监听goroutine
	// 启动超时监听goroutine：当ctx取消（超时）或事务正常关闭时触发
	if options.Timeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), options.Timeout)

		overtime := func(tx *Transaction) {
			select {
			case <-ctx.Done(): // 上下文取消（超时/手动取消）
				// 尝试回滚事务，忽略"已关闭"错误（可能已手动处理）
				if err := tm.Rollback(tx); err != nil && err != ErrCannotRollbackAClosedTx {
					// 记录回滚失败日志（如事务已提交，回滚会失败，属于正常情况）
					//tm.database.Logger.Printf("auto rollback tx %d failed: %v", tx.id, err)
					println("auto rollback tx %d failed: %v", tx.id, err)
				}
				defer cancel()
			case <-tx.doneCh: // 事务正常关闭（提交/回滚），无需处理
				defer cancel()
				return
			}
		}

		go overtime(tx)
	}

	return tx, nil
}

func (tm *TransactionManager) Commit(tx *Transaction) (err error) {
	// 事务管理器加锁，避免提交事务时，pendingWrites被修改
	defer func() {
		// 事务无论是否提交成功，都需要进行后清理
		// 待写数组置空
		for _, kvPair := range tx.pendingWrites {
			tm.KvPairPool.Put(kvPair)
		}
		tx.pendingWrites = nil
		// 从事务map中删除执行完毕的事务
		tm.ActiveTxs.Remove(strconv.FormatUint(tx.id, 10))
		tx.setStatusClosed()
		tm.unlock(tx.isWrite)
		tm.TxPool.Put(tx)
	}()

	// 如果事务已经关闭，返回无法提交已关闭事务的错误
	if tx.isClosed() {
		return ErrCannotCommitAClosedTx
	}

	// 如果数据库实例是nil
	// 关闭当前事务，并返回数据库关闭的错误
	if tm.db == nil {
		tx.setStatusClosed()
		return ErrDBClosed
	}

	// 如果pendingWrites为空直接返回
	if len(tx.pendingWrites) == 0 {
		return nil
	}

	// 将事务状态修改为提交中
	tx.setStatusCommitting()

	// 如果超过一批写入的大小则返回报错
	if uint(len(tx.pendingWrites)) > tx.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	// 写数据
	positions := make(map[string]*KvPairPos)
	//var kvPair *data.KvPair
	// 待写数组计数器，当cnt为0时，代表该事务的最后一条记录，为其添加TxFinished标识
	cnt := len(tx.pendingWrites)
	for key, pendingKvPair := range tx.pendingWrites {
		cnt--
		if cnt == 0 {
			pendingKvPair.TxFinshed = TxFinished
		}
		KvPairBuffer := bytebufferpool.Get()
		tm.TxBuffers[tx.id] = append(tm.TxBuffers[tx.id], KvPairBuffer)
		kvPairPos, err := tm.db.AppendKvPair(pendingKvPair, tm.db.KvPairHeader, KvPairBuffer)
		if err != nil {
			return err
		}

		positions[key] = kvPairPos
	}

	for _, KvPairBuffer := range tm.TxBuffers[tx.id] {
		bytebufferpool.Put(KvPairBuffer)
	}
	clear(tm.TxBuffers[tx.id])
	delete(tm.TxBuffers, tx.id)

	// 更新内存索引 和 KeySlots
	for key, kvPair := range tx.pendingWrites {
		pos := positions[key]
		var oldPos *KvPairPos
		if kvPair.Type == KvPairPuted {
			// 索引和插槽添加数据
			oldPos = tm.db.Index.Put(kvPair.Key, pos)
		} else if kvPair.Type == KvPairDeleted {
			// 索引删除数据
			oldPos, _ = tm.db.Index.Delete(kvPair.Key)
		}
		if oldPos != nil {
			tm.db.ReclaimSize += int64(oldPos.Size)
		}
	}

	// 根据配置决定是否持久化
	if tx.options.SyncWrites && tm.db.ActiveFile != nil {
		if err = tm.db.ActiveFile.Sync(); err != nil {
			return err
		}
	}

	close(tx.doneCh)
	return
}

// Rollback 处理的是未正确关闭的事务
func (tm *TransactionManager) Rollback(tx *Transaction) error {
	defer func() {
		if tx.status.Load().(TransactionStatus) == TransactionRunning {
			// 将事务状态修改为关闭
			tx.setStatusClosed()
			tm.unlock(tx.isWrite)
		} else {
			// 将事务状态修改为关闭
			tx.setStatusClosed()
		}
		tm.TxPool.Put(tx)
	}()

	// 如果数据库实例为空，则返回ErrDBClosed
	if tm.db == nil {
		// 将事务状态修改为关闭
		tx.setStatusClosed()
		return ErrDBClosed
	}

	// 如果事务处于提交状态，则返回无法回滚提交中的事务
	if tx.isCommitting() {
		return ErrCannotRollbackACommittingTx
	}

	// 如果事务处于关闭状态，则返回无法回滚提交中的事务
	if tx.isClosed() {
		return ErrCannotRollbackAClosedTx
	}

	// 清空待写数组
	tx.pendingWrites = nil
	// 丢弃该事务
	tm.ActiveTxs.Remove(strconv.FormatUint(tx.id, 10))
	close(tx.doneCh)

	return nil
}

// lock locks the database based on the transaction type.
func (tm *TransactionManager) lock(isWrite bool) {
	if isWrite {
		tm.db.DBLock.Lock()
	} else {
		tm.db.DBLock.RLock()
	}
}

// unlock unlocks the database based on the transaction type.
func (tm *TransactionManager) unlock(isWrite bool) {
	if isWrite {
		tm.db.DBLock.Unlock()
	} else {
		tm.db.DBLock.RUnlock()
	}
}
