package transaction

import (
	. "MechaKV/comment"
	"MechaKV/datafile"
	"sync/atomic"
)

type (
	KvPair    = datafile.KvPair
	KvPairPos = datafile.KvPairPos
)

type Transaction struct {
	tm            *TransactionManager
	id            uint64
	status        atomic.Value
	pendingWrites map[string]*KvPair
	isWrite       bool
	isAutoCommit  bool
	options       *TxOptions
	doneCh        chan struct{}
}

func (tx *Transaction) managed(fn func() error) (err error) {
	if tx.isClosed() {
		return ErrTxClosed
	}

	if tx.isAutoCommit {
		if err = fn(); err == nil {
			err = tx.tm.Commit(tx)
		} else {
			if RollbackErr := tx.tm.Rollback(tx); RollbackErr != nil {
				err = RollbackErr
			}
		}
	} else {
		err = fn()
	}
	return
}

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

// lock locks the database based on the transaction type.
func (tx *Transaction) lock() {
	if tx.isWrite {
		tx.tm.db.DBLock.Lock()
	} else {
		tx.tm.db.DBLock.RLock()
	}
}

// unlock unlocks the database based on the transaction type.
func (tx *Transaction) unlock() {
	if tx.isWrite {
		tx.tm.db.DBLock.Unlock()
	} else {
		tx.tm.db.DBLock.RUnlock()
	}
}

// setStatusCommitting will change the tx status to txStatusCommitting
// setStatusCommitting 将Tx修改为提交状态
func (tx *Transaction) setStatusCommitting() {
	status := TransactionCommitting
	tx.status.Store(status)
}

// setStatusClosed will change the tx status to txStatusClosed
// setStatusClosed 将Tx修改为关闭状态
func (tx *Transaction) setStatusClosed() {
	status := TransactionClosed
	tx.status.Store(status)
}

// setStatusRunning will change the tx status to txStatusRunning
// setStatusRunning 将Tx修改为执行状态
func (tx *Transaction) setStatusRunning() {
	status := TransactionRunning
	tx.status.Store(status)
}

// isRunning will check if the tx status is txStatusRunning
// isRunning 检查Tx是否处于执行状态
func (tx *Transaction) isRunning() bool {
	status := tx.status.Load().(TransactionStatus)
	return status == TransactionRunning
}

// isCommitting will check if the tx status is txStatusCommitting
// isCommitting 检查Tx是否处于提交状态
func (tx *Transaction) isCommitting() bool {
	status := tx.status.Load().(TransactionStatus)
	return status == TransactionCommitting
}

// isClosed will check if the tx status is txStatusClosed
// isClosed 检查Tx是否处于关闭状态
func (tx *Transaction) isClosed() bool {
	status := tx.status.Load().(TransactionStatus)
	return status == TransactionClosed
}
