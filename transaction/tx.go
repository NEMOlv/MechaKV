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
	tm             *TransactionManager
	id             uint64
	status         atomic.Value
	txCommitType   TxCommitType
	bucketNameToID map[string]uint64
	pendingWrites  map[string]*KvPair
	isWrite        bool
	isAutoCommit   bool
	options        *TxOptions
	doneCh         chan struct{}
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
