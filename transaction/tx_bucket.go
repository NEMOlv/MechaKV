package transaction

import (
	. "MechaKV/comment"
	"errors"
	"time"
)

func (tx *Transaction) getBucketID(bucketName string) (uint64, error) {
	bucketID, exist := tx.bucketNameToID[bucketName]
	if !exist {
		return 0, errors.New("没有该bucketID")
	}
	return bucketID, nil
}

func (tx *Transaction) GetBucket(bucketName string) error {
	_, err := tx.Put(bucketName, []byte(bucketName), nil, PERSISTENT, uint64(time.Now().UnixMilli()), PUT_NORMAL)
	if err != nil {
		return err
	}
	return nil
}

func (tx *Transaction) CreateBucket(bucketName string) error {
	_, err := tx.Put(bucketName, []byte(bucketName), nil, PERSISTENT, uint64(time.Now().UnixMilli()), PUT_NORMAL)
	if err != nil {
		return err
	}
	bucketID, err := tx.getBucketID(bucketName)
	if err != nil {
		return err
	}
	tx.bucketNameToID[bucketName] = bucketID
	return nil
}

func (tx *Transaction) DeleteBucket(bucketName string) error {
	bucketID, exsist := tx.tm.db.BucketManager.GetBucketID(bucketName)
	if !exsist {
		return errors.New("bucket is not exsist")
	}
	kvPairs, err := tx.IterateUnordered(bucketID, nil, nil, func(key, value []byte) (bool, error) {
		return true, nil
	})
	if err != nil {
		return err
	}
	keys := make([][]byte, len(kvPairs))
	for i, kvPair := range kvPairs {
		keys[i] = kvPair.Key
	}

	err = tx.BatchDelete(bucketName, keys)
	if err != nil {
		return err
	}

	DeleteBucket := func() error {
		if len(bucketName) == 0 {
			return ErrKeyIsEmpty
		}

		kvPair := tx.tm.KvPairPool.Get().(*KvPair)
		kvPair.TxId, kvPair.Type = tx.id, KvPairDeleted
		kvPair.Key = []byte(bucketName)
		kvPair.KeySize, kvPair.ValueSize = uint32(len(kvPair.Key)), 0
		kvPair.Timestamp, kvPair.TTL = uint64(time.Now().UnixMilli()), PERSISTENT

		tx.pendingWrites[bucketName] = kvPair

		return nil
	}

	err = tx.managed(DeleteBucket)
	if err != nil {
		return err
	}

	delete(tx.bucketNameToID, bucketName)

	return nil
}
