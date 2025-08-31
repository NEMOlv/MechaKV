package transaction

import (
	. "MechaKV/comment"
	"MechaKV/utils"
	"errors"
	"time"
)

func (tx *Transaction) getBucketID(bucketName string) (uint64, error) {
	//println(bucketName)
	bucketID, exist := tx.bucketNameToID[bucketName]
	//println(bucketID)
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
	_, exist := tx.tm.db.BucketManager.GetBucketID(bucketName)
	if exist {
		return errors.New("bucket exist")
	}

	CreateBucket := func() error {
		bucketID := tx.tm.db.BucketManager.GenerateBucketID()
		tx.bucketNameToID[bucketName] = bucketID

		kvPair := tx.tm.KvPairPool.Get().(*KvPair)
		kvPair.TxId, kvPair.Type = tx.id, RecordPuted
		kvPair.Key, kvPair.Value = []byte(bucketName), utils.Uint64ToBytes(bucketID)
		kvPair.KeySize, kvPair.ValueSize = uint32(len(kvPair.Key)), uint32(len(kvPair.Value))
		kvPair.Timestamp, kvPair.TTL = uint64(time.Now().UnixMilli()), PERSISTENT

		tx.pendingWrites[bucketName] = kvPair
		return nil
	}

	err := tx.managed(CreateBucket)
	if err != nil {
		return err
	}

	return nil
}

func (tx *Transaction) DropBucket(bucketName string) error {
	// 判断bucket是否存在
	bucketID, exsist := tx.tm.db.BucketManager.GetBucketID(bucketName)
	if !exsist {
		return errors.New("bucket is not exsist")
	}

	DropBucket := func() error {
		// 批量删除bucket下的存储记录
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

		for _, key := range keys {
			if err = tx.delete(bucketID, key); err != nil {
				return err
			}
		}
		kvPair := tx.tm.KvPairPool.Get().(*KvPair)
		kvPair.Type = RecordDeleted
		kvPair.TxId = tx.id
		kvPair.Key = []byte(bucketName)

		tx.pendingWrites[bucketName] = kvPair
		return nil
	}

	err := tx.managed(DropBucket)
	if err != nil {
		return err
	}

	return nil
}
