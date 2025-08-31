package bucket

import (
	. "MechaKV/datafile"
	"MechaKV/utils"
	"errors"
	"github.com/bwmarrin/snowflake"
	"github.com/valyala/bytebufferpool"
	"os"
	"sync"
	"time"
)

type BucketLockItem struct {
	lock     *sync.RWMutex
	status   string // "active" / "deleting"
	refCount int    // 引用计数，记录当前持有锁的操作数
}

func NewBucketLockItem() *BucketLockItem {
	return &BucketLockItem{
		lock:     new(sync.RWMutex),
		status:   "active",
		refCount: 0,
	}
}

type BucketManagerOptions struct {
	// 数据库目录
	DirPath    string
	BucketPath string
	// 数据文件大小
	BucketFileSize int64
	// 每次写入数据是否持久化
	SyncWrites bool
	// 累计写到多少字节后进行持久化
	BytesPerSync uint
}

var DefaultBucketManagerOptions = BucketManagerOptions{
	// 数据库目录: 默认存储在temp目录
	DirPath: os.TempDir(),
	// 数据文件大小：默认256MB
	BucketFileSize: 256 * 1024 * 1024,
	// 每条数据持久化：默认不开启
	SyncWrites: false,
	// N字节数据持久化：默认不开启
	BytesPerSync: 0,
}

type BucketManager struct {
	// 保证从BucketLock中获取锁是单线程的
	BucketManagerLock *sync.RWMutex
	// 保证每个Bucket都有一把自己的锁
	BucketLock map[uint64]*BucketLockItem
	// Buket Name-ID映射
	BucketNameToID map[string]uint64
	// Buket ID-Name映射
	BucketIDToName map[uint64]string
	// BucketID生成器
	BucketIdGenerater *snowflake.Node
	// BucketFile 用于存储桶名和ID的映射关系
	BucketFile *DataFile
	// bytesWrite
	bytesWrite uint
	// opts
	Opts BucketManagerOptions
}

func NewBucketManager(bucketPath string) (*BucketManager, error) {
	bucketIdGenerater, err := snowflake.NewNode(1)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}

	DefaultBucketManagerOptions.BucketPath = bucketPath

	return &BucketManager{
		BucketManagerLock: new(sync.RWMutex),
		BucketLock:        make(map[uint64]*BucketLockItem),
		BucketNameToID:    make(map[string]uint64),
		BucketIDToName:    make(map[uint64]string),
		BucketIdGenerater: bucketIdGenerater,
		Opts:              DefaultBucketManagerOptions,
	}, nil
}

func (bm *BucketManager) GenerateBucketID() uint64 {
	bm.BucketManagerLock.Lock()
	defer bm.BucketManagerLock.Unlock()

	return uint64(bm.BucketIdGenerater.Generate())
}

func (bm *BucketManager) GetBucketID(BucketName string) (uint64, bool) {
	bm.BucketManagerLock.RLock()
	defer bm.BucketManagerLock.RUnlock()

	BucketID, exist := bm.BucketNameToID[BucketName]
	if !exist {
		return 0, false
	}
	return BucketID, true
}

func (bm *BucketManager) GetBucketName(BucketID uint64) (string, bool) {
	bm.BucketManagerLock.RLock()
	defer bm.BucketManagerLock.RUnlock()

	BucketName, exist := bm.BucketIDToName[BucketID]
	if !exist {
		return "", false
	}
	return BucketName, true
}

// 设置当前活跃文件
func (bm *BucketManager) setActiveDataFile() error {
	var nextFileID uint32 = 0
	// 如果活跃文件不为空，则新活跃文件ID为上一活跃文件ID+1
	if bm.BucketFile != nil {
		nextFileID = bm.BucketFile.FileID + 1
	}

	bucketFile, err := OpenBucketFile(bm.Opts.BucketPath, nextFileID)
	if err != nil {
		return err
	}

	bm.BucketFile = bucketFile

	return nil
}

func (bm *BucketManager) AppendBucket(kvPair *KvPair, KvPairHeader []byte, KvPairBuffer *bytebufferpool.ByteBuffer) error {
	// 判断当前活跃数据文件是否存在
	// 如果为空则初始化数据文件
	if bm.BucketFile == nil {
		if err := bm.setActiveDataFile(); err != nil {
			return err
		}
	}

	// 对写入数据编码

	encodeKvPair := kvPair.EncodeKvPair(KvPairHeader, KvPairBuffer)
	size := kvPair.Size()
	// 如果写入数据已经达到活跃文件的阈值，关闭活跃文件，打开新文件
	if bm.BucketFile.WriteOffset+int64(size) > bm.Opts.BucketFileSize {
		//	先持久化数据文件，保证已有的数据持久到磁盘中
		if err := bm.BucketFile.Sync(); err != nil {
			return err
		}
		// 打开新的数据文件
		if err := bm.setActiveDataFile(); err != nil {
			return err
		}
	}

	if err := bm.BucketFile.Write(encodeKvPair); err != nil {
		return err
	}

	bm.bytesWrite += uint(size)
	//根据用户配置决定是否持久化
	needSync := bm.Opts.SyncWrites
	if !needSync && bm.Opts.BytesPerSync > 0 && bm.bytesWrite > bm.Opts.BytesPerSync {
		needSync = true
	}
	if needSync {
		if err := bm.BucketFile.Sync(); err != nil {
			return err
		}
		if bm.bytesWrite > 0 {
			bm.bytesWrite = 0
		}
	}

	BucketName := string(kvPair.Key)
	BucketID := utils.BytesToUint64(kvPair.Value)
	bm.BucketNameToID[BucketName] = BucketID
	bm.BucketIDToName[BucketID] = BucketName
	bm.BucketLock[BucketID] = NewBucketLockItem()

	return nil
}

//func (bm *BucketManager) AppendBucket(bucket *Bucket) error {
//	// 判断当前Bucket文件是否存在
//	// 如果为空则初始化Bucket文件
//	if bm.BucketFile == nil {
//		if err := bm.setActiveDataFile(); err != nil {
//			return err
//		}
//	}
//
//	// 对写入数据编码
//
//	encodeKvPair := bucket.EncodeBucket()
//	size := bucket.Size()
//	// 如果写入数据已经达到活跃文件的阈值，关闭活跃文件，打开新文件
//	if bm.BucketFile.WriteOffset+int64(size) > bm.Opts.BucketFileSize {
//		//	先持久化数据文件，保证已有的数据持久到磁盘中
//		if err := bm.BucketFile.Sync(); err != nil {
//			return err
//		}
//		// 打开新的数据文件
//		if err := bm.setActiveDataFile(); err != nil {
//			return err
//		}
//	}
//
//	if err := bm.BucketFile.Write(encodeKvPair); err != nil {
//		return err
//	}
//
//	bm.bytesWrite += uint(size)
//	//根据用户配置决定是否持久化
//	needSync := bm.Opts.SyncWrites
//	if !needSync && bm.Opts.BytesPerSync > 0 && bm.bytesWrite > bm.Opts.BytesPerSync {
//		needSync = true
//	}
//	if needSync {
//		if err := bm.BucketFile.Sync(); err != nil {
//			return err
//		}
//		bm.bytesWrite = 0
//	}
//
//	bm.BucketNameToID[bucket.BucketName] = bucket.BucketID
//	bm.BucketIDToName[bucket.BucketID] = bucket.BucketName
//	bm.BucketLock[bucket.BucketID] = NewBucketLockItem()
//
//	return nil
//}

// 获取锁时检查状态并更新引用计数
func (bm BucketManager) HoldBucketLock(bucketID uint64, isWrite bool) error {
	// 临界区：获取 item, exists
	bm.BucketManagerLock.Lock()
	bucketLock, exists := bm.BucketLock[bucketID]
	bm.BucketManagerLock.Unlock()
	if !exists || bucketLock.status != "active" {
		return errors.New("bucket not exists") // 锁不存在，直接退出
	}
	// 引用计数+1
	if isWrite {
		bucketLock.lock.Lock()
	} else {
		bucketLock.lock.RLock()
	}
	bucketLock.refCount++

	return nil
}

// 释放锁时减少引用计数
func (bm BucketManager) ReleaseBucketLock(bucketID uint64, isWrite bool) error {
	bm.BucketManagerLock.Lock()
	bucketLock, exists := bm.BucketLock[bucketID]
	bm.BucketManagerLock.Unlock()

	if !exists || bucketLock.status == "deleting" {
		return errors.New("bucket not exists") // 锁不存在或锁正在被删除，直接退出
	}

	if isWrite {
		bucketLock.lock.Unlock()
	} else {
		bucketLock.lock.RUnlock()
	}
	// 引用次数-1
	bucketLock.refCount--

	return nil
}

// DeleteBucketLock 该操作如果不异步执行，会一直产生阻塞
func (bm BucketManager) DeleteBucketLock(bucketID uint64) error {
	// 临界区：
	//	1.获取bucketLock, exists
	//	2.根据exists和bucketLock.status，判断锁是否存在
	//	  2.1 若锁不存在则释放BucketManagerLock后，提前退出
	//	  2.2 若锁存在，则将锁标记未正在删除中，然后释放BucketManagerLock
	bm.BucketManagerLock.Lock()
	bucketLock, exists := bm.BucketLock[bucketID]
	if !exists || bucketLock.status == "deleting" {
		bm.BucketManagerLock.Unlock()
		return errors.New("bucket not exists")
	}
	// 标记为删除中，新的任何锁操作都会被拒绝
	bucketLock.status = "deleting"
	bm.BucketManagerLock.Unlock()

	// 每N毫秒进行一次检查，直到bucketLock.refCount==0
	// 等待引用计数归0（循环检查，避免死等）
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		bm.BucketManagerLock.Lock()
		if bm.BucketLock[bucketID].refCount == 0 {
			break
		} else {
			bm.BucketManagerLock.Unlock()
		}
		_ = <-ticker.C // 短暂等待后重试
	}

	delete(bm.BucketLock, bucketID)
	bm.BucketManagerLock.Unlock()

	return nil
}
