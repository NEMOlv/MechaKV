package datafile

import (
	"encoding/binary"
	"errors"
	"github.com/bwmarrin/snowflake"
	"hash/crc32"
	"os"
	"sync"
	"time"
)

type bucketLockItem struct {
	lock     *sync.RWMutex
	status   string // "active" / "deleting"
	refCount int    // 引用计数，记录当前持有锁的操作数
}

func NewBucketLockItem() *bucketLockItem {
	return &bucketLockItem{
		lock:     new(sync.RWMutex),
		status:   "active",
		refCount: 0,
	}
}

type BucketManagerOptions struct {
	// 数据库目录
	DirPath string
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
	BucketLock map[uint64]*bucketLockItem
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

func NewBucketManager(dirPath string) (*BucketManager, error) {
	bucketIdGenerater, err := snowflake.NewNode(1)
	if err != nil {
		return nil, err
	}
	bucketFile, err := OpenBucketFile(dirPath, 0)
	if err != nil {
		return nil, err
	}
	return &BucketManager{
		BucketManagerLock: new(sync.RWMutex),
		BucketLock:        make(map[uint64]*bucketLockItem),
		BucketNameToID:    make(map[string]uint64),
		BucketIDToName:    make(map[uint64]string),
		BucketIdGenerater: bucketIdGenerater,
		BucketFile:        bucketFile,
	}, nil
}

// 获取锁时检查状态并更新引用计数
func (bm BucketManager) GetBucketLock(bucketID uint64, isWrite bool) bool {
	bm.BucketManagerLock.Lock()
	defer bm.BucketManagerLock.Unlock()

	item, exists := bm.BucketLock[bucketID]
	if !exists || item.status != "active" {
		return false // 非活跃状态，拒绝加锁
	}
	// 引用计数+1
	if isWrite {
		item.lock.Lock()
	} else {
		item.lock.RLock()
	}
	item.refCount++

	return true
}

// 释放锁时减少引用计数
func (bm BucketManager) ReleaseBucketLock(bucketID uint64, isWrite bool) {
	bm.BucketManagerLock.Lock()
	defer bm.BucketManagerLock.Unlock()

	if item, exists := bm.BucketLock[bucketID]; exists {
		if isWrite {
			item.lock.Unlock()
		} else {
			item.lock.RUnlock()
		}
		// 引用次数-1
		item.refCount--
	}
}

func (bm BucketManager) DeleteBucketLock(bucketID uint64) error {
	bm.BucketManagerLock.Lock()
	item, exists := bm.BucketLock[bucketID]
	if !exists || item.status == "deleted" {
		bm.BucketManagerLock.Unlock()
		return errors.New("bucket not exists")
	}
	// 标记为删除中，新操作会被拒绝
	item.status = "deleting"
	bm.BucketManagerLock.Unlock()

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
	defer bm.BucketManagerLock.Unlock()

	return nil
}

// Bucket 写入到数据文件结构体
type Bucket struct {
	// header
	Crc      uint32
	ID       uint64
	NameSize uint16
	HeadSize uint32
	Name     string
}

func (bucket *Bucket) Size() uint32 {
	return uint32(bucket.NameSize) + bucket.HeadSize
}

func (bucket *Bucket) EncodeBucket() []byte {
	buf := make([]byte, 4+binary.MaxVarintLen32+binary.MaxVarintLen64+binary.MaxVarintLen16)
	index := 4
	// 使用变长类型，节省空间
	index += binary.PutUvarint(buf[index:], bucket.ID)
	index += binary.PutUvarint(buf[index:], uint64(len(bucket.Name)))
	copy(buf[index:], bucket.Name)

	// 对整个kvPairPos计算CRC
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[4:], crc)

	return buf[:index]
}

func (bucket *Bucket) DecodeBucketHeader(buf []byte) {
	bucket.Crc = crc32.ChecksumIEEE(buf[:4])
	index := 4
	BucketID, n := binary.Varint(buf[index:])
	index += n
	NameSize, n := binary.Varint(buf[index:])
	index += n

	bucket.HeadSize = uint32(index)
	bucket.ID = uint64(BucketID)
	bucket.NameSize = uint16(NameSize)
}

func (bucket *Bucket) DecodeBucket(buf []byte) {
	bucket.Name = string(buf)
}

func computeBucketCRC(bucket *Bucket, headerWithoutCRC []byte) uint32 {
	if bucket == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(headerWithoutCRC)
	crc = crc32.Update(crc, crc32.IEEETable, []byte(bucket.Name))

	return crc
}

// 设置当前活跃文件
func (bm *BucketManager) setActiveDataFile() error {
	var initFileID uint32 = 0
	// 如果活跃文件不为空，则新活跃文件ID为上一活跃文件ID+1
	if bm.BucketFile != nil {
		initFileID = bm.BucketFile.FileID + 1
	}

	bucketFile, err := OpenBucketFile(bm.Opts.DirPath, initFileID)
	if err != nil {
		return err
	}

	bm.BucketFile = bucketFile

	return nil
}

func (bm *BucketManager) GetBucketID(BucketName string) (uint64, bool) {
	BucketID, exist := bm.BucketNameToID[BucketName]
	if !exist {
		return 0, false
	}
	return BucketID, true
}

func (bm *BucketManager) GetBucketName(BucketID uint64) (string, bool) {
	BucketName, exist := bm.BucketIDToName[BucketID]
	if !exist {
		return "", false
	}
	return BucketName, true
}

func (bm *BucketManager) GenerateBucketID() uint64 {
	return uint64(bm.BucketIdGenerater.Generate())
}

func (bm *BucketManager) AppendBucket(bucket *Bucket) error {
	// 内置写锁
	// 目前暂无读的操作，未来可能考虑维护读的操作
	bm.BucketManagerLock.Lock()
	defer bm.BucketManagerLock.Unlock()

	// 判断当前Bucket文件是否存在
	// 如果为空则初始化Bucket文件
	if bm.BucketFile == nil {
		if err := bm.setActiveDataFile(); err != nil {
			return err
		}
	}

	// 对写入数据编码

	encodeKvPair := bucket.EncodeBucket()
	size := bucket.Size()
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
		bm.bytesWrite = 0
	}
	return nil
}

func (bm *BucketManager) GenerateBucket(bucketName string) (err error) {
	bm.BucketManagerLock.Lock()
	defer bm.BucketManagerLock.Unlock()

	bucketID, exist := bm.GetBucketID(bucketName)
	if exist {
		return errors.New("bucket exist")
	}

	bucketID = bm.GenerateBucketID()
	bucket := &Bucket{
		Name: bucketName,
		ID:   bucketID,
	}

	err = bm.AppendBucket(bucket)
	if err != nil {
		return err
	}

	bm.BucketNameToID[bucketName] = bucketID
	bm.BucketIDToName[bucketID] = bucketName

	return
}

func (bm *BucketManager) BatchGenerateBucket(bucketNames []string) (err error) {
	bm.BucketManagerLock.Lock()
	defer bm.BucketManagerLock.Unlock()

	for _, bucketName := range bucketNames {
		bucketID, exist := bm.GetBucketID(bucketName)
		if exist {
			return errors.New("bucket exist")
		}

		bucketID = bm.GenerateBucketID()
		bucket := &Bucket{
			Name: bucketName,
			ID:   bucketID,
		}

		err = bm.AppendBucket(bucket)
		if err != nil {
			return err
		}

		bm.BucketNameToID[bucketName] = bucketID
		bm.BucketIDToName[bucketID] = bucketName
	}

	return
}

// todo deletBucket
