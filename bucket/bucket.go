package bucket

//
//import (
//	. "MechaKV/comment"
//	"encoding/binary"
//	"hash/crc32"
//)
//
////+--------+--------+------+---------+----------+------------+--------+
////| crc    | status | type |  TxID   | BucketID |  Name size | name   |
////+--------+--------+------+---------+----------+------------+--------+
////| []byte | byte   | byte | unit64  |  unit64  |   uint16   | []byte |
////| 4      | 1      | 1    | max(10) |  max(10) |   max(5)   | nsz    |
////+--------+--------+------+---------+----------+------------+--------+
//
//// Bucket 写入到数据文件结构体
//type Bucket struct {
//	// header
//	Crc            uint32
//	RecordType     RecordType
//	TxFinshed      RecordType
//	TxID           uint64
//	BucketID       uint64
//	BucketNameSize uint16 // 最大不超过64
//	HeadSize       uint32
//	BucketName     string
//}
//
//func (bucket *Bucket) Size() uint32 {
//	return uint32(bucket.BucketNameSize) + bucket.HeadSize
//}
//
//func (bucket *Bucket) EncodeBucket() []byte {
//	buf := make([]byte, MaxBucketHeaderSize)
//	index := 4
//	if bucket.TxFinshed == TxFinished {
//		index += binary.PutUvarint(buf[index:], uint64(bucket.TxFinshed))
//		index += binary.PutUvarint(buf[index:], uint64(bucket.RecordType))
//	} else {
//		//println(kvPair.Type)
//		index += binary.PutUvarint(buf[index:], uint64(bucket.RecordType))
//		//println(index)
//	}
//	// 使用变长类型，节省空间
//	index += binary.PutUvarint(buf[index:], bucket.TxID)
//	index += binary.PutUvarint(buf[index:], bucket.BucketID)
//	index += binary.PutUvarint(buf[index:], uint64(bucket.BucketNameSize))
//	copy(buf[index:], bucket.BucketName)
//
//	// 对整个kvPairPos计算CRC
//	crc := crc32.ChecksumIEEE(buf[4:])
//	binary.LittleEndian.PutUint32(buf[4:], crc)
//
//	return buf[:index]
//}
//
//func (bucket *Bucket) DecodeBucket(buf []byte) {
//	bucket.Crc = crc32.ChecksumIEEE(buf[:4])
//	index := 4
//
//	// status 事务是否提交
//	var recordType uint64
//	// 如果第一个字节中的数据为TxFinished：
//	//		代表该事务是单记录提交，直接使用status接收TxFinished，用kvPairType接收type
//	// 如果第一个字节中的数据不为TxFinished：
//	//      代表该事务是多记录提交，使用一个单独TxFinished记录作为标识，此时使用kvPairType接收type和status
//	txStatus, _ := binary.Uvarint(buf[index:])
//	if RecordType(txStatus) == TxFinished {
//		recordType, _ = binary.Uvarint(buf[index:])
//		index += 2
//	} else {
//		recordType, _ = binary.Uvarint(buf[index:])
//		index++
//	}
//
//	TxID, n := binary.Varint(buf[index:])
//	index += n
//	BucketID, n := binary.Varint(buf[index:])
//	index += n
//	NameSize, n := binary.Varint(buf[index:])
//	index += n
//
//	bucket.HeadSize = uint32(index)
//	bucket.TxFinshed = RecordType(txStatus)
//	bucket.RecordType = RecordType(recordType)
//	bucket.TxID = uint64(TxID)
//	bucket.BucketID = uint64(BucketID)
//	bucket.BucketNameSize = uint16(NameSize)
//	bucket.BucketName = string(buf[index : index+int(NameSize)])
//}
//
//func computeBucketCRC(bucket *Bucket, headerWithoutCRC []byte) uint32 {
//	if bucket == nil {
//		return 0
//	}
//
//	crc := crc32.ChecksumIEEE(headerWithoutCRC)
//	crc = crc32.Update(crc, crc32.IEEETable, []byte(bucket.BucketName))
//
//	return crc
//}
