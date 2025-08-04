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

package datafile

import (
	. "MechaKV/comment"
	"encoding/binary"
	"github.com/valyala/bytebufferpool"
	"hash/crc32"
	"strconv"
	"time"
)

// 数据文件中数据的组织模式

//+--------+--------+------+---------+-----------+--------+----------+------------+--------+--------+
//| crc    | status | type | tx id   | timestamp | TTL    | key size | value size | key    | value  |
//+--------+--------+------+---------+-----------+--------+----------+------------+--------+--------+
//| []byte | byte   | byte | unit64  | unit64    | uint32 | uint32   | uint32     | []byte | []byte |
//| 4      | 1      | 1    | max(10) | max(10)   | max(5) | max(5)   | max(5)     | ksz    | vsz    |
//+--------+--------+------+---------+-----------+--------+----------+------------+--------+--------+

// KvPairPos 数据内存索引，描述数据在磁盘上的位置
type KvPairPos struct {
	Key []byte
	// 文件id：表示将数据存储到了哪个文件当中
	Fid uint32
	// 偏移：表示将数据存储到了文件中的哪个位置
	Offset int64
	// 标识数据在磁盘上的大小
	Size uint32
	Type KvPairType
}

func NewKvPairPos(fid uint32, offset int64, size uint32) KvPairPos {
	return KvPairPos{
		Fid:    fid,
		Offset: offset,
		Size:   size,
	}
}

// KvPair 写入到数据文件的键值对
type KvPair struct {
	// header
	Crc        uint32
	TxFinshed  KvPairType
	Type       KvPairType
	TxId       uint64
	Timestamp  uint64
	TTL        uint32
	KeySize    uint32
	ValueSize  uint32
	HeaderSize uint32
	// KvPair
	Key   []byte
	Value []byte
}

func NewKvPair(key []byte, value []byte, kvPairType KvPairType) KvPair {
	return KvPair{
		Key:       key,
		Value:     value,
		Type:      kvPairType,
		Timestamp: uint64(time.Now().UnixMilli()),
	}
}

// buf *bytebufferpool.ByteBuffer
func (kvPair *KvPair) EncodeKvPair(header []byte, KvPairBuffer *bytebufferpool.ByteBuffer) []byte {
	// 如果kvPair.TxFinshed == TxFinished，代表手动提交事务完成，通常
	// 则下标为4的header字节数组存储TxFinshed，下标为4的header字节数组存储kvPairType
	index := 4
	if kvPair.TxFinshed == TxFinished {
		index += binary.PutUvarint(header[index:], uint64(kvPair.TxFinshed))
		index += binary.PutUvarint(header[index:], uint64(kvPair.Type))
	} else {
		//println(kvPair.Type)
		index += binary.PutUvarint(header[index:], uint64(kvPair.Type))
		//println(index)
	}

	// 5字节之后，存储的是TxID、Timestamp、TTL、ksz、vsz
	// 使用变长类型，节省空间
	index += binary.PutUvarint(header[index:], kvPair.TxId)
	index += binary.PutUvarint(header[index:], kvPair.Timestamp)
	index += binary.PutUvarint(header[index:], uint64(kvPair.TTL))
	index += binary.PutUvarint(header[index:], uint64(kvPair.KeySize))
	index += binary.PutUvarint(header[index:], uint64(kvPair.ValueSize))
	kvPair.HeaderSize = uint32(index)

	_, _ = KvPairBuffer.Write(header[:index])
	_, _ = KvPairBuffer.Write(kvPair.Key)
	_, _ = KvPairBuffer.Write(kvPair.Value)

	// 对整个kvPairPos计算CRC
	crc := crc32.ChecksumIEEE(KvPairBuffer.B[4:])
	binary.LittleEndian.PutUint32(KvPairBuffer.B[:4], crc)

	return KvPairBuffer.Bytes()
}

func (kvPair *KvPair) DecodeKvPairHeader(buf []byte) {
	if len(buf) < 4 {
		return
	}

	// CRC
	crc := binary.LittleEndian.Uint32(buf[:4])
	var index uint32 = 4

	// status 事务是否提交
	// kvPairType 键值对的类型
	var kvPairType uint64

	// 如果第一个字节中的数据为TxFinished：
	//		代表该事务是单记录提交，直接使用status接收TxFinished，用kvPairType接收type
	// 如果第一个字节中的数据不为TxFinished：
	//      代表该事务是多记录提交，使用一个单独TxFinished记录作为标识，此时使用kvPairType接收type和status
	status, _ := binary.Uvarint(buf[index:])
	if KvPairType(status) == TxFinished {
		index++
		kvPairType, _ = binary.Uvarint(buf[index:])
		index++
	} else {
		kvPairType, _ = binary.Uvarint(buf[index:])
		index++
	}

	// batch id
	txId, n := binary.Uvarint(buf[index:])
	index += uint32(n)

	// timestamp
	timestamp, n := binary.Uvarint(buf[index:])
	index += uint32(n)

	// TTL生命周期
	ttl, n := binary.Uvarint(buf[index:])
	index += uint32(n)

	// key size
	keySize, n := binary.Uvarint(buf[index:])
	index += uint32(n)

	// value size
	valueSize, n := binary.Uvarint(buf[index:])
	index += uint32(n)

	// 将header信息加入kvPair
	kvPair.TxFinshed = KvPairType(status)
	kvPair.Type = uint8(kvPairType)
	kvPair.Crc = crc
	kvPair.TxId = txId
	kvPair.Timestamp = timestamp
	kvPair.TTL = uint32(ttl)
	kvPair.KeySize = uint32(keySize)
	kvPair.ValueSize = uint32(valueSize)
	kvPair.HeaderSize = index
}

func (kvPair *KvPair) DecodeKvPair(buf []byte) {
	// key
	var index uint32 = 0
	key := make([]byte, kvPair.KeySize)
	copy(key[:], buf[:kvPair.KeySize])
	index += kvPair.KeySize

	// value
	value := make([]byte, kvPair.ValueSize)
	copy(value[:], buf[index:index+kvPair.ValueSize])
	index += kvPair.ValueSize

	kvPair.Key = key
	kvPair.Value = value
}

func (kvPair *KvPair) PayloadSize() int64 {
	return int64(kvPair.KeySize) + int64(kvPair.ValueSize)
}

func (kvPair *KvPair) Size() uint32 {
	return kvPair.HeaderSize + kvPair.KeySize + kvPair.ValueSize
}

func computeKvPairCRC(kvPair *KvPair, headerWithoutCRC []byte) uint32 {
	if kvPair == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(headerWithoutCRC)
	crc = crc32.Update(crc, crc32.IEEETable, kvPair.Key)
	crc = crc32.Update(crc, crc32.IEEETable, kvPair.Value)

	return crc
}

// EncodeKvPairPos 对位置信息进行编码
func EncodeKvPairPos(pos *KvPairPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	return buf[:index]
}

// DecodeKvPairPos 对位置信息进行解码
func DecodeKvPairPos(buf []byte) *KvPairPos {
	var index = 0
	fileID, n := binary.Varint(buf[index:])
	offset, _ := binary.Varint(buf[index+n:])

	kvPairPos := &KvPairPos{
		Fid:    uint32(fileID),
		Offset: offset,
	}

	return kvPairPos
}

// IsExpired checks the ttl if expired or not.
// IsExpired 检查ttl是否过期
func IsExpired(ttl uint32, timestamp uint64) bool {
	// 如果ttl是持久化，那么没有过期，返回false
	if ttl == PERSISTENT {
		return false
	}

	// 获取现在的时间
	now := time.UnixMilli(time.Now().UnixMilli())
	// 获取时间戳
	expireTime := time.UnixMilli(int64(timestamp))
	// 过期时间 = 时间戳+生命周期
	expireTime = expireTime.Add(time.Duration(ttl) * time.Second)
	// 判断过期时间是否比现在的时间小
	//		是的话说明过期了，返回true
	//		否的话说明没过期，返回false
	return expireTime.Before(now)
}

func (kvPair *KvPair) IsExpired() bool {
	return IsExpired(kvPair.TTL, kvPair.Timestamp)
}

func EncodeMergeFinished(value int) []byte {
	keyByte := []byte(MergeFinishedKey)
	valueByte := []byte(strconv.Itoa(value))
	encBytes := make([]byte, 4+1+10+len(keyByte)+len(valueByte))

	index := 4
	index += binary.PutUvarint(encBytes[index:], uint64(len(keyByte)))
	index += binary.PutUvarint(encBytes[index:], uint64(len(valueByte)))
	copy(encBytes[index:], keyByte)
	index += len(keyByte)
	copy(encBytes[index:], valueByte)
	index += len(valueByte)
	encBytes = encBytes[:index]
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)
	return encBytes
}

func DecodeMergeFinished(buf []byte) ([]byte, error) {
	// 获取存储的CRC
	getCRC := binary.LittleEndian.Uint32(buf[:4])
	// 计算当前的CRC
	computCRC := crc32.ChecksumIEEE(buf[4:])
	// 判断CRC是否一致
	if getCRC != computCRC {
		return nil, ErrInvalidCRC
	}

	index := 4
	keySize, n := binary.Uvarint(buf[index:])
	index += n
	valueSize, n := binary.Uvarint(buf[index:])
	index += n
	key := buf[index : index+int(keySize)]
	index += int(keySize)
	if string(key) != MergeFinishedKey {
		return nil, ErrNotMergeFinishedFile
	}
	value := buf[index : index+int(valueSize)]

	return value, nil
}
