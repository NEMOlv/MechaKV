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
	"bytes"
	"encoding/binary"
	"errors"
	"time"
)

// string
func (tx *Transaction) Put(key, value []byte, ttl uint32, timestamp uint64, condition PutCondition) (oldValue []byte, err error) {
	if condition == PUT_NORMAL {
		PutNormal := func() error {
			return tx.put(key, value, ttl, timestamp)
		}

		err = tx.managed(PutNormal)
	} else if condition == PUT_IF_NOT_EXISTS {
		PutIfNotExists := func() error {
			getValue, getErr := tx.get(key)
			if getErr != nil && !errors.Is(getErr, ErrKeyNotFound) {
				return getErr
			}
			// 如果Key已经存在，则直接返回nil
			if getValue != nil {
				return nil
			}
			return tx.put(key, value, ttl, timestamp)
		}

		err = tx.managed(PutIfNotExists)
	} else if condition == PUT_IF_EXISTS {
		PutIfNotExists := func() error {
			_, getErr := tx.get(key)
			if getErr != nil {
				return getErr
			}
			return tx.put(key, value, ttl, timestamp)
		}

		err = tx.managed(PutIfNotExists)
	} else if condition == PUT_AND_RETURN_OLD_VALUE {
		PutAndReturnOldValue := func() error {
			var getErr error
			oldValue, getErr = tx.get(key)
			if getErr != nil && !errors.Is(getErr, ErrKeyNotFound) {
				return getErr
			}
			return tx.put(key, value, ttl, timestamp)
		}

		err = tx.managed(PutAndReturnOldValue)
	} else if condition == APPEND_VALUE {
		AppendValue := func() error {
			var getErr error
			oldValue, getErr = tx.get(key)
			if getErr != nil && !errors.Is(getErr, ErrKeyNotFound) {
				return getErr
			}
			if oldValue != nil {
				value = append(oldValue, value...)
			}

			return tx.put(key, value, ttl, timestamp)
		}
		err = tx.managed(AppendValue)
	} else if condition == UPDATE_TTL {
		UpdateTTL := func() error {
			var getErr error
			oldValue, getErr = tx.get(key)
			if getErr != nil {
				return getErr
			}
			return tx.put(key, oldValue, ttl, timestamp)
		}
		err = tx.managed(UpdateTTL)
	}

	if err != nil {
		return nil, err
	}
	return
}

func (tx *Transaction) BatchPut(key, value [][]byte, ttl uint32, timestamp uint64, condition PutCondition) (oldValue [][]byte, err error) {
	if condition == PUT_NORMAL {
		BatchPut := func() (err error) {
			for i := 0; i < len(key); i++ {
				if err = tx.put(key[i], value[i], ttl, timestamp); err != nil {
					return
				}
			}
			return
		}

		err = tx.managed(BatchPut)
	} else if condition == PUT_IF_NOT_EXISTS {
		BatchPutIfNotExists := func() (err error) {
			for i := 0; i < len(key); i++ {
				getValue, getErr := tx.get(key[i])
				if getErr != nil && !errors.Is(getErr, ErrKeyNotFound) {
					return getErr
				}
				// 如果Key已经存在，则直接返回nil
				if getValue != nil {
					return
				}
				if err = tx.put(key[i], value[i], ttl, timestamp); err != nil {
					return
				}
			}
			return
		}

		err = tx.managed(BatchPutIfNotExists)
	} else if condition == PUT_IF_EXISTS {
		BatchPutIfExists := func() (err error) {
			for i := 0; i < len(key); i++ {
				_, getErr := tx.get(key[i])
				if getErr != nil {
					return getErr
				}
				if err = tx.put(key[i], value[i], ttl, timestamp); err != nil {
					return
				}
			}
			return
		}

		err = tx.managed(BatchPutIfExists)
	} else if condition == PUT_AND_RETURN_OLD_VALUE {
		BatchPutAndReturnOldValue := func() (err error) {
			var getErr error
			var tempValue []byte
			for i := 0; i < len(key); i++ {
				tempValue, getErr = tx.get(key[i])
				if getErr != nil && !errors.Is(getErr, ErrKeyNotFound) {
					return getErr
				}
				if err = tx.put(key[i], value[i], ttl, timestamp); err != nil {
					oldValue = nil
					return
				}
				oldValue = append(oldValue, tempValue)
			}
			return
		}

		err = tx.managed(BatchPutAndReturnOldValue)
	} else if condition == UPDATE_TTL {
		BatchUpdateTTL := func() (err error) {
			var getErr error
			var tempValue []byte
			for i := 0; i < len(key); i++ {
				tempValue, getErr = tx.get(key[i])
				if getErr != nil {
					return getErr
				}
				if err = tx.put(key[i], tempValue, ttl, timestamp); err != nil {
					return
				}
			}
			return
		}
		err = tx.managed(BatchUpdateTTL)
	}

	if err != nil {
		return nil, err
	}

	return
}

func (tx *Transaction) Delete(key []byte) (err error) {
	err = tx.managed(func() (err error) {
		err = tx.delete(key)
		return err
	})
	return
}

func (tx *Transaction) BatchDelete(key [][]byte) (err error) {
	err = tx.managed(func() (err error) {
		for i := 0; i < len(key); i++ {
			if err = tx.delete(key[i]); err != nil {
				return
			}
		}
		return
	})
	return
}

func (tx *Transaction) Get(key []byte) (value []byte, err error) {
	err = tx.managed(func() (err error) {
		value, err = tx.get(key)
		return
	})
	return
}

func (tx *Transaction) GetKvPair(key []byte) (kvPair *KvPair, err error) {
	err = tx.managed(func() (err error) {
		kvPair, err = tx.getKvPair(key)
		return
	})
	return
}

func (tx *Transaction) BatchGet(key [][]byte) (value [][]byte, err error) {
	err = tx.managed(func() (err error) {
		var tempValue []byte
		for i := 0; i < len(key); i++ {
			if tempValue, err = tx.get(key[i]); err != nil {
				return
			}
			value = append(value, tempValue)
		}
		return
	})
	return
}

// set
func (tx *Transaction) SAdd(key, member []byte) (bool, error) {
	// 查找元数据
	meta, err := tx.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	version := make([]byte, 8)
	binary.LittleEndian.PutUint64(version, uint64(meta.version))
	encSetKey := bytes.Join([][]byte{key, version, member}, nil)

	ok := false
	if _, err = tx.Get(encSetKey); err == ErrKeyNotFound {
		meta.size++
		_, _ = tx.Put(key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()), PUT_NORMAL)
		_, _ = tx.Put(encSetKey, nil, PERSISTENT, uint64(time.Now().UnixMilli()), PUT_NORMAL)
		ok = true
	}
	return ok, nil
}

func (tx *Transaction) SIsMember(key, member []byte) (bool, error) {
	meta, err := tx.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	version := make([]byte, 8)
	binary.LittleEndian.PutUint64(version, uint64(meta.version))
	encSetKey := bytes.Join([][]byte{key, version, member}, nil)

	_, err = tx.Get(encSetKey)
	if err != nil && err != ErrKeyNotFound {
		return false, err
	}
	if err == ErrKeyNotFound {
		return false, nil
	}
	return true, nil
}

func (tx *Transaction) SRem(key, member []byte) (bool, error) {
	meta, err := tx.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	version := make([]byte, 8)
	binary.LittleEndian.PutUint64(version, uint64(meta.version))
	encSetKey := bytes.Join([][]byte{key, version, member}, nil)

	_, err = tx.Get(encSetKey)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return false, nil
		} else {
			return false, err
		}
	}

	meta.size--
	_, _ = tx.Put(key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()), PUT_NORMAL)
	_ = tx.Delete(encSetKey)

	return true, nil
}

// hash
func (tx *Transaction) HSet(key, field, value []byte) (bool, error) {
	// 从Value中查找元数据
	meta, err := tx.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	version := make([]byte, 8)
	binary.LittleEndian.PutUint64(version, uint64(meta.version))
	// 构造 Hash的 SubKey
	encHashSubKey := bytes.Join([][]byte{key, version, field}, nil)

	// 先查找是否存在
	var exist = true
	if _, err = tx.Get(encHashSubKey); errors.Is(err, ErrKeyNotFound) {
		exist = false
	}

	// 不存在则更新元数据
	if !exist {
		meta.size++
		_, _ = tx.Put(key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()), PUT_NORMAL)
	}
	_, _ = tx.Put(encHashSubKey, value, PERSISTENT, uint64(time.Now().UnixMilli()), PUT_NORMAL)

	return !exist, nil
}

func (tx *Transaction) HGet(key, field []byte) ([]byte, error) {
	meta, err := tx.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}

	// 这里应该返回一个ErrKeyNotFound
	if meta.size == 0 {
		return nil, nil
	}
	version := make([]byte, 8)
	binary.LittleEndian.PutUint64(version, uint64(meta.version))
	// 构造 Hash的 SubKey
	encHashSubKey := bytes.Join([][]byte{key, version, field}, nil)

	return tx.Get(encHashSubKey)
}

func (tx *Transaction) HDel(key, field []byte) (bool, error) {
	meta, err := tx.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	version := make([]byte, 8)
	binary.LittleEndian.PutUint64(version, uint64(meta.version))
	// 构造 Hash的 SubKey
	encHashSubKey := bytes.Join([][]byte{key, version, field}, nil)

	// 先查看是否存在
	var exist = true
	_, err = tx.Get(encHashSubKey)
	if err != nil {
		if !errors.Is(err, ErrKeyNotFound) {
			exist = false
		} else {
			return false, err
		}
	}

	if exist {
		meta.size--
		_, _ = tx.Put(key, encodeMetadata(meta), PERSISTENT, uint64(time.Now().UnixMilli()), PUT_NORMAL)
		_ = tx.Delete(encHashSubKey)
	}

	return exist, nil
}
