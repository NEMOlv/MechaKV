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

package benchmark

import (
	"MechaKV/client"
	. "MechaKV/comment"
	"MechaKV/database"
	"MechaKV/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

type DB = database.DB

func destroyDB(db *DB) {
	if db != nil {
		if db.ActiveFile != nil {
			_ = db.Close()
		}
		for _, of := range db.OlderFiles {
			if of != nil {
				_ = of.Close()
			}
		}
		err := os.RemoveAll(db.Opts.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func makeData(data_num, value_len int) ([][]byte, [][]byte) {
	genkeys := make([][]byte, 500000)
	genValues := make([][]byte, 500000)
	for n := 0; n < data_num; n++ {
		genkeys[n] = utils.GenerateKey(n)
		genValues[n] = utils.GenerateValue(value_len)
	}
	return genkeys, genValues
}

func fastOpen() (*DB, *client.Client) {
	opts := database.DefaultOptions
	dir, _ := os.MkdirTemp("../temp", "MechaKV")
	opts.DirPath = dir
	db, _ := database.Open(opts)
	cli, _ := client.OpenClient(db)
	return db, cli
}

func BenchmarkPut(b *testing.B) {
	// 启动与关闭
	db, cli := fastOpen()
	defer func() {
		_ = cli.Close()
		_ = db.Close()
		destroyDB(db)
	}()

	keys, values := makeData(b.N, 64)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := cli.Put(keys[i], values[i])
		assert.Nil(b, err)
	}
}

// 基准测试Get操作
func BenchmarkGet(b *testing.B) {
	db, cli := fastOpen()
	defer func() {
		_ = cli.Close()
		_ = db.Close()
		destroyDB(db)
	}()

	// 预先插入测试数据
	keys, values := makeData(b.N, 64)
	for i := 0; i < b.N; i++ {
		if err := cli.Put(keys[i], values[i]); err != nil {
			b.Fatalf("Pre-put error: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := cli.Get(keys[i]); err != nil {
			b.Fatalf("Get error: %v", err)
		}
	}
}

// 基准测试Delete操作
func BenchmarkDelete(b *testing.B) {
	db, cli := fastOpen()
	defer func() {
		_ = cli.Close()
		_ = db.Close()
		destroyDB(db)
	}()

	// 预先插入测试数据
	keys, values := makeData(b.N, 64)
	for i := 0; i < b.N; i++ {
		if err := cli.Put(keys[i], values[i]); err != nil {
			b.Fatalf("Pre-put error: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := cli.Delete(keys[i]); err != nil {
			b.Fatalf("Delete error: %v", err)
		}
	}
}

// 基准测试PutIfNotExists操作
func BenchmarkPutIfNotExists(b *testing.B) {
	db, cli := fastOpen()
	defer func() {
		_ = cli.Close()
		_ = db.Close()
		destroyDB(db)
	}()

	// 预生成测试数据
	keys, values := makeData(b.N, 64)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := cli.Put(keys[i], values[i], client.WithPutCondition(PUT_IF_NOT_EXISTS)); err != nil {
			b.Fatalf("PutIfNotExists error: %v", err)
		}
	}
}

// 基准测试PutIfExists操作
func BenchmarkPutIfExists(b *testing.B) {
	db, cli := fastOpen()
	defer func() {
		_ = cli.Close()
		_ = db.Close()
		destroyDB(db)
	}()

	// 预先插入测试数据
	keys, values := makeData(b.N, 64)
	for i := 0; i < b.N; i++ {
		if err := cli.Put(keys[i], values[i]); err != nil {
			b.Fatalf("Pre-put error: %v", err)
		}
	}
	_, newValues := makeData(b.N, 64)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := cli.Put(keys[i], newValues[i], client.WithPutCondition(PUT_IF_EXISTS)); err != nil {
			b.Fatalf("PutIfExists error: %v", err)
		}
	}
}

// 基准测试PutAndGet操作
func BenchmarkPutAndGet(b *testing.B) {
	db, cli := fastOpen()
	defer func() {
		_ = cli.Close()
		_ = db.Close()
		destroyDB(db)
	}()

	// 预生成测试数据
	keys, values := makeData(b.N, 64)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := cli.PutAndGet(keys[i], values[i]); err != nil {
			b.Fatalf("PutAndGet error: %v", err)
		}
	}
}

// 基准测试UpdateTTL操作
func BenchmarkUpdateTTL(b *testing.B) {
	db, cli := fastOpen()
	defer func() {
		_ = cli.Close()
		_ = db.Close()
		destroyDB(db)
	}()

	// 预先插入测试数据
	keys, values := makeData(b.N, 64)
	for i := 0; i < b.N; i++ {
		if err := cli.Put(keys[i], values[i]); err != nil {
			b.Fatalf("Pre-put error: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := cli.UpdateTTL(keys[i], 3600); err != nil {
			b.Fatalf("UpdateTTL error: %v", err)
		}
	}
}

// 基准测试BatchPut操作
func BenchmarkBatchPut(b *testing.B) {
	db, cli := fastOpen()
	defer func() {
		_ = cli.Close()
		_ = db.Close()
		destroyDB(db)
	}()

	// 批量大小
	batchSize := 10000
	totalBatches := b.N / batchSize
	if totalBatches == 0 {
		totalBatches = 1
	}

	// 预生成测试数据
	allKeys, allValues := makeData(batchSize*totalBatches, 64)
	var (
		start  int
		end    int
		keys   [][]byte
		values [][]byte
		err    error
	)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < totalBatches; i++ {
		start = i * batchSize
		end = start + batchSize
		keys = allKeys[start:end]
		values = allValues[start:end]
		if err = cli.BatchPut(keys, values); err != nil {
			b.Fatalf("BatchPut error: %v", err)
		}
	}
}

// 基准测试BatchGet操作
func BenchmarkBatchGet(b *testing.B) {
	db, cli := fastOpen()
	defer func() {
		_ = cli.Close()
		_ = db.Close()
		destroyDB(db)
	}()

	// 批量大小
	batchSize := 10000
	totalBatches := b.N / batchSize
	if totalBatches == 0 {
		totalBatches = 1
	}

	// 预生成测试数据
	allKeys, allValues := makeData(batchSize*totalBatches, 64)
	// 预先插入测试数据
	for i := 0; i < totalBatches*batchSize; i++ {
		if err := cli.Put(allKeys[i], allValues[i]); err != nil {
			b.Fatalf("Pre-put error: %v", err)
		}
	}

	var (
		start int
		end   int
		keys  [][]byte
		err   error
	)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < totalBatches; i++ {
		start = i * batchSize
		end = start + batchSize
		keys = allKeys[start:end]
		for _, key := range keys {
			if _, err = cli.Get(key); err != nil {
				b.Fatalf("BatchGet error: %v", err)
			}
		}
	}
}

// 基准测试BatchDelete操作
func BenchmarkBatchDelete(b *testing.B) {
	db, cli := fastOpen()
	defer func() {
		_ = cli.Close()
		_ = db.Close()
		destroyDB(db)
	}()

	// 批量大小
	batchSize := 10000
	totalBatches := b.N / batchSize
	if totalBatches == 0 {
		totalBatches = 1
	}

	// 预生成测试数据
	allKeys, allValues := makeData(batchSize*totalBatches, 64)
	// 预先插入测试数据
	for i := 0; i < totalBatches*batchSize; i++ {
		if err := cli.Put(allKeys[i], allValues[i]); err != nil {
			b.Fatalf("Pre-put error: %v", err)
		}
	}

	var (
		start int
		end   int
		keys  [][]byte
		err   error
	)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < totalBatches; i++ {
		start = i * batchSize
		end = start + batchSize
		keys = allKeys[start:end]
		if err = cli.BatchDelete(keys); err != nil {
			b.Fatalf("BatchDelete error: %v", err)
		}
	}
}
