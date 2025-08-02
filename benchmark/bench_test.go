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

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		genkey := utils.GenerateKey(i)
		genValue := utils.GenerateValue(128)
		err := cli.Put(genkey, genValue)
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
	keys := make([][]byte, b.N)
	values := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = utils.GenerateKey(i)
		values[i] = utils.GenerateValue(128)
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
	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = utils.GenerateKey(i)
		if err := cli.Put(keys[i], []byte("value")); err != nil {
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
	keys := make([][]byte, b.N)
	values := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = utils.GenerateKey(i)
		values[i] = utils.GenerateValue(128)
	}

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
	keys := make([][]byte, b.N)
	values := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = utils.GenerateKey(i)
		values[i] = utils.GenerateValue(128)
		if err := cli.Put(keys[i], values[i]); err != nil {
			b.Fatalf("Pre-put error: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		newValue := utils.GenerateValue(128)
		if err := cli.Put(keys[i], newValue, client.WithPutCondition(PUT_IF_EXISTS)); err != nil {
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
	keys := make([][]byte, b.N)
	values := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = utils.GenerateKey(i)
		values[i] = utils.GenerateValue(128)
	}

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
	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = utils.GenerateKey(i)
		if err := cli.Put(keys[i], []byte("value")); err != nil {
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
	batchSize := 100
	totalBatches := b.N / batchSize
	if totalBatches == 0 {
		totalBatches = 1
	}

	// 预生成测试数据
	allKeys := make([][]byte, totalBatches*batchSize)
	allValues := make([][]byte, totalBatches*batchSize)
	for i := 0; i < totalBatches*batchSize; i++ {
		allKeys[i] = utils.GenerateKey(i)
		allValues[i] = utils.GenerateValue(128)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < totalBatches; i++ {
		start := i * batchSize
		end := start + batchSize
		keys := allKeys[start:end]
		values := allValues[start:end]
		if err := cli.BatchPut(keys, values); err != nil {
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
	batchSize := 100
	totalBatches := b.N / batchSize
	if totalBatches == 0 {
		totalBatches = 1
	}

	// 预先插入测试数据
	allKeys := make([][]byte, totalBatches*batchSize)
	for i := 0; i < totalBatches*batchSize; i++ {
		allKeys[i] = utils.GenerateKey(i)
		if err := cli.Put(allKeys[i], []byte("value")); err != nil {
			b.Fatalf("Pre-put error: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < totalBatches; i++ {
		start := i * batchSize
		end := start + batchSize
		keys := allKeys[start:end]
		for _, key := range keys {
			if _, err := cli.Get(key); err != nil {
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
	batchSize := 100
	totalBatches := b.N / batchSize
	if totalBatches == 0 {
		totalBatches = 1
	}

	// 预先插入测试数据
	allKeys := make([][]byte, totalBatches*batchSize)
	for i := 0; i < totalBatches*batchSize; i++ {
		allKeys[i] = utils.GenerateKey(i)
		if err := cli.Put(allKeys[i], []byte("value")); err != nil {
			b.Fatalf("Pre-put error: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < totalBatches; i++ {
		start := i * batchSize
		end := start + batchSize
		keys := allKeys[start:end]
		if err := cli.BatchDelete(keys); err != nil {
			b.Fatalf("BatchDelete error: %v", err)
		}
	}
}

// 基准测试带TTL的Put操作
func BenchmarkPutWithTTL(b *testing.B) {
	db, cli := fastOpen()
	defer func() {
		_ = cli.Close()
		_ = db.Close()
		destroyDB(db)
	}()

	// 预生成测试数据
	keys := make([][]byte, b.N)
	values := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = utils.GenerateKey(i)
		values[i] = utils.GenerateValue(128)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := cli.Put(keys[i], values[i], client.WithTTL(3600)); err != nil {
			b.Fatalf("PutWithTTL error: %v", err)
		}
	}
}
