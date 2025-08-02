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

package index

// 通用索引迭代器
type IndexIterator interface {
	// Rwind 重新回到迭代器的起点
	Rewind()
	// Seek 根据传入的key查找到第一个大于（或小于）等于的目标key，从这个key开始遍历
	Seek(key []byte)
	// Next 跳转到下一个 key
	Next()
	// Valid 是否有效，即是否已经遍历完了所有的key，用于退出遍历
	Valid() bool
	// Key 当前遍历位置的Key数据
	Key() []byte
	// Value 当前遍历位置的Value数据
	Value() *KvPairPos
	// Close 关闭迭代器，释放相应资源
	Close()
	Iterate() ([]byte, *KvPairPos, bool)
}
