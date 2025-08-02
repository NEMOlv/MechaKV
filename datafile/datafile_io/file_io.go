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

package datafile_io

// IOmanager 抽象IO管理接口，可以接入不同的IO类型
// 目前支持标准文件IO
type IOManager interface {
	// Read 从文件给定位置读取对应数据
	Read([]byte, int64) (int, error)
	// Write 写入字节数组到文件中
	Write([]byte) (int, error)
	// 持久化数据到硬盘
	Sync() error
	// 关闭文件
	Close() error
	// 获取文件大小
	Size() (int64, error)
}

// NewIOManager 初始化 IOManager
func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
