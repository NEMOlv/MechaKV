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

import (
	. "MechaKV/comment"
	"os"
)

// FileIO FileIO标准系统文件IO
type FileIO struct {
	// 系统文件描述符
	fd *os.File
	//w  io.Writer
}

// NewFileIOManager 初始化标准文件
func NewFileIOManager(fileName string) (*FileIO, error) {
	fd, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		DataFilePermision,
	)

	if err != nil {
		return nil, err
	}

	return &FileIO{fd: fd}, nil
}

// Read 读操作:将数据从文件读到content变量中
func (fio *FileIO) Read(content []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(content, offset)
}

// Write 写操作:将content变量中的内容写到文件中
func (fio *FileIO) Write(content []byte) (int, error) {
	return fio.fd.Write(content)
}

// Sync 数据持久化
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

// Close 关闭文件
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}

// Size 获取文件大小
func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
