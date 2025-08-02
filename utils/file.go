package utils

import (
	"golang.org/x/sys/windows"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// GetDataSize 获取一个目录的大小
func GetDiskSize(dirPath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// AvailableDiskSize 获取磁盘剩余可用空间大小（Windows版本）
func GetAvailableDiskSize() (uint64, error) {
	wd, err := os.Getwd()
	if err != nil {
		return 0, err
	}

	// 获取当前目录所在的卷根路径（如：C:\）
	rootPath := filepath.VolumeName(wd) + `\`

	var freeBytes, totalBytes, totalFreeBytes uint64
	err = windows.GetDiskFreeSpaceEx(
		windows.StringToUTF16Ptr(rootPath),
		&freeBytes,
		&totalBytes,
		&totalFreeBytes,
	)

	if err != nil {
		return 0, err
	}
	return freeBytes, nil
}

// CopyData 拷贝数据目录
func CopyData(src, dest string, exclude []string) error {
	// 目标目标不存在则创建
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err := os.MkdirAll(dest, os.ModePerm); err != nil {
			return err
		}
	}

	return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		//fileName := strings.Replace(path, src, "", 1)
		fileNames := strings.Split(path, "\\")
		fileName := fileNames[len(fileNames)-1]
		if fileName == "" {
			return nil
		}

		for _, e := range exclude {
			matched, err := filepath.Match(e, info.Name())
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}

		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dest, fileName), info.Mode())
		}

		data, err := os.ReadFile(filepath.Join(src, fileName))
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dest, fileName), data, info.Mode())
	})
}
