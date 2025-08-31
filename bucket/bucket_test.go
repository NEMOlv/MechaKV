package bucket

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func Test_NewBucketManager(t *testing.T) {
	// 创建并事后删除临时目录
	dir, err := os.MkdirTemp("../temp", "test_bucket")
	assert.Nil(t, err)
	defer func(path string) {
		err := os.RemoveAll(path)
		assert.Nil(t, err)
	}(dir)

	bucketManager, err := NewBucketManager(dir)
	assert.Nil(t, err)
	assert.NotNil(t, bucketManager)
	assert.NotNil(t, bucketManager.BucketFile)
	assert.NotNil(t, bucketManager.BucketIdGenerater)

	err = bucketManager.BucketFile.Close()
	assert.Nil(t, err)
}

func Test_GenerateBucketID(t *testing.T) {
	// 创建并事后删除临时目录
	dir, err := os.MkdirTemp("../temp", "test_bucket")
	assert.Nil(t, err)
	defer func(path string) {
		err := os.RemoveAll(path)
		assert.Nil(t, err)
	}(dir)

	bucketManager, err := NewBucketManager(dir)
	assert.Nil(t, err)
	bucketId := bucketManager.GenerateBucketID()
	assert.NotNil(t, bucketId)

	err = bucketManager.BucketFile.Close()
	assert.Nil(t, err)
}
