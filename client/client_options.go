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

package client

import (
	. "MechaKV/comment"
)

// 定义接口：所有需要支持"设置桶名"的类型，都需实现该方法
type BucketNameSetter interface {
	SetBucketName(name string)
}

// PutOptions Put操作配置项
type CliOptions struct {
	// 生命周期
	ttl uint32
	// 时间戳
	timestamp uint64
	// put类型
	condition PutCondition
	// 桶名
	bucketName string
}

func (opt *CliOptions) SetBucketName(bucketName string) {
	opt.bucketName = bucketName
}

var DefaultCliOptions = CliOptions{
	ttl:        PERSISTENT, // 默认为"永久"值
	condition:  PUT_NORMAL, // 默认为正常PUT
	bucketName: "default",
}

// WithTTL 配置生命周期
func WithTTL(ttl uint32) func(*CliOptions) {
	return func(opt *CliOptions) {
		opt.ttl = ttl
	}
}

// WithTimestamp 配置时间戳
func WithTimestamp(timestamp uint64) func(*CliOptions) {
	return func(opt *CliOptions) {
		opt.timestamp = timestamp
	}
}

// WithPutCondition 配置Put类型
func WithPutCondition(condition PutCondition) func(*CliOptions) {
	return func(opt *CliOptions) {
		opt.condition = condition
	}
}

// WithBucketName 配置桶名
func WithBucketName(bucketName string) func(*CliOptions) {
	return func(opt *CliOptions) {
		opt.bucketName = bucketName
	}
}
