package utils

import (
	"fmt"
	"math/rand"
	"time"
)

func RandomK(size, k int) ([]int, error) {
	// 参数校验
	if k <= 0 {
		return nil, fmt.Errorf("n必须大于0")
	}

	if size < 0 {
		return nil, fmt.Errorf("size不能为负数")
	}

	if k > size+1 {
		return nil, fmt.Errorf("n不能大于size+1（最大可选数量）")
	}

	// 初始化随机数生成器，使用当前时间作为种子
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	result := make([]int, 0, k)
	if size-k > 100 {
		selected := make(map[int]struct{})
		for len(result) < k {
			// 生成0到size之间的随机数（包含边界）
			num := r.Intn(size)
			// 检查是否已选择过
			if _, exists := selected[num]; !exists {
				selected[num] = struct{}{}
				result = append(result, num)
			}
		}
	} else {
		// 先生成有序切片，再打乱顺序，最后取前n个
		nums := make([]int, size)
		for i := range nums {
			nums[i] = i
		}
		r.Shuffle(len(nums), func(i, j int) {
			nums[i], nums[j] = nums[j], nums[i]
		})
		result = nums[:k]
	}

	return result, nil

}
