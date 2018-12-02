package util

import (
	"math/rand"
)

// quantity <= maxval
func UniqRands(quantity int, maxval int) []int {
	if maxval < quantity {
		quantity = maxval
	}
	// 初始化index slice
	intSlice := make([]int, maxval)
	for i := 0; i < maxval; i++ {
		intSlice[i] = i
	}

	for i := 0; i < quantity; i++ {
		// 把intSlice[i]和后面任意一个元素进行交换位置来达到乱序的目的
		j := rand.Int()%maxval + i
		// swap
		intSlice[i], intSlice[j] = intSlice[j], intSlice[i]
		maxval--
	}
	return intSlice[0:quantity]
}
