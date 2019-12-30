package nsqd

import (
	"bytes"
	"sync"
)

// 内存缓存池
var bp sync.Pool

// 在init的时候构造
func init() {
	bp.New = func() interface{} {
		return &bytes.Buffer{}
	}
}

// 从缓存池中Get一个方法
func bufferPoolGet() *bytes.Buffer {
	return bp.Get().(*bytes.Buffer)
}

// 向缓存池中Put一个方法
func bufferPoolPut(b *bytes.Buffer) {
	bp.Put(b)
}
