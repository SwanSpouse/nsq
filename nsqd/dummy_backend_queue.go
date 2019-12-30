package nsqd

// 假的后台队列 这个队列的所有实现都是空实现
type dummyBackendQueue struct {
	readChan chan []byte
}

// 创建一个伪后台队列
func newDummyBackendQueue() BackendQueue {
	return &dummyBackendQueue{readChan: make(chan []byte)}
}

// 向后台队列中放入一条消息
func (d *dummyBackendQueue) Put([]byte) error {
	return nil
}

// 获取读取消息的chan
func (d *dummyBackendQueue) ReadChan() <-chan []byte {
	return d.readChan
}

// 关闭后台队列
func (d *dummyBackendQueue) Close() error {
	return nil
}

// 退出
func (d *dummyBackendQueue) Delete() error {
	return nil
}

// 查看队列中未读的消息条数
func (d *dummyBackendQueue) Depth() int64 {
	return int64(0)
}

// 清空队列
func (d *dummyBackendQueue) Empty() error {
	return nil
}
