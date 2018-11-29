package nsqd

// 这是一个小根堆的实现
type inFlightPqueue []*Message

func newInFlightPqueue(capacity int) inFlightPqueue {
	return make(inFlightPqueue, 0, capacity)
}

// 交换两个元素的位置
func (pq inFlightPqueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// 向Queue中添加元素
func (pq *inFlightPqueue) Push(x *Message) {
	n := len(*pq)
	c := cap(*pq)
	// 当前容量+1超过cap的时候，会进行扩容，
	// 这里扩容扩的是cap，不是length
	if n+1 > c {
		npq := make(inFlightPqueue, n, c*2)
		copy(npq, *pq)
		*pq = npq
	}
	// 把length的指针向右移动一位，扩充一个空白元素用来填充x
	*pq = (*pq)[0 : n+1]
	x.index = n
	// 把x放入到队列的末尾
	(*pq)[n] = x
	// 进行一次堆调整
	pq.up(n)
}

// 弹出堆顶元素
func (pq *inFlightPqueue) Pop() *Message {
	n := len(*pq)
	c := cap(*pq)
	// 将堆顶元素和末端元素进行交换
	pq.Swap(0, n-1)
	// 进行一次向下调整
	pq.down(0, n-1)
	// 当符合条件时进行缩容
	if n < (c/2) && c > 25 {
		npq := make(inFlightPqueue, n, c/2)
		copy(npq, *pq)
		*pq = npq
	}
	// 获取末尾元素，并删除末尾元素
	x := (*pq)[n-1]
	x.index = -1
	*pq = (*pq)[0 : n-1]
	return x
}

// 移除i位置的节点
func (pq *inFlightPqueue) Remove(i int) *Message {
	n := len(*pq)
	// 若要移除的元素是最后一个，则直接移除就好了
	if n-1 != i {
		// 把最后一个元素和要移除的元素进行交换
		pq.Swap(i, n-1)
		// 进行一次向下堆调整
		pq.down(i, n-1)
		// 进行一次向上堆调整
		pq.up(i)
	}
	// 移除最后一个元素，将移除的元素返回
	x := (*pq)[n-1]
	x.index = -1
	*pq = (*pq)[0 : n-1]
	return x
}

// 如果堆顶元素大于max，则返回nil和堆顶元素与max之间的差值
// 如果堆顶元素小于max，则返回堆顶元素，并将堆顶元素删除。
func (pq *inFlightPqueue) PeekAndShift(max int64) (*Message, int64) {
	if len(*pq) == 0 {
		return nil, 0
	}
	// 取出堆顶元素
	x := (*pq)[0]
	// 若堆顶元素大于max，则return nil，并返回和max的差值
	if x.pri > max {
		return nil, x.pri - max
	}
	// 移除堆顶元素
	pq.Pop()
	// 将x返回
	return x, 0
}

// 进行一次向上堆调整
func (pq *inFlightPqueue) up(j int) {
	for {
		// 获取父节点的位置
		i := (j - 1) / 2 // parent
		if i == j || (*pq)[j].pri >= (*pq)[i].pri {
			break
		}
		// 若j位置节点的值小于父节点的值，则进行交换
		pq.Swap(i, j)
		// 继续调整
		j = i
	}
}

// 进行一次向下堆调整
func (pq *inFlightPqueue) down(i, n int) {
	for {
		// 找到i的左孩子
		j1 := 2*i + 1
		// 若超过堆的大小或溢出，则直接退出
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		// 把当前节点和自己的两个孩子作比较，若自己的值大于任意孩子的值，把自己和值较小的孩子交换位置。
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && (*pq)[j1].pri >= (*pq)[j2].pri {
			j = j2 // = 2*i + 2  // right child
		}
		if (*pq)[j].pri >= (*pq)[i].pri {
			break
		}
		pq.Swap(i, j)
		i = j
	}
}
