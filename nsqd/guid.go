package nsqd

// the core algorithm here was borrowed from:
// Blake Mizerany's `noeqd` https://github.com/bmizerany/noeqd
// and indirectly:
// Twitter's `snowflake` https://github.com/twitter/snowflake

// only minor cleanup and changes to introduce a type, combine the concept
// of workerID + datacenterId into a single identifier, and modify the
// behavior when sequences rollover for our specific implementation needs

import (
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

// 这一波bits操作就看不懂了
const (
	nodeIDBits     = uint64(10)
	sequenceBits   = uint64(12)
	nodeIDShift    = sequenceBits
	timestampShift = sequenceBits + nodeIDBits
	sequenceMask   = int64(-1) ^ (int64(-1) << sequenceBits)

	// ( 2012-10-28 16:23:42 UTC ).UnixNano() >> 20
	twepoch = int64(1288834974288)
)

var ErrTimeBackwards = errors.New("time has gone backwards")
var ErrSequenceExpired = errors.New("sequence expired")
var ErrIDBackwards = errors.New("ID went backward")

type guid int64

type guidFactory struct {
	sync.Mutex

	nodeID        int64 // nodeId
	sequence      int64 //
	lastTimestamp int64 // 时间戳
	lastID        guid  // 上一次生成的id
}

// New一个工厂类；用于生成Guid
func NewGUIDFactory(nodeID int64) *guidFactory {
	return &guidFactory{
		nodeID: nodeID,
	}
}

// 生成全局ID
func (f *guidFactory) NewGUID() (guid, error) {
	// TODO @limingji 这里加锁是否影响了消息ID生成的效率
	// TODO @limingji 我发现这个项目里面的好多Lock都没有在defer里面进行unlock
	f.Lock()
	// divide by 2^20=1048576, giving pseudo-milliseconds
	// 根据时间来生成GUID
	ts := time.Now().UnixNano() >> 20

	if ts < f.lastTimestamp {
		f.Unlock()
		return 0, ErrTimeBackwards
	}
	if f.lastTimestamp == ts {
		f.sequence = (f.sequence + 1) & sequenceMask
		if f.sequence == 0 {
			f.Unlock()
			return 0, ErrSequenceExpired
		}
	} else {
		f.sequence = 0
	}

	f.lastTimestamp = ts
	id := guid(((ts - twepoch) << timestampShift) |
		(f.nodeID << nodeIDShift) |
		f.sequence)

	if id <= f.lastID {
		f.Unlock()
		return 0, ErrIDBackwards
	}

	f.lastID = id
	f.Unlock()
	return id, nil
}

func (g guid) Hex() MessageID {
	var h MessageID
	var b [8]byte

	b[0] = byte(g >> 56)
	b[1] = byte(g >> 48)
	b[2] = byte(g >> 40)
	b[3] = byte(g >> 32)
	b[4] = byte(g >> 24)
	b[5] = byte(g >> 16)
	b[6] = byte(g >> 8)
	b[7] = byte(g)

	hex.Encode(h[:], b[:])
	return h
}
