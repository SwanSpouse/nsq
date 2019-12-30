package nsqd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

const (
	MsgIDLength       = 16
	minValidMsgLength = MsgIDLength + 8 + 2 // Timestamp + Attempts
)

type MessageID [MsgIDLength]byte

type Message struct {
	ID        MessageID // messageID
	Body      []byte    // 消息体
	Timestamp int64     // 时间戳
	Attempts  uint16

	// for in-flight handling
	deliveryTS time.Time     // 发送操作时间
	clientID   int64         // client id
	index      int           // 消息在优先级队列中的位置，在push到优先级队里的时候会进行赋值
	pri        int64         // 应该发送消息的时间
	deferred   time.Duration // 发送消息的延迟
}

// New一个Message出来
func NewMessage(id MessageID, body []byte) *Message {
	return &Message{
		ID:        id,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
}

// 将message写入Writer中
func (m *Message) WriteTo(w io.Writer) (int64, error) {
	var buf [10]byte
	var total int64
	// 8bytes的时间戳
	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	// 2bytes的尝试次数
	binary.BigEndian.PutUint16(buf[8:10], uint16(m.Attempts))
	// 写入时间戳和尝试次数 TODO limingji 是不是应该先拼接好所有的bytes数组，然后一波写进去
	n, err := w.Write(buf[:])
	total += int64(n)
	if err != nil {
		return total, err
	}
	// 写入ID
	n, err = w.Write(m.ID[:])
	total += int64(n)
	if err != nil {
		return total, err
	}
	// 写入Body
	n, err = w.Write(m.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}
	// 返回总写入的bytes数
	return total, nil
}

// decodeMessage deserializes data (as []byte) and creates a new Message
// message format:
// [x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
// |       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)
// |       8-byte         ||    ||                 16-byte                      || N-byte
// ------------------------------------------------------------------------------------------...
//   nanosecond timestamp    ^^                   message ID                       message body
//                        (uint16)
//                         2-byte
//                        attempts
func decodeMessage(b []byte) (*Message, error) {
	var msg Message
	// 非法消息格式
	if len(b) < minValidMsgLength {
		return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
	}
	// 获取时间戳
	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	// 获取尝试次数
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])
	// 消息id
	copy(msg.ID[:], b[10:10+MsgIDLength])
	// 消息体
	msg.Body = b[10+MsgIDLength:]
	// 返回解析好的message
	return &msg, nil
}

// 将消息写入后台队列；可能是内存队列；
func writeMessageToBackend(buf *bytes.Buffer, msg *Message, bq BackendQueue) error {
	// 重置缓冲区
	buf.Reset()
	// 写入buf
	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}
	// 把数据写入队列中
	return bq.Put(buf.Bytes())
}
