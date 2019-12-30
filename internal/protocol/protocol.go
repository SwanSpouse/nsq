package protocol

import (
	"encoding/binary"
	"io"
	"net"
)

// Protocol describes the basic behavior of any protocol in the system
type Protocol interface {
	IOLoop(conn net.Conn) error
}

// SendResponse is a server side utility function to prefix data with a length header
// and write to the supplied Writer
// 发送返回值
func SendResponse(w io.Writer, data []byte) (int, error) {
	// 首先写入数据长度
	err := binary.Write(w, binary.BigEndian, int32(len(data)))
	if err != nil {
		return 0, err
	}
	// 再写入数据
	n, err := w.Write(data)
	if err != nil {
		return 0, err
	}
	// n是数据长度、4是消息长度int32占8bytes
	return (n + 4), nil
}

// SendFramedResponse is a server side utility function to prefix data with a length header
// and frame header and write to the supplied Writer
func SendFramedResponse(w io.Writer, frameType int32, data []byte) (int, error) {
	beBuf := make([]byte, 4)
	size := uint32(len(data)) + 4
	// 写入size
	binary.BigEndian.PutUint32(beBuf, size)
	n, err := w.Write(beBuf)
	if err != nil {
		return n, err
	}
	// 写入fameType
	binary.BigEndian.PutUint32(beBuf, uint32(frameType))
	n, err = w.Write(beBuf)
	if err != nil {
		return n + 4, err
	}
	// 写入数据
	n, err = w.Write(data)
	// n是数据长度 8 = 4 + 4 data size + frameType
	return n + 8, err
}
