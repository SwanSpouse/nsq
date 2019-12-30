package nsqd

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/lg"
)

// lookupPeer is a low-level type for connecting/reading/writing to nsqlookupd
//
// A lookupPeer instance is designed to connect lazily to nsqlookupd and reconnect
// gracefully (i.e. it is all handled by the library).  Clients can simply use the
// Command interface to perform a round-trip.
// 链接NSQLookupd的一个Client
type lookupPeer struct {
	logf            lg.AppLogFunc
	addr            string            // 远端ip
	conn            net.Conn          // 链接
	state           int32             // state
	connectCallback func(*lookupPeer) // 连接成功的callback
	maxBodySize     int64             // 最大body的size
	Info            peerInfo          // peer clientInfo
}

// peerInfo contains metadata for a lookupPeer instance (and is JSON marshalable)
type peerInfo struct {
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
	BroadcastAddress string `json:"broadcast_address"`
}

// newLookupPeer creates a new lookupPeer instance connecting to the supplied address.
//
// The supplied connectCallback will be called *every* time the instance connects.
func newLookupPeer(addr string, maxBodySize int64, l lg.AppLogFunc, connectCallback func(*lookupPeer)) *lookupPeer {
	return &lookupPeer{
		logf:            l,
		addr:            addr,
		state:           stateDisconnected, // 初试状态是Disconnected
		maxBodySize:     maxBodySize,
		connectCallback: connectCallback,
	}
}

// Connect will Dial the specified address, with timeouts
func (lp *lookupPeer) Connect() error {
	// TODO @limingji Close都把修改status放在自己函数内了，Connect没有这做
	// 尝试进行连接
	lp.logf(lg.INFO, "LOOKUP connecting to %s", lp.addr)
	conn, err := net.DialTimeout("tcp", lp.addr, time.Second)
	if err != nil {
		return err
	}
	lp.conn = conn
	return nil
}

// String returns the specified address
func (lp *lookupPeer) String() string {
	return lp.addr
}

// Read implements the io.Reader interface, adding deadlines
func (lp *lookupPeer) Read(data []byte) (int, error) {
	lp.conn.SetReadDeadline(time.Now().Add(time.Second))
	return lp.conn.Read(data)
}

// Write implements the io.Writer interface, adding deadlines
func (lp *lookupPeer) Write(data []byte) (int, error) {
	lp.conn.SetWriteDeadline(time.Now().Add(time.Second))
	return lp.conn.Write(data)
}

// Close implements the io.Closer interface
func (lp *lookupPeer) Close() error {
	// 关闭后状态会变为Disconnected
	lp.state = stateDisconnected
	if lp.conn != nil {
		return lp.conn.Close()
	}
	return nil
}

// Command performs a round-trip for the specified Command.
//
// It will lazily connect to nsqlookupd and gracefully handle
// reconnecting in the event of a failure.
//
// It returns the response from nsqlookupd as []byte
func (lp *lookupPeer) Command(cmd *nsq.Command) ([]byte, error) {
	// 记录初试状态
	initialState := lp.state
	if lp.state != stateConnected {
		// 在这里尝试建立TCP连接
		err := lp.Connect()
		if err != nil {
			return nil, err
		}
		// TCP连接成功后状态变为connected
		lp.state = stateConnected
		_, err = lp.Write(nsq.MagicV1)
		if err != nil {
			lp.Close()
			return nil, err
		}
		// 如果最初的状态是stateDisconnected，则进行connectCallback
		// 在connectCallback中同步topic channel信息
		if initialState == stateDisconnected {
			lp.connectCallback(lp)
		}
		// 经过这一通操作之后如果还没有连接上，则报错。
		if lp.state != stateConnected {
			return nil, fmt.Errorf("lookupPeer connectCallback() failed")
		}
	}
	if cmd == nil {
		return nil, nil
	}
	// 将命令写入lookupd的buf中
	_, err := cmd.WriteTo(lp)
	if err != nil {
		lp.Close()
		return nil, err
	}
	// 在发送过命令之后，会读取一个返回值。
	resp, err := readResponseBounded(lp, lp.maxBodySize)
	if err != nil {
		lp.Close()
		return nil, err
	}
	// 命令返回值
	return resp, nil
}

func readResponseBounded(r io.Reader, limit int64) ([]byte, error) {
	var msgSize int32

	// message size
	err := binary.Read(r, binary.BigEndian, &msgSize)
	if err != nil {
		return nil, err
	}

	if int64(msgSize) > limit {
		return nil, fmt.Errorf("response body size (%d) is greater than limit (%d)",
			msgSize, limit)
	}

	// message binary data
	buf := make([]byte, msgSize)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}
