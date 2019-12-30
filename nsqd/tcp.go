package nsqd

import (
	"io"
	"net"
	"sync"

	"github.com/nsqio/nsq/internal/protocol"
)

// tcp server
type tcpServer struct {
	ctx   *context
	conns sync.Map
}

// 处理tcp请求
func (p *tcpServer) Handle(clientConn net.Conn) {
	// 记录有一个client链接进来了
	p.ctx.nsqd.logf(LOG_INFO, "TCP: new client(%s)", clientConn.RemoteAddr())

	// The client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	// 首先会读取4bytes的version信息
	buf := make([]byte, 4)
	_, err := io.ReadFull(clientConn, buf)
	if err != nil {
		p.ctx.nsqd.logf(LOG_ERROR, "failed to read protocol version - %s", err)
		clientConn.Close()
		return
	}
	protocolMagic := string(buf)

	p.ctx.nsqd.logf(LOG_INFO, "CLIENT(%s): desired protocol magic '%s'", clientConn.RemoteAddr(), protocolMagic)

	var prot protocol.Protocol
	switch protocolMagic {
	case "  V2":
		// V2版本 TODO @limingji disk-queue可以参考这个格式来写 👍这是一个不错的思路；毕竟你nsq都是这么写的，为啥我go-diskqueue不能这么写
		prot = &protocolV2{ctx: p.ctx}
	default:
		// 在这里会给客户端返回一个错误，说这个版本不对
		protocol.SendFramedResponse(clientConn, frameTypeError, []byte("E_BAD_PROTOCOL"))
		clientConn.Close()
		p.ctx.nsqd.logf(LOG_ERROR, "client(%s) bad protocol magic '%s'", clientConn.RemoteAddr(), protocolMagic)
		return
	}
	// 存储remoteAddr -> clientConn
	p.conns.Store(clientConn.RemoteAddr(), clientConn)
	// 循环处理来此这个Client的各种请求
	err = prot.IOLoop(clientConn)
	if err != nil {
		p.ctx.nsqd.logf(LOG_ERROR, "client(%s) - %s", clientConn.RemoteAddr(), err)
	}
	// 链接断开的时候删除Client
	p.conns.Delete(clientConn.RemoteAddr())
}

// 关闭这个Tcp链接下所有的Client
func (p *tcpServer) CloseAll() {
	p.conns.Range(func(k, v interface{}) bool {
		v.(net.Conn).Close()
		return true
	})
}
