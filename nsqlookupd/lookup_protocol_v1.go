package nsqlookupd

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/version"
)

// loopUpProtocol是V1
type LookupProtocolV1 struct {
	ctx *Context
}

// 读取并处理客户端发过来的命令
func (p *LookupProtocolV1) IOLoop(conn net.Conn) error {
	var err error
	var line string
	// 创建一个V1版本的客户端
	client := NewClientV1(conn)
	reader := bufio.NewReader(client)
	for {
		// 读取一行数据
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}
		// 移除两端多余空格
		line = strings.TrimSpace(line)
		params := strings.Split(line, " ")

		var response []byte
		// 执行命令，这里只能够接受4个命令PING IDENTIFY REGISTER UNREGISTER
		response, err = p.Exec(client, reader, params)
		if err != nil {
			ctx := ""
			if parentErr := err.(protocol.ChildErr).Parent(); parentErr != nil {
				ctx = " - " + parentErr.Error()
			}
			p.ctx.nsqlookupd.logf(LOG_ERROR, "[%s] - %s%s", client, err, ctx)
			// 发送错误返回值
			_, sendErr := protocol.SendResponse(client, []byte(err.Error()))
			if sendErr != nil {
				p.ctx.nsqlookupd.logf(LOG_ERROR, "[%s] - %s%s", client, sendErr, ctx)
				break
			}
			// errors of type FatalClientErr should forceably close the connection
			if _, ok := err.(*protocol.FatalClientErr); ok {
				break
			}
			continue
		}
		// 发送返回值
		if response != nil {
			_, err = protocol.SendResponse(client, response)
			if err != nil {
				break
			}
		}
	}
	// 客户端连接断开
	conn.Close()
	p.ctx.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): closing", client)
	if client.peerInfo != nil {
		registrations := p.ctx.nsqlookupd.DB.LookupRegistrations(client.peerInfo.id)
		for _, r := range registrations {
			if removed, _ := p.ctx.nsqlookupd.DB.RemoveProducer(r, client.peerInfo.id); removed {
				p.ctx.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s", client, r.Category, r.Key, r.SubKey)
			}
		}
	}
	return err
}

func (p *LookupProtocolV1) Exec(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	switch params[0] {
	case "PING":
		return p.PING(client, params)
	case "IDENTIFY":
		return p.IDENTIFY(client, reader, params[1:])
	case "REGISTER":
		return p.REGISTER(client, reader, params[1:])
	case "UNREGISTER":
		return p.UNREGISTER(client, reader, params[1:])
	}
	return nil, protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

func getTopicChan(command string, params []string) (string, string, error) {
	if len(params) == 0 {
		return "", "", protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("%s insufficient number of params", command))
	}

	topicName := params[0]
	var channelName string
	if len(params) >= 2 {
		channelName = params[1]
	}

	if !protocol.IsValidTopicName(topicName) {
		return "", "", protocol.NewFatalClientErr(nil, "E_BAD_TOPIC", fmt.Sprintf("%s topic name '%s' is not valid", command, topicName))
	}

	if channelName != "" && !protocol.IsValidChannelName(channelName) {
		return "", "", protocol.NewFatalClientErr(nil, "E_BAD_CHANNEL", fmt.Sprintf("%s channel name '%s' is not valid", command, channelName))
	}

	return topicName, channelName, nil
}

// 注册channel或者topic
func (p *LookupProtocolV1) REGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	// 首先看是否经过了认证
	if client.peerInfo == nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "client must IDENTIFY")
	}
	// 获取topic channel
	topic, channel, err := getTopicChan("REGISTER", params)
	if err != nil {
		return nil, err
	}
	if channel != "" {
		// key是 category, key和subKey，然后value是 NSQD相关信息，这里表示哪个channel是哪个NSQD在维护
		key := Registration{"channel", topic, channel}
		if p.ctx.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) {
			p.ctx.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s", client, "channel", topic, channel)
		}
	}
	// key是 category, key和subKey，然后value是 NSQD相关信息
	// 注册过channel之后，还会再注册一个topic，这里表明topic是哪个NSQD在维护
	key := Registration{"topic", topic, ""}
	if p.ctx.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) {
		p.ctx.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s", client, "topic", topic, "")
	}
	return []byte("OK"), nil
}

// 注销channel或者topic
func (p *LookupProtocolV1) UNREGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "client must IDENTIFY")
	}
	topic, channel, err := getTopicChan("UNREGISTER", params)
	if err != nil {
		return nil, err
	}
	if channel != "" {
		key := Registration{"channel", topic, channel}
		removed, left := p.ctx.nsqlookupd.DB.RemoveProducer(key, client.peerInfo.id)
		if removed {
			p.ctx.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s", client, "channel", topic, channel)
		}
		// for ephemeral channels, remove the channel as well if it has no producers
		if left == 0 && strings.HasSuffix(channel, "#ephemeral") {
			p.ctx.nsqlookupd.DB.RemoveRegistration(key)
		}
	} else {
		// no channel was specified so this is a topic unregistration
		// remove all of the channel registrations...
		// normally this shouldn't happen which is why we print a warning message
		// if anything is actually removed
		// 如果没有指定channel，那么就取消注册Topic以及下面所有的channel
		registrations := p.ctx.nsqlookupd.DB.FindRegistrations("channel", topic, "*")
		for _, r := range registrations {
			removed, _ := p.ctx.nsqlookupd.DB.RemoveProducer(r, client.peerInfo.id)
			if removed {
				p.ctx.nsqlookupd.logf(LOG_WARN, "client(%s) unexpected UNREGISTER category:%s key:%s subkey:%s",
					client, "channel", topic, r.SubKey)
			}
		}

		key := Registration{"topic", topic, ""}
		removed, left := p.ctx.nsqlookupd.DB.RemoveProducer(key, client.peerInfo.id)
		if removed {
			p.ctx.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
				client, "topic", topic, "")
		}
		if left == 0 && strings.HasSuffix(topic, "#ephemeral") {
			p.ctx.nsqlookupd.DB.RemoveRegistration(key)
		}
	}
	return []byte("OK"), nil
}

// 可以理解为登录
func (p *LookupProtocolV1) IDENTIFY(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	var err error
	// 判断是否IDENTIFY过
	if client.peerInfo != nil {
		return nil, protocol.NewFatalClientErr(err, "E_INVALID", "cannot IDENTIFY again")
	}
	// 读取消息的Body
	var bodyLen int32
	err = binary.Read(reader, binary.BigEndian, &bodyLen)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}
	body := make([]byte, bodyLen)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}
	// body is a json structure with producer information
	// NSQD IP是 clientID
	peerInfo := PeerInfo{id: client.RemoteAddr().String()}
	err = json.Unmarshal(body, &peerInfo)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
	}

	peerInfo.RemoteAddress = client.RemoteAddr().String()

	// require all fields
	if peerInfo.BroadcastAddress == "" || peerInfo.TCPPort == 0 || peerInfo.HTTPPort == 0 || peerInfo.Version == "" {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY", "IDENTIFY missing fields")
	}

	atomic.StoreInt64(&peerInfo.lastUpdate, time.Now().UnixNano())

	p.ctx.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): IDENTIFY Address:%s TCP:%d HTTP:%d Version:%s",
		client, peerInfo.BroadcastAddress, peerInfo.TCPPort, peerInfo.HTTPPort, peerInfo.Version)

	client.peerInfo = &peerInfo
	if p.ctx.nsqlookupd.DB.AddProducer(Registration{"client", "", ""}, &Producer{peerInfo: client.peerInfo}) {
		p.ctx.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s", client, "client", "", "")
	}

	// build a response 构建response
	data := make(map[string]interface{})
	data["tcp_port"] = p.ctx.nsqlookupd.RealTCPAddr().Port
	data["http_port"] = p.ctx.nsqlookupd.RealHTTPAddr().Port
	data["version"] = version.Binary
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %s", err)
	}
	data["broadcast_address"] = p.ctx.nsqlookupd.opts.BroadcastAddress
	data["hostname"] = hostname

	response, err := json.Marshal(data)
	if err != nil {
		p.ctx.nsqlookupd.logf(LOG_ERROR, "marshaling %v", data)
		return []byte("OK"), nil
	}
	return response, nil
}

// 回复PING
func (p *LookupProtocolV1) PING(client *ClientV1, params []string) ([]byte, error) {
	// peerInfo这个是在Identify命令中被赋值的
	if client.peerInfo != nil {
		// we could get a PING before other commands on the same client connection
		cur := time.Unix(0, atomic.LoadInt64(&client.peerInfo.lastUpdate))
		now := time.Now()
		p.ctx.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): pinged (last ping %s)", client.peerInfo.id, now.Sub(cur))
		// 更新lastUpdate时间
		atomic.StoreInt64(&client.peerInfo.lastUpdate, now.UnixNano())
	}
	return []byte("OK"), nil
}
