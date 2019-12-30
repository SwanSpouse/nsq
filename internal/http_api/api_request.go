package http_api

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// A custom http.Transport with support for deadline timeouts
// 在我看来就是一个有各种超时时间的transport
func NewDeadlineTransport(connectTimeout time.Duration, requestTimeout time.Duration) *http.Transport {
	// arbitrary values copied from http.DefaultTransport
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   connectTimeout,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		ResponseHeaderTimeout: requestTimeout,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
	}
	return transport
}

// http client结构
type Client struct {
	c *http.Client
}

// 创建一个HttpClient
func NewClient(tlsConfig *tls.Config, connectTimeout time.Duration, requestTimeout time.Duration) *Client {
	transport := NewDeadlineTransport(connectTimeout, requestTimeout)
	transport.TLSClientConfig = tlsConfig
	return &Client{
		// 构造Client
		c: &http.Client{
			Transport: transport,
			Timeout:   requestTimeout,
		},
	}
}

// GETV1 is a helper function to perform a V1 HTTP request
// and parse our NSQ daemon's expected response format, with deadlines.
func (c *Client) GETV1(endpoint string, v interface{}) error {
retry:
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return err
	}
	// 版本相关的信息都写到Header里面
	req.Header.Add("Accept", "application/vnd.nsq; version=1.0")
	// 发送Http请求
	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}
	// 读取Http返回值
	body, err := ioutil.ReadAll(resp.Body)
	// 关闭Reader
	resp.Body.Close()
	if err != nil {
		return err
	}
	// 返回值
	if resp.StatusCode != 200 {
		// 这里应该是为了兼容什么事情；
		// HTTP 403 Forbidden - 拒绝访问 如果访问被拒绝，且endpoint没有使用https；则说目有可能是因为https导致的被拒绝
		if resp.StatusCode == 403 && !strings.HasPrefix(endpoint, "https") {
			// 这样的话就会换成一个https的头再发一次请求
			endpoint, err = httpsEndpoint(endpoint, body)
			if err != nil {
				return err
			}
			// TODO @lmingji 如果httpsEndpoints里面么有给出一个https的endpoint；retry这里就容易死循环了。
			goto retry
		}
		// 在这里是没有得到正常的结果，要返回错误
		// %q 单引号围绕的字符字面值，由Go语法安全地转义 Printf("%q", 0x4E2D) => '中'
		return fmt.Errorf("got response %s %q", resp.Status, body)
	}
	// 将返回值unmarshal到指定结构里
	err = json.Unmarshal(body, &v)
	if err != nil {
		return err
	}
	return nil
}

// PostV1 is a helper function to perform a V1 HTTP request
// and parse our NSQ daemon's expected response format, with deadlines.
// 同上；POST方法的version1版本
func (c *Client) POSTV1(endpoint string) error {
retry:
	// 创建一个post request
	req, err := http.NewRequest("POST", endpoint, nil)
	if err != nil {
		return err
	}
	// 搞一个post的header
	req.Header.Add("Accept", "application/vnd.nsq; version=1.0")
	// 发送post请求
	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}
	// 读取post请求的返回值
	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	// 判断返回值结果
	if resp.StatusCode != 200 {
		if resp.StatusCode == 403 && !strings.HasPrefix(endpoint, "https") {
			endpoint, err = httpsEndpoint(endpoint, body)
			if err != nil {
				return err
			}
			goto retry
		}
		return fmt.Errorf("got response %s %q", resp.Status, body)
	}
	return nil
}

// 获取https Endpoints的方法
// 所以说这个方法一定要保证能够返回https
func httpsEndpoint(endpoint string, body []byte) (string, error) {
	var forbiddenResp struct {
		HTTPSPort int `json:"https_port"`
	}
	// 在返回值会有一个HTTPS的port
	err := json.Unmarshal(body, &forbiddenResp)
	if err != nil {
		return "", err
	}
	// 进行解析
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	// 拆分host & port；不使用原来的port了，要使用上面解析出来的新的port
	host, _, err := net.SplitHostPort(u.Host)
	if err != nil {
		return "", err
	}
	// 将schema变为https
	u.Scheme = "https"
	// 把host和port拼起来
	u.Host = net.JoinHostPort(host, strconv.Itoa(forbiddenResp.HTTPSPort))
	return u.String(), nil
}
