// This is an NSQ client that publishes incoming messages from
// stdin to the specified topic.

package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/app"
	"github.com/nsqio/nsq/internal/version"
)

var (
	topic     = flag.String("topic", "", "NSQ topic to publish to")
	delimiter = flag.String("delimiter", "\n", "character to split input from stdin")

	destNsqdTCPAddrs = app.StringArray{}
)

// 开始我还以为这个是通过nsqlookupd来查找nsqd呢
// TODO @lmj Q 这里是不是可以加一个参数，通过nsqlookupd来查找nsqd
// 参考一下别人是怎么通过nsqlookupd来找到nsqd的，不过他这个也不太适合用nsqlookupd来找到nsqd，
// 因为它要向所有的NSQD里面写消息，这样不就重复了吗？
// 所以我要加的功能应该是向Topic所在的所有NSQD随机发送消息
func init() {
	flag.Var(&destNsqdTCPAddrs, "nsqd-tcp-address", "destination nsqd TCP address (may be given multiple times)")
}

func main() {
	cfg := nsq.NewConfig()
	flag.Var(&nsq.ConfigFlag{cfg}, "producer-opt", "option to passthrough to nsq.Producer (may be given multiple times, http://godoc.org/github.com/nsqio/go-nsq#Config)")
	rate := flag.Int64("rate", 0, "Throttle messages to n/second. 0 to disable")

	flag.Parse()

	if len(*topic) == 0 {
		log.Fatal("--topic required")
	}

	if len(*delimiter) != 1 {
		log.Fatal("--delimiter must be a single byte")
	}

	stopChan := make(chan bool)
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	cfg.UserAgent = fmt.Sprintf("to_nsq/%s go-nsq/%s", version.Binary, nsq.VERSION)

	// make the producers
	producers := make(map[string]*nsq.Producer)
	// 它这个是直接连接NSQD？
	for _, addr := range destNsqdTCPAddrs {
		// 给每一个NSQD都创建一个producer
		producer, err := nsq.NewProducer(addr, cfg)
		if err != nil {
			log.Fatalf("failed to create nsq.Producer - %s", err)
		}
		producers[addr] = producer
	}

	if len(producers) == 0 {
		log.Fatal("--nsqd-tcp-address required")
	}
	// 阀门
	throttleEnabled := *rate >= 1
	balance := int64(1)
	// avoid divide by 0 if !throttleEnabled
	var interval time.Duration
	if throttleEnabled {
		interval = time.Second / time.Duration(*rate)
	}
	go func() {
		if !throttleEnabled {
			return
		}
		log.Printf("Throttling messages rate to max:%d/second", *rate)
		// every tick increase the number of messages we can send
		for _ = range time.Tick(interval) {
			n := atomic.AddInt64(&balance, 1)
			// if we build up more than 1s of capacity just bound to that
			if n > int64(*rate) {
				atomic.StoreInt64(&balance, int64(*rate))
			}
		}
	}()

	r := bufio.NewReader(os.Stdin)
	delim := (*delimiter)[0]
	go func() {
		for {
			var err error
			if throttleEnabled {
				currentBalance := atomic.LoadInt64(&balance)
				if currentBalance <= 0 {
					time.Sleep(interval)
				}
				// 读取并发送消息
				err = readAndPublish(r, delim, producers)
				atomic.AddInt64(&balance, -1)
			} else {
				err = readAndPublish(r, delim, producers)
			}
			if err != nil {
				if err != io.EOF {
					log.Fatal(err)
				}
				close(stopChan)
				break
			}
		}
	}()

	// 阻塞
	select {
	case <-termChan:
	case <-stopChan:
	}
	// 停掉所有的producer
	for _, producer := range producers {
		producer.Stop()
	}
}

// readAndPublish reads to the delim from r and publishes the bytes
// to the map of producers.
func readAndPublish(r *bufio.Reader, delim byte, producers map[string]*nsq.Producer) error {
	// 读取消息
	line, readErr := r.ReadBytes(delim)
	if len(line) > 0 {
		// trim the delimiter
		line = line[:len(line)-1]
	}

	if len(line) == 0 {
		return readErr
	}
	// 把消息发送给了所有的NSQD？这样不是所有的NSQD都重复了吗？
	for _, producer := range producers {
		err := producer.Publish(*topic, line)
		if err != nil {
			return err
		}
	}
	return readErr
}
