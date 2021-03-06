// This is an NSQ client that reads the specified topic/channel
// and re-publishes the messages to destination nsqd via TCP

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bitly/go-hostpool"
	"github.com/bitly/timer_metrics"
	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/app"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/version"
)

const (
	ModeRoundRobin = iota // round robin 轮询调度
	ModeHostPool          // epsilon-greedy 这个是随机
)

var (
	showVersion = flag.Bool("version", false, "print version string")
	channel     = flag.String("channel", "nsq_to_nsq", "nsq channel")
	destTopic   = flag.String("destination-topic", "", "use this destination topic for all consumed topics (default is consumed topic name)")
	maxInFlight = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")

	statusEvery = flag.Int("status-every", 250, "the # of requests between logging status (per destination), 0 disables")
	mode        = flag.String("mode", "hostpool", "the upstream request mode options: round-robin, hostpool (default), epsilon-greedy")

	nsqdTCPAddrs        = app.StringArray{}
	lookupdHTTPAddrs    = app.StringArray{}
	destNsqdTCPAddrs    = app.StringArray{}
	whitelistJSONFields = app.StringArray{}
	topics              = app.StringArray{}

	requireJSONField = flag.String("require-json-field", "", "for JSON messages: only pass messages that contain this field")
	requireJSONValue = flag.String("require-json-value", "", "for JSON messages: only pass messages in which the required field has this value")
)

func init() {
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&destNsqdTCPAddrs, "destination-nsqd-tcp-address", "destination nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	flag.Var(&topics, "topic", "nsq topic (may be given multiple times)")
	flag.Var(&whitelistJSONFields, "whitelist-json-field", "for JSON messages: pass this field (may be given multiple times)")
}

type PublishHandler struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	counter uint64

	addresses app.StringArray
	producers map[string]*nsq.Producer
	mode      int
	hostPool  hostpool.HostPool
	respChan  chan *nsq.ProducerTransaction

	requireJSONValueParsed   bool
	requireJSONValueIsNumber bool
	requireJSONNumber        float64

	perAddressStatus map[string]*timer_metrics.TimerMetrics
	timermetrics     *timer_metrics.TimerMetrics
}

type TopicHandler struct {
	publishHandler   *PublishHandler
	destinationTopic string
}

func (ph *PublishHandler) responder() {
	var msg *nsq.Message
	var startTime time.Time
	var address string
	var hostPoolResponse hostpool.HostPoolResponse

	for t := range ph.respChan {
		switch ph.mode {
		case ModeRoundRobin:
			msg = t.Args[0].(*nsq.Message)
			startTime = t.Args[1].(time.Time)
			hostPoolResponse = nil
			address = t.Args[2].(string)
		case ModeHostPool:
			msg = t.Args[0].(*nsq.Message)
			startTime = t.Args[1].(time.Time)
			hostPoolResponse = t.Args[2].(hostpool.HostPoolResponse)
			address = hostPoolResponse.Host()
		}

		success := t.Error == nil

		if hostPoolResponse != nil {
			if !success {
				hostPoolResponse.Mark(errors.New("failed"))
			} else {
				hostPoolResponse.Mark(nil)
			}
		}

		if success {
			msg.Finish()
		} else {
			msg.Requeue(-1)
		}

		ph.perAddressStatus[address].Status(startTime)
		ph.timermetrics.Status(startTime)
	}
}

func (ph *PublishHandler) shouldPassMessage(js map[string]interface{}) (bool, bool) {
	pass := true
	backoff := false

	if *requireJSONField == "" {
		return pass, backoff
	}

	if *requireJSONValue != "" && !ph.requireJSONValueParsed {
		// cache conversion in case needed while filtering json
		var err error
		ph.requireJSONNumber, err = strconv.ParseFloat(*requireJSONValue, 64)
		ph.requireJSONValueIsNumber = (err == nil)
		ph.requireJSONValueParsed = true
	}

	v, ok := js[*requireJSONField]
	if !ok {
		pass = false
		if *requireJSONValue != "" {
			log.Printf("ERROR: missing field to check required value")
			backoff = true
		}
	} else if *requireJSONValue != "" {
		// if command-line argument can't convert to float, then it can't match a number
		// if it can, also integers (up to 2^53 or so) can be compared as float64
		if s, ok := v.(string); ok {
			if s != *requireJSONValue {
				pass = false
			}
		} else if ph.requireJSONValueIsNumber {
			f, ok := v.(float64)
			if !ok || f != ph.requireJSONNumber {
				pass = false
			}
		} else {
			// json value wasn't a plain string, and argument wasn't a number
			// give up on comparisons of other types
			pass = false
		}
	}

	return pass, backoff
}

func filterMessage(js map[string]interface{}, rawMsg []byte) ([]byte, error) {
	if len(whitelistJSONFields) == 0 {
		// no change
		return rawMsg, nil
	}

	newMsg := make(map[string]interface{}, len(whitelistJSONFields))

	for _, key := range whitelistJSONFields {
		value, ok := js[key]
		if ok {
			// avoid printing int as float (go 1.0)
			switch tvalue := value.(type) {
			case float64:
				ivalue := int64(tvalue)
				if float64(ivalue) == tvalue {
					newMsg[key] = ivalue
				} else {
					newMsg[key] = tvalue
				}
			default:
				newMsg[key] = value
			}
		}
	}

	newRawMsg, err := json.Marshal(newMsg)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal filtered message %v", newMsg)
	}
	return newRawMsg, nil
}

func (t *TopicHandler) HandleMessage(m *nsq.Message) error {
	return t.publishHandler.HandleMessage(m, t.destinationTopic)
}

func (ph *PublishHandler) HandleMessage(m *nsq.Message, destinationTopic string) error {
	var err error
	msgBody := m.Body

	if *requireJSONField != "" || len(whitelistJSONFields) > 0 {
		var js map[string]interface{}
		err = json.Unmarshal(msgBody, &js)
		if err != nil {
			log.Printf("ERROR: Unable to decode json: %s", msgBody)
			return nil
		}

		if pass, backoff := ph.shouldPassMessage(js); !pass {
			if backoff {
				return errors.New("backoff")
			}
			return nil
		}

		msgBody, err = filterMessage(js, msgBody)

		if err != nil {
			log.Printf("ERROR: filterMessage() failed: %s", err)
			return err
		}
	}

	startTime := time.Now()

	switch ph.mode {
	case ModeRoundRobin:
		// 计数器
		counter := atomic.AddUint64(&ph.counter, 1)
		// 选择那个producer来进行produce
		idx := counter % uint64(len(ph.addresses))
		addr := ph.addresses[idx]
		p := ph.producers[addr]
		err = p.PublishAsync(destinationTopic, msgBody, ph.respChan, m, startTime, addr)
	case ModeHostPool:
		// TODO @lmj 讲道理这个不是随机的选取吗？
		hostPoolResponse := ph.hostPool.Get()
		p := ph.producers[hostPoolResponse.Host()]
		err = p.PublishAsync(destinationTopic, msgBody, ph.respChan, m, startTime, hostPoolResponse)
		if err != nil {
			hostPoolResponse.Mark(err)
		}
	}

	if err != nil {
		return err
	}
	m.DisableAutoResponse()
	return nil
}

func hasArg(s string) bool {
	argExist := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == s {
			argExist = true
		}
	})
	return argExist
}

func main() {
	var selectedMode int // 选择目标NSQD的模式

	cCfg := nsq.NewConfig()
	pCfg := nsq.NewConfig()

	flag.Var(&nsq.ConfigFlag{cCfg}, "consumer-opt", "option to passthrough to nsq.Consumer (may be given multiple times, see http://godoc.org/github.com/nsqio/go-nsq#Config)")
	flag.Var(&nsq.ConfigFlag{pCfg}, "producer-opt", "option to passthrough to nsq.Producer (may be given multiple times, see http://godoc.org/github.com/nsqio/go-nsq#Config)")

	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_to_nsq v%s\n", version.Binary)
		return
	}

	// 可以额外再指定一个TOPIC
	if len(topics) == 0 || *channel == "" {
		log.Fatal("--topic and --channel are required")
	}
	// topic名称是否合法？
	for _, topic := range topics {
		if !protocol.IsValidTopicName(topic) {
			log.Fatal("--topic is invalid")
		}
	}

	if *destTopic != "" && !protocol.IsValidTopicName(*destTopic) {
		log.Fatal("--destination-topic is invalid")
	}

	if !protocol.IsValidChannelName(*channel) {
		log.Fatal("--channel is invalid")
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatal("--nsqd-tcp-address or --lookupd-http-address required")
	}
	if len(nsqdTCPAddrs) > 0 && len(lookupdHTTPAddrs) > 0 {
		log.Fatal("use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	// TODO @lmj 目标只有Nsqd是吗？
	if len(destNsqdTCPAddrs) == 0 {
		log.Fatal("--destination-nsqd-tcp-address required")
	}

	// 选择目标NSQD的两种模式，默认是hostpool或者说是epsilon-greedy这个是随机；另一种是round-robin这个是轮询。
	switch *mode {
	case "round-robin":
		selectedMode = ModeRoundRobin
	case "hostpool", "epsilon-greedy":
		selectedMode = ModeHostPool
	}

	// 接收程序终端信号
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	// 默认user agent信息
	defaultUA := fmt.Sprintf("nsq_to_nsq/%s go-nsq/%s", version.Binary, nsq.VERSION)

	cCfg.UserAgent = defaultUA
	cCfg.MaxInFlight = *maxInFlight
	pCfg.UserAgent = defaultUA

	// 根据目标的NSQD创建好所有的producer，一个NSQD一个并且根据addr放到map中
	producers := make(map[string]*nsq.Producer)
	for _, addr := range destNsqdTCPAddrs {
		producer, err := nsq.NewProducer(addr, pCfg)
		if err != nil {
			log.Fatalf("failed creating producer %s", err)
		}
		producers[addr] = producer
	}

	perAddressStatus := make(map[string]*timer_metrics.TimerMetrics)
	if len(destNsqdTCPAddrs) == 1 {
		// disable since there is only one address
		perAddressStatus[destNsqdTCPAddrs[0]] = timer_metrics.NewTimerMetrics(0, "")
	} else {
		for _, a := range destNsqdTCPAddrs {
			perAddressStatus[a] = timer_metrics.NewTimerMetrics(*statusEvery, fmt.Sprintf("[%s]:", a))
		}
	}

	// TODO @lmj 这里为啥hostpool和epsilon-greedy不一样了呢？它俩在上面不是一样的吗？
	hostPool := hostpool.New(destNsqdTCPAddrs)
	if *mode == "epsilon-greedy" {
		hostPool = hostpool.NewEpsilonGreedy(destNsqdTCPAddrs, 0, &hostpool.LinearEpsilonValueCalculator{})
	}

	var consumerList []*nsq.Consumer

	publisher := &PublishHandler{
		addresses:        destNsqdTCPAddrs,                                           // 目标的所有NSQD地址
		producers:        producers,                                                  // 所有的producer
		mode:             selectedMode,                                               // 模式
		hostPool:         hostPool,                                                   // 目标的host池
		respChan:         make(chan *nsq.ProducerTransaction, len(destNsqdTCPAddrs)), // 回复Chan
		perAddressStatus: perAddressStatus,
		timermetrics:     timer_metrics.NewTimerMetrics(*statusEvery, "[aggregate]:"),
	}

	// 根据topic来创建consumer，一个topic一个consumer
	for _, topic := range topics {
		consumer, err := nsq.NewConsumer(topic, *channel, cCfg)
		consumerList = append(consumerList, consumer)
		if err != nil {
			log.Fatal(err)
		}

		publishTopic := topic
		if *destTopic != "" {
			publishTopic = *destTopic
		}
		topicHandler := &TopicHandler{
			publishHandler:   publisher,
			destinationTopic: publishTopic,
		}
		consumer.AddConcurrentHandlers(topicHandler, len(destNsqdTCPAddrs))
	}
	for i := 0; i < len(destNsqdTCPAddrs); i++ {
		go publisher.responder()
	}

	// 消费者都是这样的。都是先连接NSQD然后再尝试连接NSQDLookupd
	for _, consumer := range consumerList {
		// 一旦连接上之后，consumer就开始接收消息，然后往producer里面发了。
		err := consumer.ConnectToNSQDs(nsqdTCPAddrs)
		if err != nil {
			log.Fatal(err)
		}
	}
	for _, consumer := range consumerList {
		// 一旦连接上之后，consumer就开始接收消息，然后往producer里面发了。
		err := consumer.ConnectToNSQLookupds(lookupdHTTPAddrs)
		if err != nil {
			log.Fatal(err)
		}
	}

	<-termChan // wait for signal

	for _, consumer := range consumerList {
		consumer.Stop()
	}
	for _, consumer := range consumerList {
		<-consumer.StopChan
	}
}
