package rq

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/xyqweb/go-queue/qlog"
	"github.com/xyqweb/go-queue/qtypes"
)

var (
	errPublishConfirm  = errors.New("publish confirm failed")
	errConnectionReady = errors.New("connection is not ready")
)

const (
	reconnectDelay       = 5 * time.Second
	DeadLetterExchange   = "x-dead-letter-exchange"
	DeadLetterRoutingKey = "x-dead-letter-routing-key"
)

func NewClient(isPermanent ...bool) Client {
	c := &client{
		m:    new(sync.Mutex),
		done: make(chan bool),
	}
	permanent := true
	if len(isPermanent) > 0 {
		permanent = isPermanent[0]
	}
	if err := c.Connect(permanent); err != nil && !permanent {
		log.Fatal(err)
	}
	return c
}

func VerifyClient() error {
	c := NewClient(false)
	defer c.Close()
	return nil
}

type Client interface {
	Connect(isPermanent bool) error
	CloseConnection()
	HandleNotify()
	OpenChannel() (*amqp.Channel, error)
	CloseChannel(ch *amqp.Channel)
	Push(queueName string, data *qtypes.MessageData) error
	Consume(channel *amqp.Channel, queueName string) (<-chan amqp.Delivery, error)
	Close()
}
type client struct {
	m               *sync.Mutex
	conn            *amqp.Connection
	done            chan bool
	notifyConnClose chan *amqp.Error
	isReady         atomic.Bool
}

type RabbitmqConf struct {
	Enable bool `json:",optional"`
	// max retry num
	MaxRetries uint16 `json:",optional"`
	// server address ip:port
	Broker   string `json:",optional"`
	Username string `json:",optional"`
	Password string `json:",optional"`
	Vhost    string `json:",optional"`
	Exchange string `json:",optional"`
	NonBlock bool   `json:",optional,default=true"`
	// queue config
	Queue []qtypes.QueueConf `json:",optional"`
	// Delivery Acknowledgement Timeout(unit millisecond)
	ConsumerTimeout int64 `json:",optional"`
}

// Connect will create a new AMQP connection
func (c *client) Connect(isPermanent bool) error {
	if isPermanent {
		for {
			if err := c.openConnection(isPermanent); err != nil {
				qlog.DefaultLogger.Errorf("open rabbitmq connection fail: %v", err)
				time.Sleep(reconnectDelay)
			} else {
				break
			}
		}
		return nil
	}
	return c.openConnection(false)
}

func (c *client) openConnection(isPermanent bool) error {
	c.m.Lock()
	defer c.m.Unlock()
	conn, err := amqp.Dial(instance.GetAmqpURI())
	if err != nil {
		qlog.DefaultLogger.Errorf("open rabbitmq connection fail: %v", err)
		return err
	}
	c.conn = conn
	if isPermanent {
		c.notifyConnClose = make(chan *amqp.Error, 1)
		c.conn.NotifyClose(c.notifyConnClose)
	}
	c.isReady.Store(true)
	return nil
}

// CloseConnection Proactively close connection
func (c *client) CloseConnection() {
	if c.conn != nil && !c.conn.IsClosed() {
		if err := c.conn.Close(); err != nil {
			qlog.DefaultLogger.Errorf("close rabbitmq connection fail: %v", err)
		}
	}
	// wait HandleNotify reopen connection
	c.isReady.Store(false)
}

// HandleNotify handle amqp notify
func (c *client) HandleNotify() {
	for {
		select {
		case <-c.done:
			goto EndHandle
		case <-c.notifyConnClose:
			_ = c.Connect(true)
		}
	}
EndHandle:
}

// OpenChannel open channel
func (c *client) OpenChannel() (*amqp.Channel, error) {
	if !c.isReady.Load() {
		return nil, errConnectionReady
	}
	ch, err := c.conn.Channel()
	if err != nil {
		qlog.DefaultLogger.Errorf("open rabbitmq channel fail: %v", err)
		return nil, err
	}
	return ch, nil
}

// CloseChannel close channel
func (c *client) CloseChannel(ch *amqp.Channel) {
	c.closeChannel(ch)
}

// Push publish message to rabbitmq
func (c *client) Push(queueName string, data *qtypes.MessageData) error {
	if !c.isReady.Load() || c.conn.IsClosed() {
		if err := c.openConnection(false); err != nil {
			return err
		}
	}
	channel, err := c.OpenChannel()
	if err != nil {
		qlog.DefaultLogger.Errorf("open rabbitmq channel fail: %v", err)
		return err
	}
	defer c.closeChannel(channel)
	if err = channel.Confirm(false); err != nil {
		qlog.DefaultLogger.Errorf("rabbitmq channel confirm fail: %v", err)
		return err
	}
	publishConfirm := make(chan amqp.Confirmation, 1)
	channel.NotifyPublish(publishConfirm)
	name, routingKey, delay := c.getQueue(queueName, data.Delay, data.Attempt)
	args := c.getArgs(name, c.getRoutingKey(queueName), delay)
	if err = c.bindQueue(channel, name, routingKey, args); err != nil {
		qlog.DefaultLogger.Errorf("rabbitmq channel bind queue fail: %v", err)
		return err
	}
	if err = channel.Publish(instance.GetExchange(), routingKey, false, false, amqp.Publishing{
		ContentType:  "text/plain",
		DeliveryMode: amqp.Persistent,
		MessageId:    data.ID,
		Timestamp:    time.Now(),
		Body:         []byte(data.Body),
	}); err != nil {
		qlog.DefaultLogger.Errorf("rabbitmq channel publish queue fail: %v", err)
		return err
	}
	confirm := <-publishConfirm
	if !confirm.Ack {
		qlog.DefaultLogger.Errorf("rabbitmq channel publish confirm ack failed fail: %v", err)
		return errPublishConfirm
	}
	return nil
}

// get queue declare args
func (c *client) getArgs(queueName, deadLetterRoutingKey string, delay int32) amqp.Table {
	args := amqp.Table{amqp.QueueTypeArg: amqp.QueueTypeQuorum}
	if consumerTimeout := instance.GetConsumerTimeout(); consumerTimeout > 0 {
		args[amqp.ConsumerTimeoutArg] = consumerTimeout
	}
	if delay > 0 {
		args[DeadLetterExchange] = instance.GetExchange()
		args[DeadLetterRoutingKey] = deadLetterRoutingKey
		args[amqp.QueueMessageTTLArg] = delay
	}
	if instance.IsSingleActive(queueName) {
		args[amqp.SingleActiveConsumerArg] = true
	}
	return args
}

func (c *client) getQueue(queueName string, delay int32, attempt uint16) (string, string, int32) {
	if attempt > instance.GetMaxRetry() {
		delay = 0
		queueName += ".error"
	}
	if delay > 0 {
		queueName = fmt.Sprintf("%s.%d.delay", queueName, delay)
	}
	return queueName, c.getRoutingKey(queueName), delay
}

func (c *client) bindQueue(channel *amqp.Channel, queueName string, routingKey string, args amqp.Table) error {
	if err := channel.ExchangeDeclare(
		instance.GetExchange(), // name
		amqp.ExchangeDirect,    // type
		true,                   // durable
		false,                  // auto-deleted
		false,                  // internal
		false,                  // noWait
		nil,                    // arguments
	); err != nil {
		qlog.DefaultLogger.Errorf("rabbitmq channel exchange declare fail: %v", err)
		return err
	}
	if _, err := channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		args); err != nil {
		qlog.DefaultLogger.Errorf("rabbitmq channel queue declare fail: %v", err)
		return err
	}
	if err := channel.QueueBind(
		queueName,
		routingKey,
		instance.GetExchange(),
		false,
		nil); err != nil {
		qlog.DefaultLogger.Errorf("rabbitmq channel queue bind fail: %v", err)
		return err
	}
	return nil
}

// Consume start consume queue
func (c *client) Consume(channel *amqp.Channel, queueName string) (<-chan amqp.Delivery, error) {
	if err := c.bindQueue(channel, queueName, c.getRoutingKey(queueName), c.getArgs(queueName, "", 0)); err != nil {
		qlog.DefaultLogger.Errorf("rabbitmq channel consume bind fail: %v", err)
		return nil, err
	}
	if err := channel.Qos(
		10,    // prefetchCount
		0,     // prefetchSize
		false, // global
	); err != nil {
		qlog.DefaultLogger.Errorf("rabbitmq channel consume qos bind fail: %v", err)
		return nil, err
	}
	return channel.Consume(
		queueName,
		fmt.Sprintf("go:amqp:%s:%d", queueName, time.Now().UnixMilli()), // Consumer
		false, // Auto-Ack
		false, // Exclusive
		false, // No-local
		false, // No-Wait
		nil,   // Args
	)
}

// get routing key
func (c *client) getRoutingKey(queueName string) string {
	return queueName + ".key"
}

// close channel
func (c *client) closeChannel(ch *amqp.Channel) {
	if ch != nil && !ch.IsClosed() {
		if err := ch.Close(); err != nil {
			qlog.DefaultLogger.Errorf("rabbitmq close channel fail: %v", err)
		}
	}
}

// Close will cleanly shut down the channel and connection.
func (c *client) Close() {
	c.m.Lock()
	defer c.m.Unlock()
	if !c.isReady.Load() {
		return
	}
	close(c.done)
	c.CloseConnection()
	c.isReady.Store(false)
}
