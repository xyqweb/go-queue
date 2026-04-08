package client

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
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

func NewClient(must ...bool) Client {
	c := &client{
		m:    new(sync.Mutex),
		done: make(chan bool),
	}
	isMust := true
	if len(must) > 0 {
		isMust = must[0]
	}
	if err := c.Connect(isMust); err != nil && !isMust {
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
	Connect(must bool) error
	CloseConnection()
	HandleNotify()
	OpenChannel() (*amqp.Channel, error)
	CloseChannel(ch *amqp.Channel) error
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

// Connect will create a new AMQP connection
func (c *client) Connect(must bool) error {
	if must {
		for {
			if err := c.openConnection(); err != nil {
				fmt.Println(err)
				time.Sleep(reconnectDelay)
			} else {
				break
			}
		}
		return nil
	}
	return c.openConnection()
}

func (c *client) openConnection() error {
	c.m.Lock()
	defer c.m.Unlock()
	conn, err := amqp.Dial(Instance.GetAmqpURI())
	if err != nil {
		fmt.Println(err)
		return err
	}
	c.conn = conn
	c.notifyConnClose = make(chan *amqp.Error, 1)
	c.conn.NotifyClose(c.notifyConnClose)
	c.isReady.Store(true)
	return nil
}

// CloseConnection Proactively close connection
func (c *client) CloseConnection() {
	if c.conn != nil && !c.conn.IsClosed() {
		if err := c.conn.Close(); err != nil {
			fmt.Println(err)
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
		fmt.Println(err)
		return nil, err
	}
	return ch, nil
}

// CloseChannel close channel
func (c *client) CloseChannel(ch *amqp.Channel) error {
	if ch != nil && !ch.IsClosed() {
		return ch.Close()
	}
	return nil
}

// Push publish message to rabbitmq
func (c *client) Push(queueName string, data *qtypes.MessageData) error {
	if !c.isReady.Load() || c.conn.IsClosed() {
		if err := c.openConnection(); err != nil {
			return err
		}
	}
	channel, err := c.OpenChannel()
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer c.closeChannel(channel)
	if err = channel.Confirm(false); err != nil {
		fmt.Println(err)
		return err
	}
	publishConfirm := make(chan amqp.Confirmation, 1)
	channel.NotifyPublish(publishConfirm)
	name, routingKey, delay := c.getQueue(queueName, data.Delay, data.Attempt)
	args := c.getArgs(name, c.getRoutingKey(queueName), delay)
	if err = c.bindQueue(channel, name, routingKey, args); err != nil {
		fmt.Println(err)
		return err
	}
	if err = channel.Publish(Instance.GetExchange(), routingKey, false, false, amqp.Publishing{
		ContentType:  "text/plain",
		DeliveryMode: amqp.Persistent,
		MessageId:    data.ID,
		Timestamp:    time.Now(),
		Body:         []byte(data.Body),
	}); err != nil {
		fmt.Println(err)
		return err
	}
	confirm := <-publishConfirm
	if !confirm.Ack {
		fmt.Println("publish confirm failed")
		return errPublishConfirm
	}
	return nil
}

// get queue declare args
func (c *client) getArgs(queueName, deadLetterRoutingKey string, delay int32) amqp.Table {
	args := amqp.Table{amqp.QueueTypeArg: amqp.QueueTypeQuorum}
	if consumerTimeout := Instance.GetConsumerTimeout(); consumerTimeout > 0 {
		args[amqp.ConsumerTimeoutArg] = consumerTimeout
	}
	if delay > 0 {
		args[DeadLetterExchange] = Instance.GetExchange()
		args[DeadLetterRoutingKey] = deadLetterRoutingKey
		args[amqp.QueueMessageTTLArg] = delay
	}
	if Instance.IsSingleActive(queueName) {
		args[amqp.SingleActiveConsumerArg] = true
	}
	return args
}

func (c *client) getQueue(queueName string, delay int32, attempt uint16) (string, string, int32) {
	if attempt > Instance.GetMaxRetry() {
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
		Instance.GetExchange(), // name
		amqp.ExchangeDirect,    // type
		true,                   // durable
		false,                  // auto-deleted
		false,                  // internal
		false,                  // noWait
		nil,                    // arguments
	); err != nil {
		fmt.Println(err)
		return err
	}
	if _, err := channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		args); err != nil {
		fmt.Println(err)
		return err
	}
	if err := channel.QueueBind(
		queueName,
		routingKey,
		Instance.GetExchange(),
		false,
		nil); err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

// Consume start consume queue
func (c *client) Consume(channel *amqp.Channel, queueName string) (<-chan amqp.Delivery, error) {
	if err := channel.Qos(
		10,    // prefetchCount
		0,     // prefetchSize
		false, // global
	); err != nil {
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
			fmt.Println(err)
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
