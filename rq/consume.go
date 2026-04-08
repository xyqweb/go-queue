package rq

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/xyqweb/go-queue/qtypes"
)

type Consume interface {
	// Start consume
	Start()
	// Close consume
	Close()
}

// NewConsume new consume
func NewConsume(queueName string, handler func(queueName string, data *qtypes.MessageData) error) Consume {
	if instance == nil {
		log.Fatal("RabbitMQ has not been initialized yet")
	}
	c := &consume{
		client:    NewClient(true),
		done:      make(chan bool),
		isReady:   make(chan bool, 1),
		m:         &sync.Mutex{},
		queueName: queueName,
		handler:   handler,
	}
	return c
}

type consume struct {
	client          Client
	done            chan bool
	notifyChanClose chan *amqp.Error
	channel         *amqp.Channel
	m               *sync.Mutex
	isReady         chan bool
	handler         func(queueName string, data *qtypes.MessageData) error
	queueName       string
}

// Start consume
func (c *consume) Start() {
	if err := c.getChannel(); err != nil {
		fmt.Println(err)
		return
	}
	go c.client.HandleNotify()
	for {
		select {
		case <-c.done:
			goto EndLoop
		case <-c.notifyChanClose:
			if err := c.getChannel(); err != nil {
				fmt.Println(err)
				if errors.Is(err, amqp.ErrChannelMax) {
					c.client.Close()
				}
			}
		case <-c.isReady:
			go c.execJob()
		}
	}
EndLoop:
}

// exec job
func (c *consume) execJob() {
	deliveries, err := c.client.Consume(c.channel, c.queueName)
	if err != nil {
		fmt.Println(err)
		return
	}
	for delivery := range deliveries {
		c.handleJob(delivery)
	}
}

// handle job
func (c *consume) handleJob(delivery amqp.Delivery) {
	defer func() {
		if ackErr := delivery.Ack(true); ackErr != nil {
			fmt.Println("ack error: ", ackErr)
		}
		err := recover()
		if err != nil {
			fmt.Println(err)
		}
	}()
	var queueData qtypes.MessageBody
	if err := json.Unmarshal(delivery.Body, &queueData); err != nil {
		fmt.Println("json.Unmarshal err: ", err)
		return
	}
	body, err := json.Marshal(queueData.Body)
	if err != nil {
		fmt.Println("json.Marshal err: ", err)
		return
	}
	queue := &qtypes.MessageData{
		ID:        delivery.MessageId,
		Body:      string(body),
		Type:      queueData.Type,
		CreatedAt: delivery.Timestamp.UnixMilli(),
		Attempt:   queueData.Attempt,
	}
	if err := c.handler(c.queueName, queue); err != nil {
		if err = c.client.Push(c.queueName, queue); err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(err)
		}
	}
}

// get channel
func (c *consume) getChannel() error {
	for {
		ch, err := c.client.OpenChannel()
		if err != nil {
			fmt.Printf("c.client.OpenChannel error:%s\n", err)
			if errors.Is(err, amqp.ErrChannelMax) {
				c.client.Close()
			}
			time.Sleep(time.Second)
		} else {
			c.changeChannel(ch)
			break
		}
	}
	return nil
}

// change consume channel
func (c *consume) changeChannel(channel *amqp.Channel) {
	c.m.Lock()
	defer c.m.Unlock()
	c.channel = channel
	c.notifyChanClose = make(chan *amqp.Error, 1)
	c.channel.NotifyClose(c.notifyChanClose)
	c.isReady <- true
}

// Close consume
func (c *consume) Close() {
	close(c.done)
	c.client.Close()
}
