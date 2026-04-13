package rq

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/xyqweb/go-queue/qlog"
	"github.com/xyqweb/go-queue/qtypes"
)

type Pusher interface {
	// Send queue message
	// data queue message
	Send(data *qtypes.QueueDelayData) error
	// Close pusher
	Close()
}

func NewPusher() Pusher {
	if instance == nil {
		log.Fatal("RabbitMQ has not been initialized yet")
	}
	return &pusher{client: NewClient()}
}

type pusher struct {
	client Client
}

// Close pusher
func (p *pusher) Close() {
	p.client.Close()
}

// Send queue message
func (p *pusher) Send(data *qtypes.QueueDelayData) error {
	data.Attempt++
	if data.Attempt > 1 {
		data.Delay = 60000
	}
	body, err := json.Marshal(&qtypes.MessageBody{
		Body:    data.Body,
		Type:    data.Type,
		Attempt: data.Attempt,
	})
	if err != nil {
		qlog.DefaultLogger.Errorf("rabbitmq pusher json.Marshal fail: %v", err)
		return err
	}
	return p.client.Push(data.Name, &qtypes.MessageData{
		ID:        p.generateMessageId(data.Name),
		Body:      string(body),
		CreatedAt: time.Now().UnixMilli(),
		Delay:     data.Delay,
		Attempt:   data.Attempt,
	})
}

// generate message id
func (p *pusher) generateMessageId(name string) string {
	return fmt.Sprintf("%s.%d", name, time.Now().UnixMicro())
}
