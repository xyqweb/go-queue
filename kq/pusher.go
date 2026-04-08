package kq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/xyqweb/go-queue/qtypes"
)

const (
	queuePushTimeout = 2 * time.Second
)

type Pusher interface {
	// Send consume to kafka
	// topic 主题
	// data 消息内容
	// maxRetry 最大可重试次数-默认3次
	Send(data *qtypes.QueueData) error
	// Close 关闭
	Close()
}

func NewPusher() Pusher {
	if kClient == nil {
		log.Fatal("Kafka has not been initialized yet")
	}
	return &pusher{}
}

type pusher struct {
}

// Send 发送消息
func (p *pusher) Send(data *qtypes.QueueData) error {
	body, err := json.Marshal(data.Body)
	if err != nil {
		return err
	}
	value, err := json.Marshal(qtypes.MessageData{
		ID:        p.generateMessageId(data.Name),
		Body:      string(body),
		Type:      data.Type,
		CreatedAt: time.Now().UnixMilli(),
		Attempt:   data.Attempt + 1,
	})
	if err != nil {
		fmt.Println(err)
		return err
	}
	cancelCtx, cancel := context.WithTimeout(context.Background(), queuePushTimeout)
	defer cancel()
	return kClient.ProduceSync(cancelCtx, &kgo.Record{
		Value: value,
		Topic: data.Name,
	}).FirstErr()
}

// generate message id
func (p *pusher) generateMessageId(name string) string {
	return fmt.Sprintf("%s.%d", name, time.Now().UnixMicro())
}

// Close 关闭
func (p *pusher) Close() {
	kClient.Close()
}
