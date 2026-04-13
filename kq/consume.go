package kq

import (
	"context"
	"encoding/json"
	"log"

	"github.com/xyqweb/go-queue/qlog"
	"github.com/xyqweb/go-queue/qtypes"
)

type Consume interface {
	// Start 启动消费端
	// handler 处理函数
	Start() error
	// Close 关闭消费端
	Close()
}

func NewConsume(handler func(queueName string, data *qtypes.MessageData)) Consume {
	if kClient == nil {
		log.Fatal("Kafka has not been initialized yet")
	}
	return &consume{handler: handler}
}

type consume struct {
	handler func(queueName string, data *qtypes.MessageData)
}

func (c *consume) Start() error {
	for {
		fetches := kClient.PollFetches(context.Background())
		if err := fetches.Err0(); err != nil {
			return err
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			var data qtypes.MessageData
			if err := json.Unmarshal(record.Value, &data); err != nil {
				qlog.DefaultLogger.Errorf("kafka consume json.Unmarshal fail: %v", err)
				return err
			}
			c.handler(record.Topic, &data)
		}
	}
}

func (c *consume) Close() {
	kClient.Close()
}
