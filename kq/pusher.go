package kq

import (
	"context"

	"github.com/xyqweb/go-queue/qtypes"
)

type Pusher interface {
	// Send consume to kafka
	// topic 主题
	// data 消息内容
	// maxRetry 最大可重试次数-默认3次
	Send(ctx context.Context, data *qtypes.QueueData) error
	// BatchSend 批量发送
	BatchSend(ctx context.Context, data []*qtypes.QueueData) error
	// Close 关闭
	Close()
}
