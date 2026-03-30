package kq

import "github.com/xyqweb/go-queue/qtypes"

type Consume interface {
	// Start 启动消费端
	// handler 处理函数
	Start(handler func(topic string, data *qtypes.MessageData)) error
	// Close 关闭消费端
	Close()
}
