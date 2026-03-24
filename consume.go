package go_queue

import "github.com/xyqweb/go-queue/qtypes"

type Consume interface {
	// Start 启动消费端
	// topic 单个消费端队列名称
	// handler 处理函数
	Start(topic string, handler func(topic string, data *qtypes.MessageData)) error
	// Close 关闭消费端
	Close()
}

func NewConsume() {

}
