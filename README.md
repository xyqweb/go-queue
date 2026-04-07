# go-queue

Go 语言实现的消息队列客户端库，支持 **Kafka** 和 **RabbitMQ**。

## 特性

- ✅ 支持 Kafka 消息队列（基于 franz-go）
- ✅ 支持 RabbitMQ 消息队列（基于 amqp091-go）
- ✅ 延迟队列支持
- ✅ 消息重试机制
- ✅ 单活消费者支持（RabbitMQ）
- ✅ 连接自动重连
- ✅ 发布确认（Publish Confirm）
- ✅ 优雅关闭

## 安装

## 快速开始
```shell
go get github.com/xyqweb/go-queue
```
### RabbitMQ 使用示例

#### 1. 初始化配置
```shell
导入 RabbitMQ 依赖包

go get github.com/rabbitmq/amqp091-go
```
```go
package main

import ( 
	"github.com/xyqweb/go-queue/rq" 
	"github.com/xyqweb/go-queue/qtypes" 
)

func main() { 
	// 初始化 RabbitMQ 
	err := rq.NewRabbitmq(rq.RabbitmqConf{ 
		Enable: true, 
		Broker: "localhost:5672", 
		Username: "guest", 
		Password: "guest", 
		Vhost: "/", 
		Exchange: "go-queue", 
		MaxRetries: 3, 
		ConsumerTimeout: 30000, // 毫秒 
		Queue: []qtypes.QueueConf{ 
			{
				Name: "order-queue", SingleActive: true}, 
				{Name: "user-queue", SingleActive: false}, 
		}, 
	}) 
	if err != nil { 
		panic(err)
	}
}
```
#### 2. 发送消息

```go
package main

import ( 
	"github.com/xyqweb/go-queue/rq" 
	"github.com/xyqweb/go-queue/qtypes" 
)

func sendOrder() { 
	pusher := rq.NewPusher() 
	defer pusher.Close()
err := pusher.Send(&qtypes.QueueDelayData{
    Name:    "order-queue",
    Type:    "order",
    Body:    map[string]interface{}{"order_id": 12345},
    Delay:   0,      // 0 表示立即投递，单位毫秒
    Attempt: 0,
})
if err != nil {
    // 处理错误
}
```
#### 3. 消费消息

```go
package main
import ( 
	"fmt" 
	"github.com/xyqweb/go-queue/rq" 
	"github.com/xyqweb/go-queue/qtypes" 
)
func consumeOrder() { 
	consume := rq.NewConsume("order-queue", func(queueName string, data *qtypes.MessageData) { 
		fmt.Printf("收到消息 - ID: %s, Type: %s, Body: %s\n", data.ID, data.Type, string(data.Body))
    // 处理业务逻辑
    // 如果抛出 panic，消息会被重试
    })

    // 启动消费（阻塞）
    consume.Start()

// 或者在 goroutine 中运行
// go consume.Start()
}
```

### Kafka 使用示例

#### 1. 初始化配置
```shell
导入kafka 依赖包
go get github.com/twmb/franz-go
go get github.com/twmb/franz-go/pkg/kmsg
```

```go
package main
import ( 
	"github.com/xyqweb/go-queue/kq" 
	"github.com/xyqweb/go-queue/qtypes" 
)
func main() { 
	// 初始化 Kafka 
	err := kq.NewClient(kq.KafkaConf{ 
		Enable: true, 
		Brokers: []string{"localhost:9092", "localhost:9093"}, 
		GroupID: "order-group", 
		MaxBytes: 10485760, // 10MB 
		NonBlock: true, 
		Queue: []qtypes.QueueConf{ 
			{Name: "order-topic"}, 
			{Name: "user-topic"}, 
		}, 
	}, 
	[]string{"order-topic", "user-topic"}) 
	if err != nil { 
		panic(err) 
	} 
}
```

#### 2. 发送消息

```go
package main

import (
	"context"

	"github.com/xyqweb/go-queue/kq"
	"github.com/xyqweb/go-queue/qtypes"
)

func sendOrder() {
	pusher := kq.NewPusher()
	defer pusher.Close()
	err := pusher.Send(context.Background(), &qtypes.QueueData{
		Name:    "order-topic",
		Type:    "order",
		Data:    map[string]interface{}{"order_id": 12345},
		Attempt: 0,
	})
	if err != nil {
		// 处理错误
	}
}

```
#### 3. 消费消息
```go
package main

import (
	"fmt"
    "github.com/xyqweb/go-queue/kq" 
	"github.com/xyqweb/go-queue/qtypes"
)
func consumeOrder() {
	consume := kq.NewConsume(func(queueName string, data *qtypes.MessageData) {
		fmt.Printf("收到消息 - Topic: %s, ID: %s, Body: %s\n", queueName, data.ID, string(data.Body))
		// 处理业务逻辑
	})

	// 启动消费（阻塞）
	err := consume.Start()
	if err != nil {
		// 处理错误
	}
}

```

## 配置说明

### RabbitMQ 配置项

| 字段 | 类型 | 说明 | 默认值 |
|------|------|------|--------|
| Enable | bool | 是否启用 | false |
| Broker | string | 服务器地址 (ip:port) | - |
| Username | string | 用户名 | - |
| Password | string | 密码 | - |
| Vhost | string | 虚拟主机 | / |
| Exchange | string | 交换机名称 | go-queue |
| MaxRetries | uint16 | 最大重试次数 | 3 |
| ConsumerTimeout | int64 | 消费者 ACK 超时 (毫秒) | 30000 |
| NonBlock | bool | 非阻塞模式 | true |
| Queue | []QueueConf | 队列配置列表 | - |

### Kafka 配置项

| 字段 | 类型 | 说明 | 默认值 |
|------|------|------|--------|
| Enable | bool | 是否启用 | false |
| Brokers | []string | 服务器地址列表 | - |
| GroupID | string | 消费组 ID | - |
| MaxBytes | int | 最大缓冲字节数 | 10485760 |
| Username | string | SASL 用户名 | - |
| Password | string | SASL 密码 | - |
| Method | string | SASL 认证方法 (plain/scramsha256/scramsha512/awsmskiam) | - |
| MaxRetries | int | 最大重试次数 | 3 |
| NonBlock | bool | 非阻塞模式 | true |
| Queue | []QueueConf | 队列配置列表 | - |

### QueueConf 配置

| 字段 | 类型 | 说明 | 默认值 |
|------|------|------|--------|
| Name | string | 队列/主题名称 | - |
| SingleActive | bool | 单活消费者 (仅 RabbitMQ) | false |

## 高级特性

### 延迟队列（RabbitMQ）

```go
// 发送 5 秒延迟消息 pusher.Send(&qtypes.QueueDelayData{ Name: "order-queue", Type: "order", Body: orderData, Delay: 5000, // 5 秒 Attempt: 0, })
```

### 消息重试

当消费者处理消息失败时（panic），消息会自动重试：
- 第 1 次失败：立即重试
- 第 2-N 次失败：60 秒后重试
- 超过 MaxRetries：投递到错误队列（`.error` 后缀）

### 单活消费者（RabbitMQ）

配置 `SingleActive: true` 后，同一消费组只有一个消费者会收到消息：

```go
Queue: []qtypes.QueueConf{ {Name: "singleton-queue", SingleActive: true}, }
```
## 错误处理

### RabbitMQ
- 连接断开自动重连
- Channel 异常自动重建
- Publish Confirm 确保消息可达

### Kafka
- Fetches 错误自动检测
- Rebalance 时自动提交 offset
- 生产者批量发送优化

## 优雅关闭

```go
// RabbitMQ 
consume := rq.NewConsume("queue", handler) go consume.Start() // ... 业务运行中 consume.Close()
// Kafka 
consume := kq.NewConsume(handler) go consume.Start() // ... 业务运行中 consume.Close()
```

## 注意事项

1. **初始化顺序**：在使用 Pusher/Consume 前必须先调用 `NewRabbitmq()` 或 `kq.NewClient()`
2. **RabbitMQ 版本要求**：需要 RabbitMQ 3.x 支持 Quorum Queue
3. **Kafka 版本要求**：最低支持 Kafka 4.0.0
4. **并发安全**：所有组件都是并发安全的
5. **资源释放**：使用完毕后务必调用 `Close()` 方法

