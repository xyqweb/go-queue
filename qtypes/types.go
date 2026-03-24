package qtypes

// QueueConf queue config
type QueueConf struct {
	Enable     bool         `json:",optional"`                        // 启用状态
	ServerType string       `json:",optional,options=kafka|rabbitmq"` // 队列服务类型
	Kafka      KafkaConf    `json:",optional"`                        // kafka配置
	Rabbitmq   RabbitmqConf `json:",optional"`                        // rabbitmq配置
	Queues     []TopicConf  `json:",optional"`                        // 队列名称
	MaxRetries int          `json:",optional"`                        // 最大重试次数
}

// TopicConf topic conf
type TopicConf struct {
	Name         string `json:",optional"` // queue name
	SingleActive bool   `json:",optional"` // single active consumer
}

type KafkaConf struct {
	Brokers  []string `json:",optional"`                  // server address ip:port
	CaFile   string   `json:",optional"`                  // ca file
	MaxBytes int      `json:",optional,default=10485760"` // 10M
	Username string   `json:",optional"`
	Password string   `json:",optional"`
	Method   string   `json:",optional"`              // sign method
	GroupID  string   `json:",optional"`              // consume group id
	NonBlock bool     `json:",optional,default=true"` //
}

type RabbitmqConf struct {
	Broker   string `json:",optional"` // server address ip:port
	Username string `json:",optional"`
	Password string `json:",optional"`
	Vhost    string `json:",optional"`
	Exchange string `json:",optional"`
	NonBlock bool   `json:",optional,default=true"` //
}

// QueueData 队列数据
type QueueData struct {
	Name string
	Type string
	Data any
}
type MessageData struct {
	ID        string
	Payload   any
	Type      string
	CreatedAt int64
	UpdatedAt int64
	Retries   int
	Error     string `json:",omitempty"`
}
