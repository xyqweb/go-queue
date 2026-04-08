package qtypes

// QueueConf topic conf
type QueueConf struct {
	// queue name
	Name string `json:",optional"`
	// single active consumer,Effective only in RabbitMQ
	SingleActive bool `json:",optional"`
}

// QueueData 队列数据
type QueueData struct {
	// queue name
	Name string
	// queue type
	Type string
	// queue body
	Body any
	// execution count
	Attempt uint16
}
type QueueDelayData struct {
	// queue name
	Name string
	// queue type
	Type string
	// queue body
	Body any
	// delay time(unit millisecond)
	Delay int32
	// execution count
	Attempt uint16
}
type MessageData struct {
	// message id
	ID string `json:"id"`
	// queue type
	Type string `json:"type"`
	// queue body
	Body string `json:"body"`
	// queue create time
	CreatedAt int64 `json:"createdAt"`
	// delay time(unit millisecond)
	Delay int32 `json:"delay"`
	// execution count
	Attempt uint16 `json:"attempt"`
}
type MessageBody struct {
	// queue body
	Body any `json:"body"`
	// queue type
	Type string `json:"type"`
	// execution count
	Attempt uint16 `json:"attempt"`
}
type KafkaConf struct {
	Enable bool `json:",optional"`
	// max retry num
	//MaxRetries int `json:",optional"`
	// server address ip:port
	Brokers []string `json:",optional"`
	// sets the max amount of bytes that the client will buffer
	MaxBytes int    `json:",optional,default=10485760"`
	Username string `json:",optional"`
	Password string `json:",optional"`
	// sign method
	Method string `json:",optional"`
	// consume group id
	GroupID  string `json:",optional"`
	NonBlock bool   `json:",optional,default=true"`
	// queue config
	Queue []QueueConf `json:",optional"`
}

type RabbitmqConf struct {
	Enable bool `json:",optional"`
	// max retry num
	MaxRetries uint16 `json:",optional"`
	// server address ip:port
	Broker   string `json:",optional"`
	Username string `json:",optional"`
	Password string `json:",optional"`
	Vhost    string `json:",optional"`
	Exchange string `json:",optional"`
	NonBlock bool   `json:",optional,default=true"`
	// queue config
	Queue []QueueConf `json:",optional"`
	// Delivery Acknowledgement Timeout(unit millisecond)
	ConsumerTimeout int64 `json:",optional"`
}
