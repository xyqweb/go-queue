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
