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
	Data any
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
	Delay uint32
	// execution count
	Attempt uint16
}
type MessageData struct {
	// message id
	ID string
	// queue body
	Body []byte
	// queue type
	Type string
	// queue create time
	CreatedAt int64
	// delay time(unit millisecond)
	Delay uint32
	// execution count
	Attempt uint16
	// error message
	//Error string `json:",omitempty"`
}
