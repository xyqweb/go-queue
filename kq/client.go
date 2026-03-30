package kq

import "github.com/xyqweb/go-queue/qtypes"

type KafkaConf struct {
	Enable bool `json:",optional"`
	// max retry num
	MaxRetries int `json:",optional"`
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
	Queue []qtypes.QueueConf `json:",optional"`
}
