package kq

import (
	"fmt"
	"testing"
	"time"

	"github.com/xyqweb/go-queue/qtypes"
)

func TestConsume_Multiple(t *testing.T) {
	c := NewConsume(func(queueName string, data *qtypes.MessageData) {
		fmt.Println("queueName:", queueName, "data:", data)
	})
	//c.Start()
	cha := make(chan bool, 1)
	cha <- true
	timer := time.NewTicker(8 * time.Second)
	for {
		select {
		case <-cha:
			go c.Start()
		case <-timer.C:
			close(cha)
			return
		}
	}
}
