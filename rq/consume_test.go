package rq

import (
	"fmt"
	"testing"
	"time"

	"github.com/xyqweb/go-queue/qtypes"
)

func TestConsume_Multiple(t *testing.T) {
	c := NewConsume("test-queue", func(queueName string, data *qtypes.MessageData) error {
		fmt.Println("queueName:", queueName, "data:", data)
		return nil
	})
	cha := make(chan bool, 1)
	cha <- true
	timer := time.NewTicker(2 * time.Second)
	for {
		select {
		case <-cha:
			go c.Start()
		case <-timer.C:
			c.Close()
			close(cha)
			return
		}
	}
}
