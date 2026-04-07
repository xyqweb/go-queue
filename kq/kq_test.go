package kq

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/xyqweb/go-queue/qtypes"
)

func TestMain(m *testing.M) {
	err := NewClient(KafkaConf{
		Enable: true,
		//MaxRetries: 3,
		Brokers:  []string{"127.0.0.1:9092"},
		MaxBytes: 1048576,
		Username: "",
		Password: "",
		Method:   "plain",
		GroupID:  "kq-tet",
		NonBlock: false,
		Queue: []qtypes.QueueConf{
			{Name: "test-queue"},
		},
	})
	if err != nil {
		log.Fatalf("kafka init error: %v", err)
	}
	code := m.Run()
	defer func() {
		fmt.Println("kq test end")
		kClient.Close()
	}()
	os.Exit(code)
}
