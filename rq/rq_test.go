package rq

import (
	"log"
	"os"
	"testing"

	"github.com/xyqweb/go-queue/qtypes"
	"github.com/xyqweb/go-queue/rq/internal/client"
)

func TestMain(m *testing.M) {
	err := client.NewRabbitmq(client.RabbitmqConf{
		Enable:     true,
		MaxRetries: 3,
		Broker:     "localhost:5672",
		Username:   "guest",
		Password:   "guest",
		Vhost:      "mall",
		Exchange:   "exchange",
		NonBlock:   false,
		Queue: []qtypes.QueueConf{
			{Name: "order", SingleActive: true},
			{Name: "user", SingleActive: false},
		},
		ConsumerTimeout: 0,
	})
	if err != nil {
		log.Fatalf("err:%v", err)
	}
	code := m.Run()
	os.Exit(code)
}
