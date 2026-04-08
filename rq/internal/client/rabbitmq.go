package client

import (
	"fmt"
	"net/url"
	"sync"

	"github.com/xyqweb/go-queue/qtypes"
)

var (
	Instance     Rabbitmq
	rabbitmqOnce sync.Once
)

type Rabbitmq interface {
	GetExchange() string
	GetConsumerTimeout() int64
	GetAmqpURI() string
	GetQueueMap() map[string]bool
	GetMaxRetry() uint16
	IsSingleActive(queueName string) bool
	Verify() error
	Initialize()
}
type rabbitmq struct {
	Conf     qtypes.RabbitmqConf
	amqpURI  string
	queueMap map[string]bool
}

func (r *rabbitmq) IsSingleActive(queueName string) bool {
	if isSingle, ok := r.queueMap[queueName]; ok {
		return isSingle
	} else {
		return false
	}
}

func (r *rabbitmq) GetExchange() string {
	return r.Conf.Exchange
}
func (r *rabbitmq) GetConsumerTimeout() int64 {
	return r.Conf.ConsumerTimeout
}
func (r *rabbitmq) GetAmqpURI() string {
	return r.amqpURI
}

func (r *rabbitmq) GetQueueMap() map[string]bool {
	return r.queueMap
}
func (r *rabbitmq) GetMaxRetry() uint16 {
	return r.Conf.MaxRetries
}

// Verify verify rabbitmq config is correct
func (r *rabbitmq) Verify() error {
	return VerifyClient()
}

func (r *rabbitmq) Initialize() {
	for _, q := range r.Conf.Queue {
		r.queueMap[q.Name] = q.SingleActive
	}
	r.amqpURI = r.getRabbitURL()
}

func (r *rabbitmq) getRabbitURL() string {
	return fmt.Sprintf("amqp://%s:%s@%s/%s", r.Conf.Username, url.PathEscape(r.Conf.Password), r.Conf.Broker, r.Conf.Vhost)
}

func NewRabbitmq(rabbitmqConf qtypes.RabbitmqConf) error {
	if !rabbitmqConf.Enable {
		return nil
	}
	rabbitmqOnce.Do(func() {
		Instance = &rabbitmq{Conf: rabbitmqConf, queueMap: make(map[string]bool, len(rabbitmqConf.Queue))}
		Instance.Initialize()
	})
	if !rabbitmqConf.NonBlock {
		if err := Instance.Verify(); err != nil {
			return err
		}
	}
	return nil
}
