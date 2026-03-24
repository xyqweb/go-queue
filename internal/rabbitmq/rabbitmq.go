package rabbitmq

import (
	"fmt"
	"net/url"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/xyqweb/go-queue/qtypes"
)

var (
	rabbitmq *Rabbitmq
)

func NewRabbitmq(conf *qtypes.RabbitmqConf) error {
	amqpUrl := fmt.Sprintf("amqp://%s:%s@%s/", conf.Username, url.QueryEscape(conf.Password), conf.Broker)
	rabbitmq = &Rabbitmq{amqpUrl: amqpUrl, conf: conf}
	return rabbitmq.Verify()
}

type Rabbitmq struct {
	amqpUrl string
	conf    *qtypes.RabbitmqConf
}

func (r *Rabbitmq) GetConnection() (*amqp.Connection, error) {
	connection, err := amqp.DialConfig(r.amqpUrl, amqp.Config{
		Vhost:      r.conf.Vhost,
		Properties: amqp.NewConnectionProperties(),
	})
	if err != nil {
		return nil, err
	}
	return connection, nil
}

// Verify config is correct
func (r *Rabbitmq) Verify() error {
	if r.conf.NonBlock {
		return nil
	}
	connection, err := r.GetConnection()
	if err != nil {
		return err
	}
	defer r.CloseConnection(connection)
	return nil
}
func (r *Rabbitmq) GetChannel(connection *amqp.Connection) (*amqp.Channel, error) {
	if connection == nil || connection.IsClosed() {
		return nil, amqp.ErrClosed
	}
	return connection.Channel()
}

func (r *Rabbitmq) CloseChannel(channel *amqp.Channel) {
	if channel != nil && !channel.IsClosed() {
		_ = channel.Close()
	}
}

// CloseConnection close rabbitmq connection
func (r *Rabbitmq) CloseConnection(connection *amqp.Connection) {
	if connection != nil && !connection.IsClosed() {
		_ = connection.Close()
	}
}

type Client struct {
	m               *sync.Mutex
	queueName       string
	connection      *amqp.Connection
	channel         *amqp.Channel
	done            chan bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	isReady         bool
}
