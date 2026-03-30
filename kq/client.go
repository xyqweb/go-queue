package kq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/xyqweb/go-queue/qtypes"
)

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

var kClient *kgo.Client

func NewClient(conf KafkaConf, topic []string) (err error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(conf.Brokers...),
		kgo.MinVersions(kversion.V4_0_0()),
		kgo.MaxBufferedBytes(conf.MaxBytes),
		kgo.AllowAutoTopicCreation(),
		kgo.ClientID(fmt.Sprintf("kq-:%d", os.Getpid())),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelError, nil)),
		// ===== 消费者配置（即使只做生产也建议配置）=====
		kgo.ConsumerGroup(conf.GroupID),
		kgo.ConsumeTopics(topic...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // 首次消费从头
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
			cl.CommitOffsetsSync(
				ctx,
				cl.UncommittedOffsets(),
				func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, commitErr error) {
					// 处理网络级错误
					if commitErr != nil {
						fmt.Println(commitErr)
						//logx.WithContext(ctx).Errorf("commit error: %v", commitErr)
						return
					}
					// 处理分区级错误（关键！）
					for _, t := range resp.Topics {
						for _, part := range t.Partitions {
							if part.ErrorCode != 0 {
								fmt.Println(kerr.ErrorForCode(part.ErrorCode))
								//logx.WithContext(ctx).Errorf("kerr: %v", kerr.ErrorForCode(part.ErrorCode))
							}
						}
					}
					// ✅ 即使失败也继续 Rebalance（Kafka 协议要求）
				},
			)
			// ⚠️ 注意：CommitOffsetsSync 是同步阻塞的！
			// 执行完 onDone 后才返回，确保提交完成再释放分区
		}),
		// ===== 生产者配置 =====
		kgo.ProducerBatchMaxBytes(10485760),       // 10MB
		kgo.ProducerLinger(10 * time.Millisecond), // 批量发送优化
	}
	if conf.Username != "" && conf.Password != "" && conf.Method != "" {
		switch conf.Method {
		case "plain":
			opts = append(opts, kgo.SASL(plain.Auth{
				User: conf.Username,
				Pass: conf.Password,
			}.AsMechanism()))
		case "scramsha256":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: conf.Username,
				Pass: conf.Password,
			}.AsSha256Mechanism()))
		case "scramsha512":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: conf.Username,
				Pass: conf.Password,
			}.AsSha512Mechanism()))
		case "awsmskiam":
			opts = append(opts, kgo.SASL(aws.Auth{
				AccessKey: conf.Username,
				SecretKey: conf.Password,
			}.AsManagedStreamingIAMMechanism()))
		default:
			return errors.New("unsupported method: " + conf.Method)
		}
	}
	kClient, err = kgo.NewClient(opts...)
	return err
}
