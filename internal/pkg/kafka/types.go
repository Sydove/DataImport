package newkafka

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Partition *int32
	Headers   map[string]string
}

// validate
//
//	@Description: 校验消息体最基本的发送条件
//	@receiver m
//	@return error
func (m Message) validate() error {
	if m.Topic == "" {
		return fmt.Errorf("message topic is required")
	}
	return nil
}

type HandlerFunc func(ctx context.Context, msg *Message) error

type TopicSpec struct {
	Name              string
	NumPartitions     int
	ReplicationFactor int
	ConfigMap         map[string]string
}

type TopicOffset struct {
	Topic     string
	Partition int32
	Offset    kafka.Offset
}

type ProducerOptions struct {
	CompressionType       string
	BatchSize             int
	LingerMs              int
	Acks                  string
	MaxPending            int64
	QueueMaxMessages      int
	QueueMaxKBytes        int
	EnableIdempotence     bool
	RequestTimeoutMs      int
	MessageTimeoutMs      int
	SocketTimeoutMs       int
	Retries               int
	RetryBackoffMs        int
	BackpressureWait      time.Duration
	BackpressureThreshold int
}

// withDefaults
//
//	@Description: 为 Producer 配置补充默认值
//	@receiver o
//	@return ProducerOptions
func (o ProducerOptions) withDefaults() ProducerOptions {
	if o.CompressionType == "" {
		o.CompressionType = "snappy"
	}
	if o.BatchSize == 0 {
		o.BatchSize = 65536
	}
	if o.LingerMs == 0 {
		o.LingerMs = 10
	}
	if o.Acks == "" {
		o.Acks = "all"
	}
	if o.MaxPending == 0 {
		o.MaxPending = 5000
	}
	if o.QueueMaxMessages == 0 {
		o.QueueMaxMessages = 10000
	}
	if o.QueueMaxKBytes == 0 {
		o.QueueMaxKBytes = 65536
	}
	if !o.EnableIdempotence {
		o.EnableIdempotence = true
	}
	if o.RequestTimeoutMs == 0 {
		o.RequestTimeoutMs = 60000
	}
	if o.MessageTimeoutMs == 0 {
		o.MessageTimeoutMs = 120000
	}
	if o.SocketTimeoutMs == 0 {
		o.SocketTimeoutMs = 60000
	}
	if o.Retries == 0 {
		o.Retries = 5
	}
	if o.RetryBackoffMs == 0 {
		o.RetryBackoffMs = 100
	}
	if o.BackpressureWait == 0 {
		o.BackpressureWait = 10 * time.Millisecond
	}
	if o.BackpressureThreshold == 0 {
		o.BackpressureThreshold = 1
	}
	return o
}

type ProducerStats struct {
	SentCount        int64
	SuccessCount     int64
	FailedCount      int64
	FatalErrorCount  int64
	PendingCount     int64
	QueueLen         int64
	BackpressureHits int64
}

type ConsumerOptions struct {
	GroupID          string
	Topics           []string
	AutoOffsetReset  string
	CommitBatchSize  int
	CommitInterval   time.Duration
	PollTimeout      time.Duration
	EnableAutoCommit bool
	MaxPollRecords   int
	FetchMaxWait     time.Duration
	WorkerCount      int
	WorkQueueSize    int
}

// withDefaults
//
//	@Description: 为 Consumer 配置补充默认值
//	@receiver o
//	@return ConsumerOptions
func (o ConsumerOptions) withDefaults() ConsumerOptions {
	if o.AutoOffsetReset == "" {
		o.AutoOffsetReset = "earliest"
	}
	if o.CommitBatchSize == 0 {
		o.CommitBatchSize = 100
	}
	if o.CommitInterval == 0 {
		o.CommitInterval = 5 * time.Second
	}
	if o.PollTimeout == 0 {
		o.PollTimeout = 500 * time.Millisecond
	}
	if o.MaxPollRecords == 0 {
		o.MaxPollRecords = 50
	}
	if o.FetchMaxWait == 0 {
		o.FetchMaxWait = 100 * time.Millisecond
	}
	if o.WorkerCount == 0 {
		o.WorkerCount = 4
	}
	if o.WorkQueueSize == 0 {
		o.WorkQueueSize = o.MaxPollRecords * 4
	}
	return o
}

type BrokerInfo struct {
	ID   int
	Host string
	Port int
}

type ClusterMetadata struct {
	Brokers []BrokerInfo
	Topics  []string
}
