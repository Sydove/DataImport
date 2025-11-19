package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaClient struct {
	producer       *kafka.Producer
	consumer       *kafka.Consumer
	consumerGroup  string
	wg             sync.WaitGroup
	cancelFunc     context.CancelFunc
	deliveryChan   chan kafka.Event
	offsetLock     sync.Mutex
	pendingOffsets []*kafka.Message
}

// Config 用于创建 KafkaClient 时传入可配置参数
type Config struct {
	BootstrapServers string
	ConsumerGroup    string
	ProducerConfig   map[string]interface{}
	ConsumerConfig   map[string]interface{}
	DeliveryChanSize int
}

// NewKafkaClient 创建 Kafka 客户端
func NewKafkaClient(cfg Config) (*KafkaClient, error) {
	if cfg.BootstrapServers == "" {
		return nil, errors.New("bootstrap servers cannot be empty")
	}

	// 生产者默认配置
	producerCfg := &kafka.ConfigMap{
		"bootstrap.servers":            cfg.BootstrapServers,
		"acks":                         "all",
		"enable.idempotence":           true,
		"max.in.flight":                1,
		"message.timeout.ms":           30000,
		"queue.buffering.max.messages": 100000,
	}
	for k, v := range cfg.ProducerConfig {
		producerCfg.SetKey(k, v)
	}

	p, err := kafka.NewProducer(producerCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	// 消费者默认配置
	consumerCfg := &kafka.ConfigMap{
		"bootstrap.servers":  cfg.BootstrapServers,
		"group.id":           cfg.ConsumerGroup,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	}
	for k, v := range cfg.ConsumerConfig {
		consumerCfg.SetKey(k, v)
	}

	c, err := kafka.NewConsumer(consumerCfg)
	if err != nil {
		p.Close()
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	deliveryChanSize := cfg.DeliveryChanSize
	if deliveryChanSize <= 0 {
		deliveryChanSize = 1000
	}

	client := &KafkaClient{
		producer:       p,
		consumer:       c,
		consumerGroup:  cfg.ConsumerGroup,
		deliveryChan:   make(chan kafka.Event, deliveryChanSize),
		pendingOffsets: make([]*kafka.Message, 0),
	}

	// 启动异步 ACK 处理
	client.wg.Add(1)
	go client.handleDelivery()

	return client, nil
}

// handleDelivery 异步处理生产者 ACK
func (k *KafkaClient) handleDelivery() {
	defer k.wg.Done()
	for e := range k.deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("Delivery failed: %v", ev.TopicPartition.Error)
			}
		default:
			// 其他事件忽略
		}
	}
}

// AsyncProduce 异步生产消息
func (k *KafkaClient) AsyncProduce(topic string, key, value []byte) error {
	return k.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          value,
		Headers:        []kafka.Header{{Key: "produced_at", Value: []byte(time.Now().Format(time.RFC3339))}},
	}, k.deliveryChan)
}

// StartConsuming 启动消费者循环（批量提交 offset）
func (k *KafkaClient) StartConsuming(ctx context.Context, topics []string, handler func(msg *kafka.Message) error, commitInterval time.Duration) error {
	if err := k.consumer.SubscribeTopics(topics, nil); err != nil {
		return fmt.Errorf("failed to subscribe topics: %w", err)
	}

	ctx, k.cancelFunc = context.WithCancel(ctx)
	k.wg.Add(1)

	go func() {
		defer k.wg.Done()
		ticker := time.NewTicker(commitInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				k.commitPendingOffsets()
				return
			case <-ticker.C:
				k.commitPendingOffsets()
			default:
				msg, err := k.consumer.ReadMessage(100 * time.Millisecond)
				if err != nil {
					if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
						continue
					}
					log.Printf("Consumer error: %v", err)
					continue
				}

				if err := handler(msg); err != nil {
					log.Printf("Handler failed: %v", err)
					continue
				}

				// 收集待提交 offset
				k.offsetLock.Lock()
				k.pendingOffsets = append(k.pendingOffsets, msg)
				k.offsetLock.Unlock()
			}
		}
	}()

	return nil
}

// commitPendingOffsets 批量提交 offset
func (k *KafkaClient) commitPendingOffsets() {
	k.offsetLock.Lock()
	defer k.offsetLock.Unlock()
	if len(k.pendingOffsets) == 0 {
		return
	}
	offsets := make([]kafka.TopicPartition, len(k.pendingOffsets))
	for i, m := range k.pendingOffsets {
		offsets[i] = m.TopicPartition
	}
	_, err := k.consumer.CommitOffsets(offsets)
	if err != nil {
		log.Printf("Failed to commit offsets: %v", err)
	}
	k.pendingOffsets = k.pendingOffsets[:0]
}

// Close 安全关闭客户端
func (k *KafkaClient) Close() {
	if k.cancelFunc != nil {
		k.cancelFunc()
	}

	k.wg.Wait()

	if k.producer != nil {
		for k.producer.Len() > 0 {
			k.producer.Flush(1000)
		}
		close(k.deliveryChan)
		k.producer.Close()
	}

	if k.consumer != nil {
		k.commitPendingOffsets()
		k.consumer.Close()
	}
}

// HealthCheck 检查健康状态
func (k *KafkaClient) HealthCheck() error {
	if k.producer == nil || k.consumer == nil {
		return errors.New("client not initialized")
	}

	// 生产者检查
	if _, err := k.producer.GetMetadata(nil, false, 2000); err != nil {
		return fmt.Errorf("producer health failed: %w", err)
	}

	// 消费者检查
	if _, err := k.consumer.GetMetadata(nil, false, 2000); err != nil {
		return fmt.Errorf("consumer health failed: %w", err)
	}

	return nil
}
