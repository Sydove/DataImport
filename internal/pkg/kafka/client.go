package kafka

import (
	"fmt"
	"strings"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/spf13/viper"
)

// Client Kafka 客户端
type Client struct {
	producer    *kafka.Producer
	adminClient *kafka.AdminClient
	//consumers   []*kafka.Consumer
	consumers []*Consumer
	mu        sync.RWMutex
}

// NewClient 创建 Kafka 客户端
func NewClient() (*Client, error) {
	// 创建 Admin Client
	adminConfig := getBaseConfig()
	admin, err := kafka.NewAdminClient(adminConfig)
	if err != nil {
		return nil, fmt.Errorf("创建 Admin Client 失败: %w", err)
	}

	// 创建 Producer（使用默认配置）
	producer, err := NewProducer(ProducerConfig{
		CompressionType: "snappy",
		BatchSize:       51200,
		LingerMs:        10,
		Acks:            "all",
		MaxPending:      10000,
	})
	if err != nil {
		admin.Close()
		return nil, fmt.Errorf("创建 Producer 失败: %w", err)
	}

	return &Client{
		producer:    producer.producer,
		adminClient: admin,
		consumers:   make([]*Consumer, 0),
	}, nil
}

// NewClientWithConfig 使用自定义配置创建 Kafka 客户端
func NewClientWithConfig(producerConfig ProducerConfig) (*Client, error) {
	adminConfig := getBaseConfig()
	admin, err := kafka.NewAdminClient(adminConfig)
	if err != nil {
		return nil, fmt.Errorf("创建 Admin Client 失败: %w", err)
	}

	producer, err := NewProducer(producerConfig)
	if err != nil {
		admin.Close()
		return nil, fmt.Errorf("创建 Producer 失败: %w", err)
	}

	return &Client{
		producer:    producer.producer,
		adminClient: admin,
		consumers:   make([]*Consumer, 0),
	}, nil
}

// GetProducer 获取 Producer
func (c *Client) GetProducer() *kafka.Producer {
	return c.producer
}

// GetAdminClient 获取 Admin Client
func (c *Client) GetAdminClient() *kafka.AdminClient {
	return c.adminClient
}

// Close 关闭所有资源
func (c *Client) Close() error {
	fmt.Println("关闭所有资源")
	c.mu.Lock()
	defer c.mu.Unlock()

	// 关闭 Producer
	if c.producer != nil {
		c.producer.Close()
	}

	// 关闭 Admin Client
	if c.adminClient != nil {
		c.adminClient.Close()
	}

	// 关闭所有 Consumer
	for _, consumer := range c.consumers {
		if consumer != nil {
			fmt.Printf("%+v\n", consumer)
			consumer.Close()
		}
	}

	return nil
}

// getBaseConfig 获取基础配置
func getBaseConfig() *kafka.ConfigMap {
	kafkaAddress := viper.GetStringSlice("kafka.addr")
	return &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(kafkaAddress, ","),
	}
}

// addConsumer 添加 Consumer 到管理列表
func (c *Client) addConsumer(consumer *Consumer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumers = append(c.consumers, consumer)
}
