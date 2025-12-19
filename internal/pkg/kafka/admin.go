package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// CreateTopic 创建 Topic
func (c *Client) CreateTopic(config TopicConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := c.adminClient.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{{
			Topic:             config.Name,
			NumPartitions:     config.NumPartitions,
			ReplicationFactor: config.ReplicationFactor,
		}},
	)

	if err != nil {
		return fmt.Errorf("创建 Topic 失败: %w", err)
	}

	fmt.Printf("Topic '%s' 创建成功 (分区: %d, 副本: %d)\n",
		config.Name, config.NumPartitions, config.ReplicationFactor)
	return nil
}

// DeleteTopic 删除 Topic
func (c *Client) DeleteTopic(topicName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := c.adminClient.DeleteTopics(
		ctx,
		[]string{topicName},
		kafka.SetAdminOperationTimeout(5000),
	)

	if err != nil {
		return fmt.Errorf("删除 Topic 失败: %w", err)
	}

	fmt.Printf("Topic '%s' 删除成功\n", topicName)
	return nil
}

// GetClusterMetadata 获取集群信息
func (c *Client) GetClusterMetadata() ([]Message, error) {
	meta, err := c.adminClient.GetMetadata(nil, true, 5000)
	if err != nil {
		return nil, fmt.Errorf("获取集群元数据失败: %w", err)
	}

	fmt.Printf("Broker 数量: %d\n", len(meta.Brokers))

	results := make([]Message, 0)
	for _, b := range meta.Brokers {
		results = append(results, Message{
			ID:   fmt.Sprintf("%d", b.ID),
			Host: b.Host,
			Port: fmt.Sprintf("%d", b.Port),
		})
		fmt.Printf("  - Broker %d: %s:%d\n", b.ID, b.Host, b.Port)
	}

	fmt.Printf("Topic 数量: %d\n", len(meta.Topics))
	for topicName := range meta.Topics {
		fmt.Printf("  - Topic: %s\n", topicName)
	}

	return results, nil
}

// ListTopics 列出所有 Topic
func (c *Client) ListTopics() ([]string, error) {
	meta, err := c.adminClient.GetMetadata(nil, true, 5000)
	if err != nil {
		return nil, fmt.Errorf("获取 Topic 列表失败: %w", err)
	}

	topics := make([]string, 0, len(meta.Topics))
	for topicName := range meta.Topics {
		topics = append(topics, topicName)
	}

	return topics, nil
}

// CreateConsumer 创建 Consumer
func (c *Client) CreateConsumer(config ConsumerConfig, consumerID int, stopCtx context.Context) (*Consumer, error) {
	consumer, err := NewConsumer(config, consumerID, stopCtx)
	if err != nil {
		return nil, err
	}

	c.addConsumer(consumer)
	return consumer, nil
}

func (c *Client) WaitTopicReady(topic string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		md, err := c.adminClient.GetMetadata(&topic, false, 5_000)
		if err == nil {
			if t, ok := md.Topics[topic]; ok && t.Error.Code() == kafka.ErrNoError {
				// topic 存在且无错误
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("topic %s not ready", topic)
}
