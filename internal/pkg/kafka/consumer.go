package kafka

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Consumer Kafka Consumer 包装
type Consumer struct {
	consumer *kafka.Consumer
	config   ConsumerConfig
}

// NewConsumer 创建 Consumer
func NewConsumer(config ConsumerConfig, consumerID int) (*Consumer, error) {
	kafkaConfig := getBaseConfig()

	// 设置 Consumer 配置
	kafkaConfig.SetKey("group.id", config.GroupID)
	kafkaConfig.SetKey("client.id", fmt.Sprintf("consumer-%d", consumerID))
	kafkaConfig.SetKey("auto.offset.reset", config.AutoOffsetReset)
	kafkaConfig.SetKey("go.events.channel.enable", true)
	kafkaConfig.SetKey("go.application.rebalance.enable", true)

	consumer, err := kafka.NewConsumer(kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("创建 Consumer 失败: %w", err)
	}

	err = consumer.SubscribeTopics(config.Topics, nil)
	if err != nil {
		consumer.Close()
		return nil, fmt.Errorf("订阅 Topic 失败: %w", err)
	}

	return &Consumer{
		consumer: consumer,
		config:   config,
	}, nil
}

// StartConsuming 开始消费消息
func (c *Consumer) StartConsuming(consumerID int, handler func(msg *kafka.Message) error) {
	messageCount := 0
	offsetMap := make(map[kafka.TopicPartition]kafka.Offset)

	for ev := range c.consumer.Events() {
		switch e := ev.(type) {
		case *kafka.Message:
			fmt.Printf("Consumer_%d got msg: %s, partition=%d offset=%d\n",
				consumerID, string(e.Value), e.TopicPartition.Partition, e.TopicPartition.Offset)

			// 处理消息
			if handler != nil {
				if err := handler(e); err != nil {
					fmt.Printf("处理消息失败: %v\n", err)
				}
			}

			// 保存该分区最新 offset
			tp := kafka.TopicPartition{
				Topic:     e.TopicPartition.Topic,
				Partition: e.TopicPartition.Partition,
			}
			offsetMap[tp] = e.TopicPartition.Offset + 1

			messageCount++

			// 达到批量数量，提交一次
			if messageCount >= c.config.CommitBatchSize {
				var offsets []kafka.TopicPartition

				for tp, off := range offsetMap {
					tp.Offset = off
					offsets = append(offsets, tp)
				}

				_, err := c.consumer.CommitOffsets(offsets)
				if err != nil {
					fmt.Println("批量提交失败:", err)
				} else {
					fmt.Println("批量提交成功:", offsets)
				}

				// 清空计数
				messageCount = 0
			}

		case kafka.AssignedPartitions:
			fmt.Println("分配分区:", e.Partitions)
			c.consumer.Assign(e.Partitions)

		case kafka.RevokedPartitions:
			fmt.Println("回收分区")
			c.consumer.Unassign()

		case kafka.Error:
			fmt.Println("Kafka Error:", e)
		}
	}
}

// Close 关闭 Consumer
func (c *Consumer) Close() error {
	return c.consumer.Close()
}

// GetConsumer 获取底层的 kafka.Consumer
func (c *Consumer) GetConsumer() *kafka.Consumer {
	return c.consumer
}
