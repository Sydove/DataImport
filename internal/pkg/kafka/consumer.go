package kafka

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Consumer Kafka Consumer 包装
type Consumer struct {
	consumer *kafka.Consumer
	config   ConsumerConfig
	stopCh   chan struct{}
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
	stopCh := make(chan struct{})
	return &Consumer{
		consumer: consumer,
		config:   config,
		stopCh:   stopCh,
	}, nil
}

// StartConsuming 开始消费消息
func (c *Consumer) StartConsuming(consumerID int, handler func(msg *kafka.Message) error, total *int64) {
	messageCount := 0
	offsetMap := make(map[kafka.TopicPartition]kafka.Offset)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case ev := <-c.consumer.Events():
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
				// 增加总消息数
				atomic.AddInt64(total, 1)

				// 保存该分区最新 offset
				tp := e.TopicPartition
				tp.Offset = e.TopicPartition.Offset + 1
				offsetMap[tp] = tp.Offset
				messageCount++

				// 达到批量数量，提交一次
				if messageCount >= c.config.CommitBatchSize {
					var offsets []kafka.TopicPartition
					for tp, off := range offsetMap {
						tp.Offset = off
						offsets = append(offsets, tp)
					}
					if len(offsets) > 0 {
						_, err := c.consumer.CommitOffsets(offsets)
						if err != nil {
							fmt.Println("批量提交失败:", err)
						} else {
							fmt.Println("批量提交成功:", offsets)
							offsetMap = make(map[kafka.TopicPartition]kafka.Offset)
						}
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
		case <-ticker.C:
			//定时提交
			var offsets []kafka.TopicPartition
			for tp, off := range offsetMap {
				tp.Offset = off
				offsets = append(offsets, tp)
			}
			if len(offsets) > 0 {
				if _, err := c.consumer.CommitOffsets(offsets); err != nil {
					fmt.Println("提交失败", err)
				} else {
					fmt.Println("定时批量提交成功", offsets)
					offsetMap = make(map[kafka.TopicPartition]kafka.Offset)
				}
			}
		case <-c.stopCh:
			fmt.Println("程序停止,检查是否还有offset需要提交")
			var offsets []kafka.TopicPartition
			for tp, off := range offsetMap {
				tp.Offset = off
				offsets = append(offsets, tp)
			}
			if len(offsets) > 0 {
				if _, err := c.consumer.CommitOffsets(offsets); err != nil {
					fmt.Println("提交失败")
				} else {
					fmt.Println("结束程序批量提交成功", offsets)
				}
			}
			return
		}
	}

}

// Close 关闭 Consumer
func (c *Consumer) Close() error {
	close(c.stopCh)
	return c.consumer.Close()
}

// GetConsumer 获取底层的 kafka.Consumer
func (c *Consumer) GetConsumer() *kafka.Consumer {
	return c.consumer
}
