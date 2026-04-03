package newkafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumer struct {
	raw       *kafka.Consumer
	opts      ConsumerOptions
	closeOnce sync.Once
}

// newConsumer
//
//	@Description: 创建底层 Kafka Consumer 封装
//	@param baseConfig
//	@param opts
//	@return *Consumer
//	@return error
func newConsumer(baseConfig *kafka.ConfigMap, opts ConsumerOptions) (*Consumer, error) {
	opts = opts.withDefaults()
	if opts.GroupID == "" {
		return nil, fmt.Errorf("consumer group id is required")
	}
	if len(opts.Topics) == 0 {
		return nil, fmt.Errorf("consumer topics is empty")
	}

	baseConfig.SetKey("group.id", opts.GroupID)
	baseConfig.SetKey("auto.offset.reset", opts.AutoOffsetReset)
	baseConfig.SetKey("enable.auto.commit", opts.EnableAutoCommit)
	baseConfig.SetKey("go.application.rebalance.enable", true)

	rawConsumer, err := kafka.NewConsumer(baseConfig)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}

	if err := rawConsumer.SubscribeTopics(opts.Topics, nil); err != nil {
		rawConsumer.Close()
		return nil, fmt.Errorf("subscribe kafka topics: %w", err)
	}

	return &Consumer{
		raw:  rawConsumer,
		opts: opts,
	}, nil
}

// Run
//
//	@Description: 运行消费循环，handler 成功后按批次或定时提交 offset
//	@receiver c
//	@param ctx
//	@param handler
//	@return error
func (c *Consumer) Run(ctx context.Context, handler HandlerFunc) error {
	if handler == nil {
		return fmt.Errorf("consumer handler is required")
	}

	pendingOffsets := make(map[string]TopicOffset)
	processed := 0
	commitTicker := time.NewTicker(c.opts.CommitInterval)
	defer commitTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			if !c.opts.EnableAutoCommit {
				if err := c.commitPending(pendingOffsets); err != nil && !isIgnorableCommitError(err) {
					return err
				}
			}
			return nil
		case <-commitTicker.C:
			// 定时提交用于兜底，避免低流量场景下 offset 长时间不落盘。
			if !c.opts.EnableAutoCommit && processed > 0 {
				if err := c.commitPending(pendingOffsets); err != nil && !isIgnorableCommitError(err) {
					return err
				}
				pendingOffsets = make(map[string]TopicOffset)
				processed = 0
			}
		default:
		}

		event := c.raw.Poll(int(c.opts.PollTimeout.Milliseconds()))
		if event == nil {
			continue
		}

		switch e := event.(type) {
		case *kafka.Message:
			msg := fromKafkaMessage(e)
			if err := handler(ctx, &msg); err != nil {
				return err
			}

			if c.opts.EnableAutoCommit {
				continue
			}

			offset := TopicOffset{
				Topic:     *e.TopicPartition.Topic,
				Partition: e.TopicPartition.Partition,
				Offset:    e.TopicPartition.Offset + 1,
			}
			pendingOffsets[offsetKey(offset)] = offset
			processed++

			if processed >= c.opts.CommitBatchSize {
				// 达到批量阈值后提交，减少频繁 commit 带来的额外开销。
				if err := c.commitPending(pendingOffsets); err != nil && !isIgnorableCommitError(err) {
					return err
				}
				pendingOffsets = make(map[string]TopicOffset)
				processed = 0
			}
		case kafka.AssignedPartitions:
			if err := c.raw.Assign(e.Partitions); err != nil {
				return fmt.Errorf("assign partitions: %w", err)
			}
		case kafka.RevokedPartitions:
			if !c.opts.EnableAutoCommit {
				if err := c.commitPending(pendingOffsets); err != nil && !isIgnorableCommitError(err) {
					return err
				}
				pendingOffsets = make(map[string]TopicOffset)
				processed = 0
			}
			c.raw.Unassign()
		case kafka.Error:
			if e.IsFatal() {
				return fmt.Errorf("fatal kafka consumer error: %w", e)
			}
		}
	}
}

// Commit
//
//	@Description: 手动提交一组 offset
//	@receiver c
//	@param ctx
//	@param offsets
//	@return error
func (c *Consumer) Commit(_ context.Context, offsets ...TopicOffset) error {
	if len(offsets) == 0 {
		return nil
	}

	partitions := make([]kafka.TopicPartition, 0, len(offsets))
	for _, offset := range offsets {
		topic := offset.Topic
		partitions = append(partitions, kafka.TopicPartition{
			Topic:     &topic,
			Partition: offset.Partition,
			Offset:    offset.Offset,
		})
	}

	if _, err := c.raw.CommitOffsets(partitions); err != nil {
		return fmt.Errorf("commit kafka offsets: %w", err)
	}
	return nil
}

// Close
//
//	@Description: 关闭 Consumer
//	@receiver c
//	@return error
func (c *Consumer) Close() error {
	var err error
	c.closeOnce.Do(func() {
		err = c.raw.Close()
	})
	return err
}

// commitPending
//
//	@Description: 提交当前缓存的待提交 offset
//	@receiver c
//	@param pending
//	@return error
func (c *Consumer) commitPending(pending map[string]TopicOffset) error {
	if len(pending) == 0 {
		return nil
	}

	offsets := make([]TopicOffset, 0, len(pending))
	for _, offset := range pending {
		offsets = append(offsets, offset)
	}

	return c.Commit(context.Background(), offsets...)
}

// fromKafkaMessage
//
//	@Description: 将底层 Kafka Message 转换为模块内统一消息结构
//	@param msg
//	@return Message
func fromKafkaMessage(msg *kafka.Message) Message {
	result := Message{
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: make(map[string]string, len(msg.Headers)),
	}

	if msg.TopicPartition.Topic != nil {
		result.Topic = *msg.TopicPartition.Topic
	}

	for _, header := range msg.Headers {
		result.Headers[header.Key] = string(header.Value)
	}

	return result
}

// offsetKey
//
//	@Description: 生成 partition 维度的 offset 索引键
//	@param offset
//	@return string
func offsetKey(offset TopicOffset) string {
	return fmt.Sprintf("%s:%d", offset.Topic, offset.Partition)
}

func isIgnorableCommitError(err error) bool {
	if err == nil {
		return false
	}

	var kafkaErr kafka.Error
	if !errors.As(err, &kafkaErr) {
		return false
	}

	switch kafkaErr.Code() {
	case kafka.ErrUnknownMemberID, kafka.ErrIllegalGeneration, kafka.ErrState:
		return true
	default:
		return false
	}
}
