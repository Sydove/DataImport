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

type consumeTask struct {
	msg    Message
	offset TopicOffset
}

type taskResult struct {
	offset TopicOffset
	err    error
}

type partitionProgress struct {
	nextOffset kafka.Offset
	completed  map[kafka.Offset]struct{}
	inFlight   int
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

	jobs := make(chan consumeTask, c.opts.WorkQueueSize)
	results := make(chan taskResult, c.opts.WorkQueueSize)

	var workerWG sync.WaitGroup
	// 启动工作池,获取消息,写入es
	for i := 0; i < c.opts.WorkerCount; i++ {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			for task := range jobs {
				taskCtx := ctx
				err := handler(taskCtx, &task.msg)
				select {
				// 执行结果写入到 results channel中
				case results <- taskResult{offset: task.offset, err: err}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	defer func() {
		// 关闭channel,等待工作池完成
		close(jobs)
		workerWG.Wait()
		close(results)
	}()
	// 跟踪每个分区的消费进度
	progress := make(map[string]*partitionProgress)
	// 存储已完成处理可以提交的偏移量
	readyOffsets := make(map[string]TopicOffset)
	// 记录已完成处理且可以提交的消息数量
	readyCount := 0
	// 记录当前正在处理中的消息数量
	inFlight := 0
	// 定时提交的偏移量
	commitTicker := time.NewTicker(c.opts.CommitInterval)
	defer commitTicker.Stop()

	for {
		// 统计消费结果
		for len(results) > 0 {
			result := <-results
			inFlight--
			if result.err != nil {
				// 需要返回错误吗?
				return result.err
			}
			if c.opts.EnableAutoCommit {
				continue
			}
			var advanced int
			advanced, readyOffsets = c.markCompleted(progress, readyOffsets, result.offset)
			readyCount += advanced
		}

		select {
		case <-ctx.Done():
			for inFlight > 0 {
				result := <-results
				inFlight--
				if result.err != nil && !errors.Is(result.err, context.Canceled) {
					return result.err
				}
				if c.opts.EnableAutoCommit {
					continue
				}
				var advanced int
				advanced, readyOffsets = c.markCompleted(progress, readyOffsets, result.offset)
				readyCount += advanced
			}
			if !c.opts.EnableAutoCommit {
				if err := c.commitPending(readyOffsets); err != nil && !isIgnorableCommitError(err) {
					return err
				}
			}
			return nil
		case result := <-results:
			inFlight--
			if result.err != nil {
				return result.err
			}
			if c.opts.EnableAutoCommit {
				continue
			}
			var advanced int
			advanced, readyOffsets = c.markCompleted(progress, readyOffsets, result.offset)
			readyCount += advanced
			if readyCount >= c.opts.CommitBatchSize {
				if err := c.commitPending(readyOffsets); err != nil && !isIgnorableCommitError(err) {
					return err
				}
				readyOffsets = make(map[string]TopicOffset)
				readyCount = 0
			}
		case <-commitTicker.C:
			if !c.opts.EnableAutoCommit && readyCount > 0 {
				if err := c.commitPending(readyOffsets); err != nil && !isIgnorableCommitError(err) {
					return err
				}
				readyOffsets = make(map[string]TopicOffset)
				readyCount = 0
			}
		default:
		}

		// 批量拉取数据
		batch, err := c.pollBatch(ctx)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			continue
		}

		for _, event := range batch {
			switch e := event.(type) {
			case *kafka.Message:
				msg := fromKafkaMessage(e)
				offset := TopicOffset{
					Topic:     *e.TopicPartition.Topic,
					Partition: e.TopicPartition.Partition,
					Offset:    e.TopicPartition.Offset + 1,
				}
				if !c.opts.EnableAutoCommit {
					c.registerInFlight(progress, offset)
				}
				select {
				case jobs <- consumeTask{msg: msg, offset: offset}:
					inFlight++
				case <-ctx.Done():
					return ctx.Err()
				}
			case kafka.AssignedPartitions:
				if err := c.raw.Assign(e.Partitions); err != nil {
					return fmt.Errorf("assign partitions: %w", err)
				}
			case kafka.RevokedPartitions:
				for inFlight > 0 {
					result := <-results
					inFlight--
					if result.err != nil && !errors.Is(result.err, context.Canceled) {
						return result.err
					}
					if c.opts.EnableAutoCommit {
						continue
					}
					var advanced int
					advanced, readyOffsets = c.markCompleted(progress, readyOffsets, result.offset)
					readyCount += advanced
				}
				if !c.opts.EnableAutoCommit {
					if err := c.commitPending(readyOffsets); err != nil && !isIgnorableCommitError(err) {
						return err
					}
					readyOffsets = make(map[string]TopicOffset)
					readyCount = 0
					progress = make(map[string]*partitionProgress)
				}
				c.raw.Unassign()
			case kafka.Error:
				if e.IsFatal() {
					return fmt.Errorf("fatal kafka consumer error: %w", e)
				}
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

func (c *Consumer) pollBatch(ctx context.Context) ([]kafka.Event, error) {
	batch := make([]kafka.Event, 0, c.opts.MaxPollRecords)
	deadline := time.Now().Add(c.opts.FetchMaxWait)

	for len(batch) < c.opts.MaxPollRecords {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		event := c.raw.Poll(int(c.opts.PollTimeout.Milliseconds()))
		if event != nil {
			batch = append(batch, event)
			continue
		}

		if len(batch) > 0 || time.Now().After(deadline) {
			break
		}
	}

	return batch, nil
}

func (c *Consumer) registerInFlight(progress map[string]*partitionProgress, offset TopicOffset) {
	key := offsetKey(offset)
	state, ok := progress[key]
	if !ok {
		state = &partitionProgress{
			nextOffset: offset.Offset,
			completed:  make(map[kafka.Offset]struct{}),
		}
		progress[key] = state
	}
	if state.nextOffset == 0 || offset.Offset < state.nextOffset {
		state.nextOffset = offset.Offset
	}
	state.inFlight++
}

// markCompleted
//
//	@Description: 标记已完成处理的消息,并更新可提交的偏移量
//	@receiver c
//	@param progress 跟踪每个分区的消费进度的映射
//	@param readyOffsets 可提交的偏移量映射
//	@param offset
//	@return int
//	@return map[string]TopicOffset
func (c *Consumer) markCompleted(progress map[string]*partitionProgress, ready map[string]TopicOffset, offset TopicOffset) (int, map[string]TopicOffset) {
	key := offsetKey(offset)
	state, ok := progress[key]
	if !ok {
		state = &partitionProgress{
			nextOffset: offset.Offset,
			completed:  make(map[kafka.Offset]struct{}),
		}
		progress[key] = state
	}
	state.completed[offset.Offset] = struct{}{}
	if state.inFlight > 0 {
		state.inFlight--
	}

	advanced := 0
	for {
		if _, ok := state.completed[state.nextOffset]; !ok {
			break
		}
		delete(state.completed, state.nextOffset)
		state.nextOffset++
		advanced++
	}

	if advanced > 0 {
		ready[key] = TopicOffset{
			Topic:     offset.Topic,
			Partition: offset.Partition,
			Offset:    state.nextOffset,
		}
	}

	return advanced, ready
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
