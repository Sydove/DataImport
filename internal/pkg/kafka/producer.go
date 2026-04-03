package newkafka

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer struct {
	raw       *kafka.Producer
	opts      ProducerOptions
	closeOnce sync.Once

	sentCount        int64
	successCount     int64
	failedCount      int64
	fatalErrorCount  int64
	backpressureHits int64
	lastFatalErr     atomic.Value
}

// newProducer
//
//	@Description: 创建底层 Kafka Producer 封装
//	@param baseConfig
//	@param opts
//	@return *Producer
//	@return error
func newProducer(baseConfig *kafka.ConfigMap, opts ProducerOptions) (*Producer, error) {
	opts = opts.withDefaults()

	baseConfig.SetKey("compression.type", opts.CompressionType)
	baseConfig.SetKey("batch.size", opts.BatchSize)
	baseConfig.SetKey("linger.ms", opts.LingerMs)
	baseConfig.SetKey("acks", opts.Acks)
	baseConfig.SetKey("queue.buffering.max.messages", opts.QueueMaxMessages)
	baseConfig.SetKey("queue.buffering.max.kbytes", opts.QueueMaxKBytes)
	baseConfig.SetKey("queue.buffering.backpressure.threshold", opts.BackpressureThreshold)
	baseConfig.SetKey("enable.idempotence", opts.EnableIdempotence)
	baseConfig.SetKey("request.timeout.ms", opts.RequestTimeoutMs)
	baseConfig.SetKey("message.timeout.ms", opts.MessageTimeoutMs)
	baseConfig.SetKey("socket.timeout.ms", opts.SocketTimeoutMs)
	baseConfig.SetKey("retries", opts.Retries)
	baseConfig.SetKey("retry.backoff.ms", opts.RetryBackoffMs)

	rawProducer, err := kafka.NewProducer(baseConfig)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}

	producer := &Producer{
		raw:  rawProducer,
		opts: opts,
	}

	go producer.handleDeliveryReports()
	return producer, nil
}

// Publish
//
//	@Description: 发送单条消息，并在发送前执行本地背压控制
//	@receiver p
//	@param ctx
//	@param msg
//	@return error
func (p *Producer) Publish(ctx context.Context, msg Message) error {
	if err := msg.validate(); err != nil {
		return err
	}

	for {
		stats := p.Stats()
		if stats.PendingCount < p.opts.MaxPending && stats.QueueLen < p.opts.MaxPending {
			break
		}

		atomic.AddInt64(&p.backpressureHits, 1)
		// 当本地待确认消息或发送队列过大时，短暂等待下游追平。
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(p.opts.BackpressureWait):
		}
	}

	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &msg.Topic,
		},
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: toKafkaHeaders(msg.Headers),
	}
	if msg.Partition != nil {
		kafkaMsg.TopicPartition.Partition = *msg.Partition
	}

	if err := p.raw.Produce(kafkaMsg, nil); err != nil {
		return fmt.Errorf("publish kafka message: %w", err)
	}

	p.markSent()
	return nil
}

// PublishBatch
//
//	@Description: 顺序发送一组消息，任一消息失败则立即返回
//	@receiver p
//	@param ctx
//	@param msgs
//	@return error
func (p *Producer) PublishBatch(ctx context.Context, msgs []Message) error {
	for _, msg := range msgs {
		if err := p.Publish(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

// Flush
//
//	@Description: 等待 Producer 将本地缓冲区中的消息尽量发送完成
//	@receiver p
//	@param ctx
//	@return error
func (p *Producer) Flush(ctx context.Context) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		stats := p.Stats()
		if stats.PendingCount == 0 && stats.QueueLen == 0 {
			return nil
		}

		if remaining := p.raw.Flush(500); remaining == 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Stats
//
//	@Description: 获取当前 Producer 的发送统计信息
//	@receiver p
//	@return ProducerStats
func (p *Producer) Stats() ProducerStats {
	sent := atomic.LoadInt64(&p.sentCount)
	success := atomic.LoadInt64(&p.successCount)
	failed := atomic.LoadInt64(&p.failedCount)
	pending := sent - (success + failed)
	if pending < 0 {
		pending = 0
	}

	var queueLen int64
	if p.raw != nil {
		queueLen = int64(p.raw.Len())
	}

	return ProducerStats{
		SentCount:        sent,
		SuccessCount:     success,
		FailedCount:      failed,
		FatalErrorCount:  atomic.LoadInt64(&p.fatalErrorCount),
		PendingCount:     pending,
		QueueLen:         queueLen,
		BackpressureHits: atomic.LoadInt64(&p.backpressureHits),
	}
}

// LastFatalError
//
//	@Description: 返回最近一次收到的 fatal Kafka error
//	@receiver p
//	@return error
func (p *Producer) LastFatalError() error {
	lastErr := p.lastFatalErr.Load()
	if lastErr == nil {
		return nil
	}
	return lastErr.(error)
}

// Close
//
//	@Description: 关闭 Producer
//	@receiver p
//	@return error
func (p *Producer) Close() error {
	p.closeOnce.Do(func() {
		if p.raw != nil {
			p.raw.Close()
		}
	})
	return nil
}

// handleDeliveryReports
//
//	@Description:  Kafka 生产者的异步消息确认
//	@receiver p
func (p *Producer) handleDeliveryReports() {
	for event := range p.raw.Events() {
		switch e := event.(type) {
		case *kafka.Message:
			p.markDelivery(e.TopicPartition.Error)
		case kafka.Error:
			if e.IsFatal() {
				p.markFatalError(e)
			}
		}
	}
}

// markSent
//
//	@Description: 记录一条消息已被 Producer 接受
//	@receiver p
func (p *Producer) markSent() {
	atomic.AddInt64(&p.sentCount, 1)
}

// markDelivery
//
//	@Description: 根据 delivery report 更新成功或失败统计
//	@receiver p
//	@param err
func (p *Producer) markDelivery(err error) {
	if err != nil {
		atomic.AddInt64(&p.failedCount, 1)
		return
	}
	atomic.AddInt64(&p.successCount, 1)
}

// markFatalError
//
//	@Description: 记录 producer 级别的 fatal 错误，不参与消息发送失败统计
//	@receiver p
//	@param err
func (p *Producer) markFatalError(err error) {
	if err == nil {
		return
	}
	atomic.AddInt64(&p.fatalErrorCount, 1)
	p.lastFatalErr.Store(err)
}

// toKafkaHeaders
//
//	@Description: 将 map 结构的 headers 转换为 Kafka 原生 headers
//	@param headers
//	@return []kafka.Header
func toKafkaHeaders(headers map[string]string) []kafka.Header {
	if len(headers) == 0 {
		return nil
	}

	result := make([]kafka.Header, 0, len(headers))
	for key, value := range headers {
		result = append(result, kafka.Header{
			Key:   key,
			Value: []byte(value),
		})
	}
	return result
}
