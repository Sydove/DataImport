package kafka

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Producer Kafka Producer 包装
type Producer struct {
	producer *kafka.Producer
	tracker  *ProducerTracker
}

// NewProducer 创建 Producer
func NewProducer(config ProducerConfig) (*Producer, error) {
	kafkaConfig := getBaseConfig()

	// 设置 Producer 配置
	kafkaConfig.SetKey("compression.type", config.CompressionType)
	kafkaConfig.SetKey("batch.size", config.BatchSize)
	kafkaConfig.SetKey("linger.ms", config.LingerMs)
	kafkaConfig.SetKey("acks", config.Acks)
	kafkaConfig.SetKey("queue.buffering.max.messages", 2000000)
	kafkaConfig.SetKey("queue.buffering.max.kbytes", 1048576)
	kafkaConfig.SetKey("enable.idempotence", true)

	producer, err := kafka.NewProducer(kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("创建 Producer 失败: %w", err)
	}

	p := &Producer{
		producer: producer,
		tracker: &ProducerTracker{
			maxPending: config.MaxPending,
		},
	}

	// 启动 ACK 处理
	go p.handleDeliveryReports()

	return p, nil
}

// handleDeliveryReports 处理消息发送的 ACK
func (p *Producer) handleDeliveryReports() {
	for e := range p.producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				// 发送失败
				atomic.AddInt64(&p.tracker.failedCount, 1)
				fmt.Printf("❌ 消息发送失败: %v\n", ev.TopicPartition.Error)
			} else {
				// 发送成功
				atomic.AddInt64(&p.tracker.successCount, 1)
			}
		case kafka.Error:
			fmt.Printf("⚠️ Kafka Error: %v\n", ev)
		}
	}
	fmt.Println("📢 Delivery report handler 退出")
}

// ProduceWithBackpressure 带背压控制的消息发送
func (p *Producer) ProduceWithBackpressure(ctx context.Context, msg *kafka.Message) error {
	// 检查待确认消息数量
	for {
		sent := atomic.LoadInt64(&p.tracker.sentCount)
		success := atomic.LoadInt64(&p.tracker.successCount)
		failed := atomic.LoadInt64(&p.tracker.failedCount)
		pending := sent - (success + failed)
		queueLen := int64(p.producer.Len())

		// 背压控制：如果待确认消息数超过阈值，等待
		if pending >= p.tracker.maxPending || queueLen >= p.tracker.maxPending {
			atomic.AddInt64(&p.tracker.backpressureHit, 1)

			// 只在第一次触发背压时打印
			hitCount := atomic.LoadInt64(&p.tracker.backpressureHit)
			if hitCount == 1 || hitCount%100 == 0 {
				fmt.Printf("🔴 背压触发 (第 %d 次): 待确认 %d 条，队列 %d 条，等待处理...\n",
					hitCount, pending, queueLen)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Millisecond):
				// 等待一段时间后重试
				continue
			}
		}

		// 待确认数量在阈值内，可以发送
		break
	}

	// 发送消息
	err := p.producer.Produce(msg, nil)
	if err != nil {
		return fmt.Errorf("提交消息失败: %w", err)
	}

	// 发送成功，计数 +1
	atomic.AddInt64(&p.tracker.sentCount, 1)
	return nil
}

// Produce 普通发送（无背压控制）
func (p *Producer) Produce(msg *kafka.Message) error {
	err := p.producer.Produce(msg, nil)
	if err != nil {
		return err
	}
	atomic.AddInt64(&p.tracker.sentCount, 1)
	return nil
}

// WaitForCompletion 等待所有消息发送完成
func (p *Producer) WaitForCompletion(ctx context.Context) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	startTime := time.Now()
	lastReported := int64(0)

	for {
		sent := atomic.LoadInt64(&p.tracker.sentCount)
		success := atomic.LoadInt64(&p.tracker.successCount)
		failed := atomic.LoadInt64(&p.tracker.failedCount)
		completed := success + failed
		queueLen := p.producer.Len()

		// 检查是否全部完成
		if sent > 0 && completed == sent && queueLen == 0 {
			elapsed := time.Since(startTime)
			backpressureHits := atomic.LoadInt64(&p.tracker.backpressureHit)

			fmt.Printf("\n✅ 全部完成! 总数: %d, 成功: %d, 失败: %d, 耗时: %v\n",
				sent, success, failed, elapsed)
			fmt.Printf("📊 背压统计: 触发 %d 次\n", backpressureHits)

			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("⏱️ 超时: 发送 %d 条，完成 %d 条 (成功 %d, 失败 %d), 队列中 %d 条",
				sent, completed, success, failed, queueLen)

		case <-ticker.C:
			// 只在进度变化时打印
			if completed != lastReported {
				pending := sent - completed
				progress := float64(completed) / float64(sent) * 100
				elapsed := time.Since(startTime)

				// 计算速率
				rate := float64(completed) / elapsed.Seconds()

				// 估算剩余时间
				var eta time.Duration
				if rate > 0 {
					eta = time.Duration(float64(sent-completed)/rate) * time.Second
				}

				backpressureHits := atomic.LoadInt64(&p.tracker.backpressureHit)

				fmt.Printf("📊 进度: %.1f%% (%d/%d) | 成功: %d, 失败: %d | 待确认: %d | 队列: %d | 背压: %d 次 | 速率: %.0f msg/s | 耗时: %v | ETA: %v\n",
					progress, completed, sent, success, failed, pending, queueLen, backpressureHits, rate,
					elapsed.Round(time.Second), eta.Round(time.Second))

				lastReported = completed
			}
		}
	}
}

// GetStats 获取统计信息
func (p *Producer) GetStats() (sent, success, failed, backpressureHits int64) {
	return p.tracker.GetStats()
}

// Close 关闭 Producer
func (p *Producer) Close() {
	p.producer.Close()
}
