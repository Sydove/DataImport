package main

import (
	_ "DataImport/internal/pkg/config"
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/spf13/viper"
)

type KafkaClient struct {
	producer    *kafka.Producer
	adminClient *kafka.AdminClient
	consumer    []*kafka.Consumer
}

// ProducerTracker 用于跟踪 Producer 发送状态
type ProducerTracker struct {
	sentCount       int64 // 已发送消息数
	successCount    int64 // 成功确认数
	failedCount     int64 // 失败数
	maxPending      int64 // 最大待确认消息数（背压阈值）
	backpressureHit int64 // 背压触发次数
	mu              sync.Mutex
	failedMsgs      []FailedMessage // 失败的消息详情
}

type FailedMessage struct {
	Value string
	Error error
	Time  time.Time
}

type Message struct {
	ID   string
	Host string
	Port string
}

type ConfigCategory int

const (
	AdminClient ConfigCategory = 1
	Product     ConfigCategory = 2
	Consumer    ConfigCategory = 3
)

func getKafkaConfig(category ConfigCategory) *kafka.ConfigMap {
	kafkaAddress := viper.GetStringSlice("kafka.addr")
	config := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(kafkaAddress, ","),
	}
	switch category {
	case AdminClient:
		return config
	case Product:
		config.SetKey("compression.type", "snappy")
		config.SetKey("batch.size", 51200)
		config.SetKey("acks", "all")
		config.SetKey("linger.ms", 10)
		config.SetKey("queue.buffering.max.messages", 2000000)
		config.SetKey("queue.buffering.max.kbytes", 1048576)
		config.SetKey("enable.idempotence", true)
	case Consumer:
		config.SetKey("go.events.channel.enable", true)        // 启用事件通道
		config.SetKey("go.application.rebalance.enable", true) // 消费者离开重分配
	}
	return config
}

// NewKafkaClient
//
//	@Description: 创建kafka对象 consumer/producer/adminClient
//	@return *KafkaClient
//	@return error
func NewKafkaClient() (*KafkaClient, error) {
	clientConfig := getKafkaConfig(AdminClient)
	admin, err := kafka.NewAdminClient(clientConfig)
	if err != nil {
		return nil, err
	}

	productConfig := getKafkaConfig(Product)
	product, err := kafka.NewProducer(productConfig)
	if err != nil {
		return nil, err
	}

	return &KafkaClient{
		adminClient: admin,
		producer:    product,
	}, nil
}

func (k *KafkaClient) CreateTopic(topicName string) error {
	_, err := k.adminClient.CreateTopics(
		context.Background(),
		[]kafka.TopicSpecification{{
			Topic:             topicName,
			NumPartitions:     3,
			ReplicationFactor: 3,
		}},
	)
	return err
}

// GetClusterMsg
//
//	@Description: 获取集群的信息
//	@receiver k
//	@return error
func (k *KafkaClient) GetClusterMsg() ([]Message, error) {
	meta, _ := k.adminClient.GetMetadata(nil, true, 5000)
	fmt.Printf("Broker 数量: %d\n", len(meta.Brokers))

	results := make([]Message, 0)
	for _, b := range meta.Brokers {
		results = append(results, Message{
			ID:   fmt.Sprintf("%d", b.ID),
			Host: b.Host,
			Port: fmt.Sprintf("%d", b.Port),
		})
	}
	for t := range meta.Topics {
		fmt.Printf("Topic: %s\n", t)
	}
	return results, nil
}

// DeleteTopic
//
//	@Description: 删除topic
//	@receiver k
//	@param topicName
//	@return error
func (k *KafkaClient) DeleteTopic(topicName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	_, err := k.adminClient.DeleteTopics(ctx, []string{topicName}, kafka.SetAdminOperationTimeout(5000))
	if err != nil {
		return err
	}
	return nil
}

// CreateConsumer
//
//	@Description: 创建一个kafka consumer
//	@receiver k
//	@param topicNames
//	@param groupID
//	@param offset
//	@return *kafka.Consumer
//	@return error
func (k *KafkaClient) CreateConsumer(topicNames []string, groupID string, consumerID int, offset string) (*kafka.Consumer, error) {
	consumerConfig := getKafkaConfig(Consumer)
	consumerConfig.SetKey("group.id", groupID)
	consumerConfig.SetKey("client.id", fmt.Sprintf("consumer-%d", consumerID))
	consumerConfig.SetKey("auto.offset.reset", offset) //earliest或latest

	consumer, err := kafka.NewConsumer(consumerConfig)
	if err != nil {
		return nil, err
	}
	err = consumer.SubscribeTopics(topicNames, nil)
	if err != nil {
		return nil, err
	}
	k.consumer = append(k.consumer, consumer)
	return consumer, nil
}

// Close
//
//	@Description: 关闭所有相关的资源对象
//	@receiver k
//	@return error
func (k *KafkaClient) Close() error {
	k.producer.Close()
	k.adminClient.Close()
	for _, consumer := range k.consumer {
		consumer.Close()
	}
	return nil
}

// handleDeliveryReports 处理消息发送的 ACK
func handleDeliveryReports(producer *kafka.Producer, tracker *ProducerTracker) {
	for e := range producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				// 发送失败
				atomic.AddInt64(&tracker.failedCount, 1)

				tracker.mu.Lock()
				tracker.failedMsgs = append(tracker.failedMsgs, FailedMessage{
					Value: string(ev.Value),
					Error: ev.TopicPartition.Error,
					Time:  time.Now(),
				})
				tracker.mu.Unlock()

				fmt.Printf("❌ 消息发送失败: %v\n", ev.TopicPartition.Error)
			} else {
				// 发送成功
				atomic.AddInt64(&tracker.successCount, 1)
			}
		case kafka.Error:
			fmt.Printf("⚠️ Kafka Error: %v\n", ev)
		}
	}
	fmt.Println("📢 Delivery report handler 退出")
}

// produceWithBackpressure 带背压控制的消息发送
func produceWithBackpressure(ctx context.Context, producer *kafka.Producer, tracker *ProducerTracker, msg *kafka.Message) error {
	// 检查待确认消息数量
	for {
		sent := atomic.LoadInt64(&tracker.sentCount)
		success := atomic.LoadInt64(&tracker.successCount)
		failed := atomic.LoadInt64(&tracker.failedCount)
		pending := sent - (success + failed)
		queueLen := int64(producer.Len())

		// 背压控制：如果待确认消息数超过阈值，等待
		if pending >= tracker.maxPending || queueLen >= tracker.maxPending {
			atomic.AddInt64(&tracker.backpressureHit, 1)

			// 只在第一次触发背压时打印
			hitCount := atomic.LoadInt64(&tracker.backpressureHit)
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
	err := producer.Produce(msg, nil)
	if err != nil {
		return fmt.Errorf("提交消息失败: %w", err)
	}

	// 发送成功，计数 +1
	atomic.AddInt64(&tracker.sentCount, 1)
	return nil
}

// waitForCompletion 等待所有消息发送完成
func waitForCompletion(ctx context.Context, producer *kafka.Producer, tracker *ProducerTracker) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	startTime := time.Now()
	lastReported := int64(0)

	for {
		sent := atomic.LoadInt64(&tracker.sentCount)
		success := atomic.LoadInt64(&tracker.successCount)
		failed := atomic.LoadInt64(&tracker.failedCount)
		completed := success + failed
		queueLen := producer.Len()

		// 检查是否全部完成
		if sent > 0 && completed == sent && queueLen == 0 {
			elapsed := time.Since(startTime)
			backpressureHits := atomic.LoadInt64(&tracker.backpressureHit)

			fmt.Printf("\n✅ 全部完成! 总数: %d, 成功: %d, 失败: %d, 耗时: %v\n",
				sent, success, failed, elapsed)
			fmt.Printf("📊 背压统计: 触发 %d 次\n", backpressureHits)

			// 如果有失败的消息，打印详情
			if failed > 0 {
				tracker.mu.Lock()
				fmt.Printf("\n⚠️ 失败消息详情 (共 %d 条):\n", len(tracker.failedMsgs))
				for i, msg := range tracker.failedMsgs {
					if i >= 10 {
						fmt.Printf("... 还有 %d 条失败消息\n", len(tracker.failedMsgs)-10)
						break
					}
					fmt.Printf("  %d. 消息: %s, 错误: %v\n", i+1, msg.Value, msg.Error)
				}
				tracker.mu.Unlock()
			}

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

				backpressureHits := atomic.LoadInt64(&tracker.backpressureHit)

				fmt.Printf("📊 进度: %.1f%% (%d/%d) | 成功: %d, 失败: %d | 待确认: %d | 队列: %d | 背压: %d 次 | 速率: %.0f msg/s | 耗时: %v | ETA: %v\n",
					progress, completed, sent, success, failed, pending, queueLen, backpressureHits, rate,
					elapsed.Round(time.Second), eta.Round(time.Second))

				lastReported = completed
			}
		}
	}
}

func createProducer(kafkaOperator *KafkaClient) error {
	// 创建 tracker，设置最大待确认消息数为 10000
	// 可以根据实际情况调整这个值
	tracker := &ProducerTracker{
		maxPending: 10000, // 🔥 背压阈值：最多允许 10000 条消息待确认
		failedMsgs: make([]FailedMessage, 0),
	}

	// 启动 ACK 处理 goroutine
	go handleDeliveryReports(kafkaOperator.producer, tracker)

	topicName := "this_topic"
	messageCount := 101 // 0 到 100，共 101 条

	fmt.Printf("🚀 开始发送 %d 条消息（背压阈值: %d）...\n", messageCount, tracker.maxPending)
	startTime := time.Now()

	// 创建带超时的上下文（用于背压控制）
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	wg := sync.WaitGroup{}

	for i := 0; i <= 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			timestamp := time.Now().UnixNano()
			messageValue := fmt.Sprintf("hello world %d", timestamp)

			// 🔥 使用带背压控制的发送函数
			err := produceWithBackpressure(ctx, kafkaOperator.producer, tracker, &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topicName,
					Partition: kafka.PartitionAny,
				},
				Value: []byte(messageValue),
			})

			if err != nil {
				fmt.Printf("⚠️ 发送失败 [msg-%d]: %v\n", i, err)
			}
		}(i)
	}

	// 等待所有 goroutine 完成消息提交
	wg.Wait()

	submitDuration := time.Since(startTime)
	sent := atomic.LoadInt64(&tracker.sentCount)
	backpressureHits := atomic.LoadInt64(&tracker.backpressureHit)

	fmt.Printf("📝 所有消息已提交到队列，实际提交: %d 条，耗时: %v，背压触发: %d 次\n",
		sent, submitDuration, backpressureHits)

	// 等待所有消息发送完成，最多等待 5 分钟
	fmt.Println("⏳ 等待所有消息确认...")
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer waitCancel()

	if err := waitForCompletion(waitCtx, kafkaOperator.producer, tracker); err != nil {
		return fmt.Errorf("发送失败: %w", err)
	}

	totalDuration := time.Since(startTime)
	fmt.Printf("📈 总耗时: %v (提交: %v, 确认: %v)\n",
		totalDuration, submitDuration, totalDuration-submitDuration)

	return nil
}

func createConsumer(kafkaOperator *KafkaClient, consumerId int) {
	consumer, err := kafkaOperator.CreateConsumer([]string{"this_topic"}, "number_one", consumerId, "earliest")
	if err != nil {
		panic(err)
	}

	commitBatchSize := 10
	messageCount := 0
	offsetMap := make(map[kafka.TopicPartition]kafka.Offset)

	for ev := range consumer.Events() { // 从 channel 中读取事件
		switch e := ev.(type) {

		case *kafka.Message:
			fmt.Printf("Consumer_%d got msg: %s, partition=%d offset=%d\n",
				consumerId, string(e.Value), e.TopicPartition.Partition, e.TopicPartition.Offset)

			// 保存该分区最新 offset
			tp := kafka.TopicPartition{
				Topic:     e.TopicPartition.Topic,
				Partition: e.TopicPartition.Partition,
			}
			offsetMap[tp] = e.TopicPartition.Offset + 1 // 下一条消息offset(Kafka的要求)

			messageCount++

			// 达到批量数量，提交一次
			if messageCount >= commitBatchSize {
				var offsets []kafka.TopicPartition

				for tp, off := range offsetMap {
					tp.Offset = off
					offsets = append(offsets, tp)
				}

				_, err := consumer.CommitOffsets(offsets)
				if err != nil {
					fmt.Println("❌ 批量提交失败:", err)
				} else {
					fmt.Println("✅ 批量提交成功:", offsets)
				}

				// 清空计数
				messageCount = 0
			}

		// 分区分配 rebalance
		case kafka.AssignedPartitions:
			fmt.Println("📌 分配分区:", e.Partitions)
			consumer.Assign(e.Partitions)

		case kafka.RevokedPartitions:
			fmt.Println("📌 回收分区")
			consumer.Unassign()

		case kafka.Error:
			fmt.Println("❌ Kafka Error:", e)
		}
	}
}

func main() {
	kafkaOperator, err := NewKafkaClient()
	if err != nil {
		fmt.Println("❌ init kafka client error:", err)
		panic(err)
	}
	defer kafkaOperator.Close()

	//if err := kafkaOperator.CreateTopic("this_topic"); err != nil {
	//	fmt.Println("❌ create topic error:", err)
	//}

	wg := sync.WaitGroup{}
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	// 生产消息
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := createProducer(kafkaOperator); err != nil {
			fmt.Printf("❌ Producer 错误: %v\n", err)
		}
	}()

	// 消费消息
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			createConsumer(kafkaOperator, i)
		}(i)
	}

	// 等待信号
	<-signalChan
	fmt.Println("\n🛑 收到退出信号，正在关闭...")
}
