package main

import (
	_ "DataImport/internal/pkg/config"
	"DataImport/internal/pkg/kafka"
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	kafkaGo "github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	// 1. 创建 Kafka 客户端
	client, err := kafka.NewClient()
	if err != nil {
		fmt.Printf("❌ 初始化 Kafka 客户端失败: %v\n", err)
		panic(err)
	}
	defer client.Close()

	// 2. 获取集群信息
	fmt.Println("═════════ 集群信息 ═════════")
	_, err = client.GetClusterMetadata()
	if err != nil {
		fmt.Printf("❌ 获取集群信息失败: %v\n", err)
	}

	// 3. 创建 Topic（如果需要）
	// err = client.CreateTopic(kafka.TopicConfig{
	//     Name:              "this_topic",
	//     NumPartitions:     3,
	//     ReplicationFactor: 3,
	// })
	// if err != nil {
	//     fmt.Printf("❌ 创建 Topic 失败: %v\n", err)
	// }

	wg := sync.WaitGroup{}
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	// 4. 启动 Producer
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runProducer(client); err != nil {
			fmt.Printf("❌ Producer 错误: %v\n", err)
		}
	}()

	// 5. 启动多个 Consumer
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()
			if err := runConsumer(client, consumerID); err != nil {
				fmt.Printf("❌ Consumer %d 错误: %v\n", consumerID, err)
			}
		}(i)
	}

	// 6. 等待退出信号
	<-signalChan
	fmt.Println("\n🛑 收到退出信号，正在关闭...")
}

// runProducer 运行 Producer
func runProducer(client *kafka.Client) error {
	// 创建自定义配置的 Producer
	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		CompressionType: "snappy",
		BatchSize:       51200,
		LingerMs:        10,
		Acks:            "all",
		MaxPending:      10000, // 背压阈值
	})
	if err != nil {
		return err
	}
	defer producer.Close()

	topicName := "this_topic"
	messageCount := 101

	fmt.Printf("🚀 开始发送 %d 条消息...\n", messageCount)
	startTime := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	wg := sync.WaitGroup{}

	// 发送消息
	for i := 0; i <= 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			timestamp := time.Now().UnixNano()
			messageValue := fmt.Sprintf("hello world %d", timestamp)

			// 使用带背压控制的发送
			err := producer.ProduceWithBackpressure(ctx, &kafkaGo.Message{
				TopicPartition: kafkaGo.TopicPartition{
					Topic:     &topicName,
					Partition: kafkaGo.PartitionAny,
				},
				Value: []byte(messageValue),
			})

			if err != nil {
				fmt.Printf("⚠️ 发送失败 [msg-%d]: %v\n", i, err)
			}
		}(i)
	}

	wg.Wait()

	submitDuration := time.Since(startTime)
	sent, _, _, backpressureHits := producer.GetStats()

	fmt.Printf("📝 所有消息已提交到队列，实际提交: %d 条，耗时: %v，背压触发: %d 次\n",
		sent, submitDuration, backpressureHits)

	// 等待所有消息发送完成
	fmt.Println("⏳ 等待所有消息确认...")
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer waitCancel()

	if err := producer.WaitForCompletion(waitCtx); err != nil {
		return fmt.Errorf("发送失败: %w", err)
	}

	totalDuration := time.Since(startTime)
	fmt.Printf("📈 总耗时: %v (提交: %v, 确认: %v)\n",
		totalDuration, submitDuration, totalDuration-submitDuration)

	return nil
}

// runConsumer 运行 Consumer
func runConsumer(client *kafka.Client, consumerID int) error {
	consumer, err := client.CreateConsumer(kafka.ConsumerConfig{
		GroupID:         "number_one",
		Topics:          []string{"this_topic"},
		AutoOffsetReset: "earliest",
		CommitBatchSize: 10,
	}, consumerID)
	if err != nil {
		return err
	}
	defer consumer.Close()

	// 定义消息处理函数
	handler := func(msg *kafkaGo.Message) error {
		// 这里可以添加自定义的消息处理逻辑
		// fmt.Printf("处理消息: %s\n", string(msg.Value))
		return nil
	}

	// 开始消费
	consumer.StartConsuming(consumerID, handler)

	return nil
}
