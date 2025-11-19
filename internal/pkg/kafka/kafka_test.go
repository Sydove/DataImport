package kafka

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestNewKafkaClient(t *testing.T) {
	brokers := [3]string{"192.168.31.181:9092", "192.168.31.182:9093", "192.168.31.183:9094"}
	group := "test-group"
	topic := "test-final"

	// 创建客户端
	client, err := NewKafkaClient(Config{
		BootstrapServers: brokers[0],
		ConsumerGroup:    group,
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动消费者
	err = client.StartConsuming(ctx, []string{topic}, func(msg *kafka.Message) error {
		fmt.Printf("Received message: key=%s, value=%s, partition=%d, offset=%d\n",
			string(msg.Key), string(msg.Value),
			msg.TopicPartition.Partition, msg.TopicPartition.Offset)
		return nil
	}, time.Second*5) // 每 5 秒提交一次 offset
	if err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}

	// 测试发送消息
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("hello kafka %d", i))
		if err := client.AsyncProduce(topic, key, value); err != nil {
			log.Printf("Failed to produce message: %v", err)
		} else {
			log.Printf("Produced message: key=%s value=%s", key, value)
		}
		time.Sleep(time.Millisecond * 200) // 模拟发送间隔
	}

	// 等待消费者接收消息
	log.Println("Waiting for consumer to receive messages...")
	time.Sleep(time.Second * 5)
	log.Println("Test finished.")

}
