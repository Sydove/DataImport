package kafka

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaClient struct {
	producer *kafka.Producer
	consumer *kafka.Consumer
}

func NewKafkaClient(bootstrapServers string) (*KafkaClient, error) {
	// 初始化生产者
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}

	// 初始化消费者
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          "test-consumer-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}

	return &KafkaClient{
		producer: p,
		consumer: c,
	}, nil
}

func (k *KafkaClient) Close() {
	if k.producer != nil {
		k.producer.Close()
	}
	if k.consumer != nil {
		k.consumer.Close()
	}
}

func (k *KafkaClient) CreateTopic(topic string, numPartitions int, replicationFactor int) error {
	// Kafka-go库不直接支持创建主题，你需要使用Kafka自带的脚本或API来完成
	// 这里只是一个占位符
	log.Println("Creating topic:", topic)
	return nil
}

func (k *KafkaClient) Produce(topic string, message string) error {
	return k.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte(message),
	}, nil)
}

func (k *KafkaClient) Consume(topic string) error {
	err := k.consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		return err
	}

	for {
		msg, err := k.consumer.ReadMessage(-1)
		if err == nil {
			log.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		}
	}

	return nil
}
