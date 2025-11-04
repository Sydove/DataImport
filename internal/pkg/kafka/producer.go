package kafka

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func (k *KafkaClient) Producer(topic string, message string) error {
	err := k.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte(message),
	}, nil)
	if err != nil {
		log.Printf("Produce failed: %v\n", err)
		return err
	}
	return nil
}
