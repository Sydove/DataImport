package kafka

import (
	"log"
)

func (k *KafkaClient) Consumer(topic string) error {
	err := k.consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		return err
	}

	for {
		msg, err := k.consumer.ReadMessage(-1)
		if err == nil {
			log.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			return err
		}
	}

	return nil
}
