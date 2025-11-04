package kafka

import "testing"

func TestNewKafkaClient(t *testing.T) {
	kafkaClient, err := NewKafkaClient("192.168.31.164:30092")
	if err != nil {
		t.Errorf("NewKafkaClient() returned error: %v", err)
	}
	if kafkaClient == nil {
		t.Errorf("NewKafkaClient() returned nil")
	}

}
