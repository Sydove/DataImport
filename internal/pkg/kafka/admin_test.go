package newkafka

import (
	"context"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type fakeAdminClient struct {
	createResults [][]kafka.TopicResult
	deleteResults [][]kafka.TopicResult
	metadata      []*kafka.Metadata
	createCalls   int
	deleteCalls   int
	metadataCalls int
}

func (f *fakeAdminClient) CreateTopics(_ context.Context, _ []kafka.TopicSpecification, _ ...kafka.CreateTopicsAdminOption) ([]kafka.TopicResult, error) {
	if f.createCalls >= len(f.createResults) {
		return []kafka.TopicResult{{Error: kafka.NewError(kafka.ErrNoError, "", false)}}, nil
	}
	result := f.createResults[f.createCalls]
	f.createCalls++
	return result, nil
}

func (f *fakeAdminClient) DeleteTopics(_ context.Context, _ []string, _ ...kafka.DeleteTopicsAdminOption) ([]kafka.TopicResult, error) {
	if f.deleteCalls >= len(f.deleteResults) {
		return []kafka.TopicResult{{Error: kafka.NewError(kafka.ErrUnknownTopicOrPart, "", false)}}, nil
	}
	result := f.deleteResults[f.deleteCalls]
	f.deleteCalls++
	return result, nil
}

func (f *fakeAdminClient) GetMetadata(_ *string, _ bool, _ int) (*kafka.Metadata, error) {
	if f.metadataCalls >= len(f.metadata) {
		return &kafka.Metadata{Topics: map[string]kafka.TopicMetadata{}}, nil
	}
	result := f.metadata[f.metadataCalls]
	f.metadataCalls++
	return result, nil
}

func (f *fakeAdminClient) Close() {}

func TestAdminRecreateTopicRetriesCreateUntilDeletePropagates(t *testing.T) {
	oldInterval := topicOperationPollInterval
	topicOperationPollInterval = time.Millisecond
	t.Cleanup(func() {
		topicOperationPollInterval = oldInterval
	})

	admin := &Admin{
		raw: &fakeAdminClient{
			metadata: []*kafka.Metadata{
				{
					Topics: map[string]kafka.TopicMetadata{
						"full_load": {Error: kafka.NewError(kafka.ErrNoError, "", false)},
					},
				},
			},
			deleteResults: [][]kafka.TopicResult{
				{{Error: kafka.NewError(kafka.ErrNoError, "", false)}},
			},
			createResults: [][]kafka.TopicResult{
				{{Error: kafka.NewError(kafka.ErrTopicAlreadyExists, "", false)}},
				{{Error: kafka.NewError(kafka.ErrNoError, "", false)}},
			},
		},
		metadataTimeout: time.Second,
	}

	err := admin.RecreateTopic(context.Background(), TopicSpec{
		Name:              "full_load",
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, time.Second)
	if err != nil {
		t.Fatalf("recreate topic: %v", err)
	}

	fake := admin.raw.(*fakeAdminClient)
	if fake.deleteCalls != 1 {
		t.Fatalf("delete calls = %d, want 1", fake.deleteCalls)
	}
	if fake.createCalls != 2 {
		t.Fatalf("create calls = %d, want 2", fake.createCalls)
	}
}

func TestAdminRecreateTopicDeletesAfterStaleMetadataMiss(t *testing.T) {
	oldInterval := topicOperationPollInterval
	topicOperationPollInterval = time.Millisecond
	t.Cleanup(func() {
		topicOperationPollInterval = oldInterval
	})

	admin := &Admin{
		raw: &fakeAdminClient{
			metadata: []*kafka.Metadata{
				{
					Topics: map[string]kafka.TopicMetadata{
						"full_load": {Error: kafka.NewError(kafka.ErrUnknownTopicOrPart, "", false)},
					},
				},
			},
			deleteResults: [][]kafka.TopicResult{
				{{Error: kafka.NewError(kafka.ErrNoError, "", false)}},
			},
			createResults: [][]kafka.TopicResult{
				{{Error: kafka.NewError(kafka.ErrTopicAlreadyExists, "", false)}},
				{{Error: kafka.NewError(kafka.ErrNoError, "", false)}},
			},
		},
		metadataTimeout: time.Second,
	}

	err := admin.RecreateTopic(context.Background(), TopicSpec{
		Name:              "full_load",
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, time.Second)
	if err != nil {
		t.Fatalf("recreate topic: %v", err)
	}

	fake := admin.raw.(*fakeAdminClient)
	if fake.deleteCalls != 1 {
		t.Fatalf("delete calls = %d, want 1", fake.deleteCalls)
	}
	if fake.createCalls != 2 {
		t.Fatalf("create calls = %d, want 2", fake.createCalls)
	}
}
