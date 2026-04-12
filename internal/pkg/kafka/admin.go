package newkafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var topicOperationPollInterval = 200 * time.Millisecond

type adminClient interface {
	CreateTopics(context.Context, []kafka.TopicSpecification, ...kafka.CreateTopicsAdminOption) ([]kafka.TopicResult, error)
	DeleteTopics(context.Context, []string, ...kafka.DeleteTopicsAdminOption) ([]kafka.TopicResult, error)
	GetMetadata(*string, bool, int) (*kafka.Metadata, error)
	Close()
}

type Admin struct {
	raw             adminClient
	metadataTimeout time.Duration
	closeOnce       sync.Once
}

// newAdmin
//
//	@Description: 创建底层 Kafka Admin Client 封装
//	@param baseConfig
//	@param metadataTimeout
//	@return *Admin
//	@return error
func newAdmin(baseConfig *kafka.ConfigMap, metadataTimeout time.Duration) (*Admin, error) {
	rawAdmin, err := kafka.NewAdminClient(baseConfig)
	if err != nil {
		return nil, fmt.Errorf("create kafka admin client: %w", err)
	}

	if metadataTimeout == 0 {
		metadataTimeout = 5 * time.Second
	}

	return &Admin{
		raw:             rawAdmin,
		metadataTimeout: metadataTimeout,
	}, nil
}

// EnsureTopic
//
//	@Description: 确保指定 Topic 存在，不存在时自动创建
//	@receiver a
//	@param ctx
//	@param spec
//	@return error
func (a *Admin) EnsureTopic(ctx context.Context, spec TopicSpec) error {
	if spec.Name == "" {
		return fmt.Errorf("topic name is required")
	}

	_, err := a.createTopic(ctx, spec)
	if err != nil {
		return err
	}
	return nil
}

// RecreateTopic
//
//	@Description: 删除指定 Topic 并等待同名 Topic 可重新创建
//	@receiver a
//	@param ctx
//	@param spec
//	@param timeout
//	@return error
func (a *Admin) RecreateTopic(ctx context.Context, spec TopicSpec, timeout time.Duration) error {
	if spec.Name == "" {
		return fmt.Errorf("topic name is required")
	}

	exists, err := a.TopicExists(spec.Name)
	if err != nil {
		return err
	}

	deleteRequested := false
	if exists {
		if _, err := a.deleteTopicIfPresent(ctx, spec.Name); err != nil {
			return err
		}
		deleteRequested = true
	}

	deadline := time.Now().Add(timeout)
	for {
		created, err := a.createTopic(ctx, spec)
		if err != nil {
			return err
		}
		if created {
			return nil
		}

		// Metadata 可能短暂落后于控制器状态；如果第一次创建就收到 already exists，
		// 说明同名 topic 仍在集群中，再补发一次删除请求后继续等待。
		if !deleteRequested {
			if _, err := a.deleteTopicIfPresent(ctx, spec.Name); err != nil {
				return err
			}
			deleteRequested = true
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("topic %s is still present after delete timeout", spec.Name)
		}
		if err := sleepContext(ctx, topicOperationPollInterval); err != nil {
			return err
		}
	}
}

// DeleteTopic
//
//	@Description: 删除指定 Topic
//	@receiver a
//	@param ctx
//	@param name
//	@return error
func (a *Admin) DeleteTopic(ctx context.Context, name string) error {
	_, err := a.deleteTopicIfPresent(ctx, name)
	return err
}

// WaitTopicDeleted
//
//	@Description: 等待 Topic 从集群元数据中彻底消失
//	@receiver a
//	@param topic
//	@param timeout
//	@return error
func (a *Admin) WaitTopicDeleted(topic string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		md, err := a.raw.GetMetadata(&topic, false, int(a.metadataTimeout.Milliseconds()))
		if err == nil {
			t, ok := md.Topics[topic]
			if !ok || t.Error.Code() == kafka.ErrUnknownTopic || t.Error.Code() == kafka.ErrUnknownPartition {
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("topic %s is still present after delete timeout", topic)
}

// WaitTopicReady
//
//	@Description: 等待 Topic 元数据可用
//	@receiver a
//	@param topic
//	@param timeout
//	@return error
func (a *Admin) WaitTopicReady(topic string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		md, err := a.raw.GetMetadata(&topic, false, int(a.metadataTimeout.Milliseconds()))
		if err == nil {
			if t, ok := md.Topics[topic]; ok && topicReady(t) {
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("topic %s not ready", topic)
}

func topicReady(topicMeta kafka.TopicMetadata) bool {
	if topicMeta.Error.Code() != kafka.ErrNoError {
		return false
	}
	if len(topicMeta.Partitions) == 0 {
		return false
	}
	for _, partition := range topicMeta.Partitions {
		if partition.Error.Code() != kafka.ErrNoError {
			return false
		}
		if partition.Leader == -1 {
			return false
		}
	}
	return true
}

// TopicExists
//
//	@Description: 检查指定 Topic 是否存在
//	@receiver a
//	@param topic
//	@return bool
//	@return error
func (a *Admin) TopicExists(topic string) (bool, error) {
	md, err := a.raw.GetMetadata(&topic, false, int(a.metadataTimeout.Milliseconds()))
	if err != nil {
		return false, fmt.Errorf("get topic metadata %s: %w", topic, err)
	}

	topicMeta, ok := md.Topics[topic]
	if !ok {
		return false, nil
	}

	return topicMeta.Error.Code() == kafka.ErrNoError, nil
}

// ListTopics
//
//	@Description: 列出当前集群的全部 Topic 名称
//	@receiver a
//	@return []string
//	@return error
func (a *Admin) ListTopics() ([]string, error) {
	md, err := a.raw.GetMetadata(nil, true, int(a.metadataTimeout.Milliseconds()))
	if err != nil {
		return nil, fmt.Errorf("list kafka topics: %w", err)
	}

	topics := make([]string, 0, len(md.Topics))
	for topic := range md.Topics {
		topics = append(topics, topic)
	}
	return topics, nil

}

// Metadata
//
//	@Description: 获取当前集群的 brokers 和 topics 元信息
//	@receiver a
//	@return ClusterMetadata
//	@return error
func (a *Admin) Metadata() (ClusterMetadata, error) {
	md, err := a.raw.GetMetadata(nil, true, int(a.metadataTimeout.Milliseconds()))
	if err != nil {
		return ClusterMetadata{}, fmt.Errorf("get kafka metadata: %w", err)
	}

	result := ClusterMetadata{
		Brokers: make([]BrokerInfo, 0, len(md.Brokers)),
		Topics:  make([]string, 0, len(md.Topics)),
	}

	for _, broker := range md.Brokers {
		result.Brokers = append(result.Brokers, BrokerInfo{
			ID:   int(broker.ID),
			Host: broker.Host,
			Port: broker.Port,
		})
	}
	for topic := range md.Topics {
		result.Topics = append(result.Topics, topic)
	}

	return result, nil
}

// Close
//
//	@Description: 关闭 Admin Client
//	@receiver a
//	@return error
func (a *Admin) Close() error {
	a.closeOnce.Do(func() {
		a.raw.Close()
	})
	return nil
}

func (a *Admin) createTopic(ctx context.Context, spec TopicSpec) (bool, error) {
	results, err := a.raw.CreateTopics(ctx, []kafka.TopicSpecification{{
		Topic:             spec.Name,
		NumPartitions:     spec.NumPartitions,
		ReplicationFactor: spec.ReplicationFactor,
		Config:            spec.ConfigMap,
	}})
	if err != nil {
		return false, fmt.Errorf("create topic %s: %w", spec.Name, err)
	}
	if len(results) == 0 {
		return true, nil
	}

	switch results[0].Error.Code() {
	case kafka.ErrNoError:
		return true, nil
	case kafka.ErrTopicAlreadyExists:
		return false, nil
	default:
		return false, fmt.Errorf("create topic %s: %s", spec.Name, results[0].Error.String())
	}
}

func (a *Admin) deleteTopicIfPresent(ctx context.Context, name string) (bool, error) {
	results, err := a.raw.DeleteTopics(ctx, []string{name}, kafka.SetAdminOperationTimeout(a.metadataTimeout))
	if err != nil {
		return false, fmt.Errorf("delete topic %s: %w", name, err)
	}
	if len(results) == 0 {
		return false, nil
	}

	switch results[0].Error.Code() {
	case kafka.ErrNoError:
		return true, nil
	case kafka.ErrUnknownTopic, kafka.ErrUnknownPartition, kafka.ErrUnknownTopicOrPart:
		return false, nil
	default:
		return false, fmt.Errorf("delete topic %s: %s", name, results[0].Error.String())
	}
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
