package syncES

import (
	newkafka "DataImport/internal/pkg/kafka"
	"DataImport/internal/pkg/utils"
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"time"
)

func ensureTopics(ctx context.Context, admin *newkafka.Admin, cfg Config) error {
	if cfg.RecreateTopics {
		if err := deleteTopicIfExists(ctx, admin, cfg.TopicName); err != nil {
			return err
		}
		if err := deleteTopicIfExists(ctx, admin, cfg.DLQTopicName); err != nil {
			return err
		}
	}

	if err := admin.EnsureTopic(ctx, newkafka.TopicSpec{
		Name:              cfg.TopicName,
		NumPartitions:     cfg.TopicPartitions,
		ReplicationFactor: 1,
		ConfigMap: map[string]string{
			"retention.bytes":     "1073741824",
			"segment.bytes":       "104857600",
			"cleanup.policy":      "delete",
			"retention.ms":        "604800000",
			"segment.ms":          "86400000",
			"min.insync.replicas": "1",
			"compression.type":    "snappy",
		},
	}); err != nil {
		return fmt.Errorf("ensure topic %s: %w", cfg.TopicName, err)
	}

	if err := admin.EnsureTopic(ctx, newkafka.TopicSpec{
		Name:              cfg.DLQTopicName,
		NumPartitions:     cfg.DLQPartitions,
		ReplicationFactor: 1,
		ConfigMap: map[string]string{
			"retention.bytes":     "1073741824",
			"segment.bytes":       "104857600",
			"cleanup.policy":      "delete",
			"retention.ms":        "604800000",
			"segment.ms":          "86400000",
			"min.insync.replicas": "1",
			"compression.type":    "snappy",
		},
	}); err != nil {
		return fmt.Errorf("ensure topic %s: %w", cfg.DLQTopicName, err)
	}

	if err := admin.WaitTopicReady(cfg.TopicName, 30*time.Second); err != nil {
		return err
	}
	if err := admin.WaitTopicReady(cfg.DLQTopicName, 30*time.Second); err != nil {
		return err
	}
	return nil
}

func deleteTopicIfExists(ctx context.Context, admin *newkafka.Admin, topic string) error {
	exists, err := admin.TopicExists(topic)
	if err != nil {
		return fmt.Errorf("check topic %s: %w", topic, err)
	}
	if !exists {
		return nil
	}
	if err := admin.DeleteTopic(ctx, topic); err != nil {
		return fmt.Errorf("delete topic %s: %w", topic, err)
	}
	return nil
}

func ensureIndex(ctx context.Context, client interface {
	IndexExists(context.Context, string) (bool, error)
	DeleteIndex(context.Context, string) error
	CreateIndexWithMapping(context.Context, string, map[string]interface{}) error
}, cfg Config) error {
	if !cfg.CreateIndex && !cfg.DeleteIndexFirst {
		return nil
	}

	exists, err := client.IndexExists(ctx, cfg.IndexName)
	if err != nil {
		return fmt.Errorf("check index exists: %w", err)
	}
	if exists && cfg.DeleteIndexFirst {
		if err := client.DeleteIndex(ctx, cfg.IndexName); err != nil {
			return fmt.Errorf("delete index %s: %w", cfg.IndexName, err)
		}
		exists = false
	}
	if exists || !cfg.CreateIndex {
		return nil
	}

	mappingPath, err := articleMappingPath()
	if err != nil {
		return err
	}
	mapping, err := utils.ReadJSONFile(mappingPath)
	if err != nil {
		return fmt.Errorf("read index mapping: %w", err)
	}
	if err := client.CreateIndexWithMapping(ctx, cfg.IndexName, mapping); err != nil {
		return fmt.Errorf("create index %s: %w", cfg.IndexName, err)
	}
	return nil
}

func articleMappingPath() (string, error) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("resolve article mapping path: runtime caller unavailable")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", "..", "..", "config", "article.json")), nil
}
