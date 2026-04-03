package syncES

import (
	"DataImport/internal/pkg/es"
	newkafka "DataImport/internal/pkg/kafka"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type ESWriter struct {
	client    *es.ESClient
	indexName string
	retryWait time.Duration
}

func NewESWriter(client *es.ESClient, indexName string, retryWait time.Duration) *ESWriter {
	return &ESWriter{
		client:    client,
		indexName: indexName,
		retryWait: retryWait,
	}
}

func (w *ESWriter) WriteBatch(ctx context.Context, workerID int, batch FullLoadBatch) ([]string, error) {
	docs := recordsToDocuments(batch.Records)
	var failedIDs []string
	var lastErr error

	for attempt := 0; attempt < w.client.MaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return failedIDs, ctx.Err()
		default:
		}

		failedIDs, lastErr = w.client.BulkInsertDocuments(workerID, ctx, w.indexName, docs)
		if lastErr == nil {
			return failedIDs, nil
		}

		if attempt == w.client.MaxRetries-1 {
			break
		}

		timer := time.NewTimer(w.retryWait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return failedIDs, ctx.Err()
		case <-timer.C:
		}
	}

	return failedIDs, fmt.Errorf("write batch to es after retries: %w", lastErr)
}

type Handler struct {
	workerID    int
	esWriter    *ESWriter
	dlqProducer *newkafka.Producer
	cfg         Config
	stats       *Stats
}

func NewHandler(
	workerID int,
	esWriter *ESWriter,
	dlqProducer *newkafka.Producer,
	cfg Config,
	stats *Stats,
) *Handler {
	return &Handler{
		workerID:    workerID,
		esWriter:    esWriter,
		dlqProducer: dlqProducer,
		cfg:         cfg,
		stats:       stats,
	}
}

// Handle
//
//	@Description: 写入es数据库
//	@receiver h
//	@param ctx
//	@param msg
//	@return error
func (h *Handler) Handle(ctx context.Context, msg *newkafka.Message) error {
	var batch FullLoadBatch
	if err := json.Unmarshal(msg.Value, &batch); err != nil {
		return fmt.Errorf("unmarshal full load batch: %w", err)
	}

	failedIDs, err := h.esWriter.WriteBatch(ctx, h.workerID, batch)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		h.stats.esFailedBatches.Add(1)
		if dlqErr := h.publishDLQ(ctx, batch, failedIDs, err); dlqErr != nil {
			return fmt.Errorf("write batch failed: %w; publish dlq failed: %w", err, dlqErr)
		}
		h.stats.consumedBatches.Add(1)
		h.stats.TouchHandled()
		return nil
	}

	h.stats.consumedBatches.Add(1)
	h.stats.TouchHandled()
	return nil
}

// publishDLQ
//
//	@Description: 推送到死信队列
//	@receiver h
//	@param ctx
//	@param batch
//	@param failedIDs
//	@param cause
//	@return error
func (h *Handler) publishDLQ(ctx context.Context, batch FullLoadBatch, failedIDs []string, cause error) error {
	payload, err := json.Marshal(FullLoadDLQMessage{
		BatchID:      batch.BatchID,
		StartID:      batch.StartID,
		EndID:        batch.EndID,
		FailedIDs:    failedIDs,
		ErrorMessage: cause.Error(),
		CreatedAt:    time.Now(),
	})
	if err != nil {
		return fmt.Errorf("marshal dlq payload: %w", err)
	}

	if err := h.dlqProducer.Publish(ctx, newkafka.Message{
		Topic: h.cfg.DLQTopicName,
		Key:   []byte(batch.BatchID),
		Value: payload,
	}); err != nil {
		return fmt.Errorf("publish dlq message: %w", err)
	}

	h.stats.dlqPublished.Add(1)
	return nil
}

// recordsToDocuments
//
//	@Description: 将ArticleRecord转换为ES文档结构
//	@param records
//	@return []map[string]interface{}
func recordsToDocuments(records []ArticleRecord) []map[string]interface{} {
	docs := make([]map[string]interface{}, 0, len(records))
	for _, record := range records {
		docs = append(docs, map[string]interface{}{
			"id":         record.ID,
			"content":    record.Content,
			"origin_id":  record.OriginID,
			"title":      record.Title,
			"created_at": record.CreatedAt,
			"account_id": record.AccountID,
		})
	}
	return docs
}
