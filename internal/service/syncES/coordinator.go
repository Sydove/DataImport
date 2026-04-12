package syncES

import (
	"DataImport/internal/db/postgresql"
	"DataImport/internal/pkg/es"
	newkafka "DataImport/internal/pkg/kafka"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

type Coordinator struct {
	cfg          Config
	manager      *newkafka.Manager
	admin        *newkafka.Admin
	producer     *newkafka.Producer
	dlqProducer  *newkafka.Producer
	esClient     *es.ESClient
	reader       *PGReader
	stats        *Stats
	job          Job
	lastPushedID int
}

// Run
//
//	@Description: 初始化相关组件,启动程序
//	@param ctx: 系统信号上下文,用于停止运行程序
//	@param cfg: 配置文件
//	@return error
func Run(ctx context.Context, cfg Config) error {
	if err := postgresql.InitDB(); err != nil {
		return fmt.Errorf("init postgres: %w", err)
	}

	manager, err := newkafka.NewManagerFromViper()
	if err != nil {
		return fmt.Errorf("create kafka manager: %w", err)
	}
	defer manager.Close()

	admin, err := manager.NewAdmin()
	if err != nil {
		return fmt.Errorf("create kafka admin: %w", err)
	}

	producer, err := manager.NewProducer(newkafka.ProducerOptions{
		CompressionType: "snappy",
		BatchSize:       cfg.ProducerBatchSize,
		LingerMs:        cfg.ProducerLingerMs,
		Acks:            "all",
		MaxPending:      cfg.ProducerMaxPending,
	})
	if err != nil {
		return fmt.Errorf("create full-load producer: %w", err)
	}

	dlqProducer, err := manager.NewProducer(newkafka.ProducerOptions{
		CompressionType: "snappy",
		BatchSize:       cfg.ProducerBatchSize,
		LingerMs:        cfg.ProducerLingerMs,
		Acks:            "all",
		MaxPending:      cfg.ProducerMaxPending,
	})
	if err != nil {
		return fmt.Errorf("create dlq producer: %w", err)
	}

	coordinator := &Coordinator{
		cfg:         cfg,
		manager:     manager,
		admin:       admin,
		producer:    producer,
		dlqProducer: dlqProducer,
		esClient:    es.NewESClient(),
		reader:      NewPGReader(cfg.PageSize, cfg.ReaderTimeout),
		stats:       newStats(),
	}
	return coordinator.Run(ctx)
}

func (c *Coordinator) Run(ctx context.Context) error {
	// 生成当前任务的可取消的上下文
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	job, initErr := initJob(runCtx)
	if initErr != nil {
		return initErr
	}
	c.job = job
	if c.job.ResumedFromDisk {
		fmt.Printf(
			"恢复 full_load 任务: job_id=%s snapshot_max_id=%d resume_from_id=%d\n",
			c.job.ID,
			c.job.SnapshotMaxID,
			c.job.ResumeFromID,
		)
	} else {
		fmt.Printf(
			"启动新的 full_load 任务: job_id=%s snapshot_max_id=%d\n",
			c.job.ID,
			c.job.SnapshotMaxID,
		)
	}

	if err := ensureTopics(runCtx, c.admin, c.cfg); err != nil {
		return err
	}
	if err := ensureIndex(runCtx, c.esClient, c.cfg); err != nil {
		return err
	}

	group, groupCtx := errgroup.WithContext(runCtx)
	producerDone := make(chan struct{})
	var producerFinished atomic.Bool

	// 启动生产者
	group.Go(func() error {
		defer close(producerDone)
		if err := c.runProducer(groupCtx); err != nil {
			return err
		}
		producerFinished.Store(true)
		return nil
	})

	// 启动消费者
	for i := 0; i < c.cfg.ConsumerWorkers; i++ {
		workerID := i + 1
		group.Go(func() error {
			return c.runConsumer(groupCtx, workerID)
		})
	}

	// 监听生产者状态,判断是否需要退出
	group.Go(func() error {
		select {
		case <-groupCtx.Done():
			return nil
		case <-producerDone:
		}

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-groupCtx.Done():
				return nil
			case <-ticker.C:
				// 检查生产者是否完成,且队列为空,且空闲时间超过配置的超时时间
				stats := c.producer.Stats()
				idleFor := time.Since(c.stats.LastHandledAt())
				if producerFinished.Load() && stats.PendingCount == 0 && stats.QueueLen == 0 && idleFor >= c.cfg.ConsumerDrainIdle {
					cancel()
					return nil
				}
			}
		}
	})

	err := group.Wait()
	// 刷新本地的生产者队列,确保所有消息都被发送完成
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer flushCancel()

	if flushErr := c.dlqProducer.Flush(flushCtx); flushErr != nil && err == nil {
		err = fmt.Errorf("flush dlq producer: %w", flushErr)
	}

	if err == nil {
		if checkpointErr := deleteCheckpoint(); checkpointErr != nil {
			err = checkpointErr
		}
	}

	c.printSummary()

	if err != nil && errors.Is(err, context.Canceled) && ctx.Err() != nil {
		return nil
	}
	return err
}

// runProducer
//
//	@Description: 启动生产者,读取数据库,并发布到Kafka主题
//	@receiver c
//	@param ctx
//	@return error
func (c *Coordinator) runProducer(ctx context.Context) error {
	if c.job.SnapshotMaxID == 0 {
		return c.flushProducerOnStop(nil)
	}

	startID := c.job.ResumeFromID
	for {
		if err := c.waitForPipelineCapacity(ctx); err != nil {
			return c.flushProducerOnStop(err)
		}

		// 是否收到取消信号,则退出
		select {
		case <-ctx.Done():
			return c.flushProducerOnStop(ctx.Err())
		default:
		}

		records, err := c.reader.ReadBatch(ctx, startID, c.job.SnapshotMaxID)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return c.flushProducerOnStop(ctx.Err())
			}
			return err
		}
		if len(records) == 0 {
			break
		}

		batch := FullLoadBatch{
			BatchID:   fmt.Sprintf("article-%d-%d", records[0].ID, records[len(records)-1].ID),
			StartID:   records[0].ID,
			EndID:     records[len(records)-1].ID,
			Source:    "postgresql.article",
			CreatedAt: time.Now().UTC(),
			Records:   records,
		}

		payload, err := json.Marshal(batch)
		if err != nil {
			return fmt.Errorf("marshal full load batch: %w", err)
		}

		if err := c.producer.Publish(ctx, newkafka.Message{
			Topic: c.cfg.TopicName,
			Key:   nil,
			Value: payload,
		}); err != nil {
			// 收到取消信号
			if errors.Is(err, context.Canceled) {
				return c.flushProducerOnStop(ctx.Err())
			}
			return fmt.Errorf("publish full load batch: %w", err)
		}

		c.stats.readBatches.Add(1)
		c.stats.readRecords.Add(int64(len(records)))
		c.stats.publishedBatches.Add(1)
		c.lastPushedID = batch.EndID
		startID = batch.EndID
	}

	return c.flushProducerOnStop(nil)
}

func (c *Coordinator) waitForPipelineCapacity(ctx context.Context) error {
	if c.cfg.MaxPipelineBatches <= 0 {
		return nil
	}

	wait := c.cfg.ProducerThrottleWait
	if wait <= 0 {
		wait = 100 * time.Millisecond
	}

	for {
		backlog := c.stats.publishedBatches.Load() - c.stats.consumedBatches.Load()
		if backlog < c.cfg.MaxPipelineBatches {
			return nil
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *Coordinator) flushProducerOnStop(stopErr error) error {
	// 刷新full load producer本地队列,确保所有主topic消息在消费者退出前已经进入Kafka
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer flushCancel()

	if err := c.producer.Flush(flushCtx); err != nil {
		return fmt.Errorf("flush full-load producer: %w", err)
	}

	if stopErr != nil {
		checkpoint := Checkpoint{
			JobID:              c.job.ID,
			SnapshotMaxID:      c.job.SnapshotMaxID,
			LastPublishedEndID: c.lastPushedID,
		}
		if checkpoint.LastPublishedEndID == 0 {
			checkpoint.LastPublishedEndID = c.job.ResumeFromID
		}
		if err := saveCheckpoint(checkpoint); err != nil {
			return err
		}
	}
	return stopErr
}

func (c *Coordinator) runConsumer(ctx context.Context, workerID int) error {
	consumer, err := c.manager.NewConsumer(newkafka.ConsumerOptions{
		GroupID:          c.consumerGroupID(),
		Topics:           []string{c.cfg.TopicName},
		AutoOffsetReset:  "earliest",
		CommitBatchSize:  c.cfg.CommitBatchSize,
		CommitInterval:   c.cfg.CommitInterval,
		EnableAutoCommit: false,
		MaxPollRecords:   c.cfg.ConsumerBatchSize,
		FetchMaxWait:     c.cfg.ConsumerBatchWait,
		WorkerCount:      c.cfg.ConsumerPoolSize,
		WorkQueueSize:    c.cfg.ConsumerQueueSize,
	})
	if err != nil {
		return fmt.Errorf("create consumer-%d: %w", workerID, err)
	}
	defer consumer.Close()

	handler := NewHandler(
		workerID,
		NewESWriter(c.esClient, c.cfg.IndexName, c.cfg.ESRetryWait),
		c.dlqProducer,
		c.cfg,
		c.stats,
	)

	fmt.Printf("consumer-%d 启动成功\n", workerID)
	if err := consumer.Run(ctx, handler.Handle); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return fmt.Errorf("run consumer-%d: %w", workerID, err)
	}
	fmt.Printf("consumer-%d 已退出\n", workerID)
	return nil
}

func (c *Coordinator) consumerGroupID() string {
	return fmt.Sprintf("full_load_group_%s", c.job.ID)
}

func (c *Coordinator) printSummary() {
	fmt.Printf(
		"full_load summary: read_batches=%d read_records=%d published_batches=%d consumed_batches=%d es_failed_batches=%d dlq_published=%d\n",
		c.stats.readBatches.Load(),
		c.stats.readRecords.Load(),
		c.stats.publishedBatches.Load(),
		c.stats.consumedBatches.Load(),
		c.stats.esFailedBatches.Load(),
		c.stats.dlqPublished.Load(),
	)
}

func initJob(ctx context.Context) (Job, error) {
	checkpoint, err := loadCheckpoint()
	if err != nil {
		return Job{}, err
	}
	if checkpoint != nil {
		return Job{
			ID:              checkpoint.JobID,
			SnapshotMaxID:   checkpoint.SnapshotMaxID,
			ResumeFromID:    checkpoint.LastPublishedEndID,
			ResumedFromDisk: true,
		}, nil
	}

	row := postgresql.Pool.QueryRow(ctx, "SELECT COALESCE(MAX(id), 0) FROM article")

	var snapshotMaxID int
	if err := row.Scan(&snapshotMaxID); err != nil {
		return Job{}, fmt.Errorf("query snapshot max id: %w", err)
	}

	return Job{
		ID:            time.Now().Format("20060102_150405"),
		SnapshotMaxID: snapshotMaxID,
	}, nil
}
