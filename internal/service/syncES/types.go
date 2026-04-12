package syncES

import (
	"sync/atomic"
	"time"
)

const (
	TopicName    = "full_load"
	DLQTopicName = "full_load_dlq"
	IndexName    = "article"
)

type ArticleRecord struct {
	Content   string    `json:"content"`
	OriginID  string    `json:"origin_id"`
	ID        int       `json:"id"`
	Title     string    `json:"title"`
	CreatedAt time.Time `json:"created_at"`
	AccountID int       `json:"account_id"`
}

type FullLoadBatch struct {
	BatchID   string          `json:"batch_id"`
	StartID   int             `json:"start_id"`
	EndID     int             `json:"end_id"`
	Source    string          `json:"source"`
	CreatedAt time.Time       `json:"created_at"`
	Records   []ArticleRecord `json:"records"`
}

type FullLoadDLQMessage struct {
	BatchID      string    `json:"batch_id"`
	StartID      int       `json:"start_id"`
	EndID        int       `json:"end_id"`
	FailedIDs    []string  `json:"failed_ids"`
	ErrorMessage string    `json:"error_message"`
	CreatedAt    time.Time `json:"created_at"`
}

type Config struct {
	TopicName            string        // Kafka 主题名称，用于存储全量数据
	DLQTopicName         string        // 死信队列主题名称，用于存储处理失败的数据
	IndexName            string        // Elasticsearch 索引名称
	PageSize             int           // 分页大小，每次从数据源读取的记录数
	ConsumerWorkers      int           // 消费者工作线程数
	TopicPartitions      int           // Kafka 主题分区数
	DLQPartitions        int           // 死信队列分区数
	CommitBatchSize      int           // 提交批次大小
	CommitInterval       time.Duration // 提交间隔时间
	ConsumerBatchSize    int           // 单次拉取的最大消息数
	ConsumerBatchWait    time.Duration // 批量拉取的最长等待时间
	ConsumerPoolSize     int           // 单个 consumer 内部异步 worker 数量
	ConsumerQueueSize    int           // 单个 consumer 内部待处理队列长度
	ProducerMaxPending   int64         // 生产者最大待处理消息数
	MaxPipelineBatches   int64         // 允许 producer 超前 consumer 的最大 batch 数
	ProducerLingerMs     int           // 生产者linger时间（毫秒）
	ProducerBatchSize    int           // 生产者批次大小
	ProducerThrottleWait time.Duration // 应用层背压等待时间
	ConsumerDrainIdle    time.Duration // 消费者空闲时的排水时间
	ReaderTimeout        time.Duration // 读取超时时间
	ESRetryWait          time.Duration // Elasticsearch 重试等待时间
	RecreateTopics       bool          // 是否重新创建主题
	CreateIndex          bool          // 是否创建索引
	DeleteIndexFirst     bool          // 是否先删除索引
}

type Job struct {
	ID              string
	SnapshotMaxID   int
	ResumeFromID    int
	ResumedFromDisk bool
}

func DefaultConfig() Config {
	return Config{
		TopicName:            TopicName,       // Kafka 主题名称，用于存储全量数据
		DLQTopicName:         DLQTopicName,    // 死信队列主题名称，用于存储处理失败的数据
		IndexName:            IndexName,       // Elasticsearch 索引名称
		PageSize:             20,              // 分页大小，每次从数据源读取的记录数
		ConsumerWorkers:      6,               // 消费者工作线程数
		TopicPartitions:      6,               // Kafka 主题分区数
		DLQPartitions:        1,               // 死信队列分区数
		CommitBatchSize:      50,              // 提交批次大小
		CommitInterval:       5 * time.Second, // 提交间隔时间
		ConsumerBatchSize:    50,              // 单次拉取的最大消息数
		ConsumerBatchWait:    100 * time.Millisecond,
		ConsumerPoolSize:     2, // 每个 consumer 保守并发，避免 ES 突然承压过大
		ConsumerQueueSize:    200,
		ProducerMaxPending:   5000,  // 生产者最大待处理消息数
		MaxPipelineBatches:   72,    // 允许约 6*2*6 个 batch 在 consumer 前方排队
		ProducerLingerMs:     10,    // 生产者linger时间（毫秒）
		ProducerBatchSize:    65536, // 生产者批次大小
		ProducerThrottleWait: 100 * time.Millisecond,
		ConsumerDrainIdle:    10 * time.Second,  // 消费者空闲时的排水时间
		ReaderTimeout:        160 * time.Second, // 读取超时时间
		ESRetryWait:          2 * time.Second,   // Elasticsearch 重试等待时间
		RecreateTopics:       true,
		CreateIndex:          true,
		DeleteIndexFirst:     true,
	}
}

type Stats struct {
	readBatches      atomic.Int64 //已读取的批次数量
	readRecords      atomic.Int64 // 已读取的记录数量
	publishedBatches atomic.Int64 // 已发布的批次数量
	consumedBatches  atomic.Int64 // 已消费的批次数量
	esFailedBatches  atomic.Int64 // es写入失败的批次数量
	dlqPublished     atomic.Int64 // 已发布到死信队列的批次数量
	lastHandledUnix  atomic.Int64 // 最后一批处理的时间戳
}

func newStats() *Stats {
	stats := &Stats{}
	stats.TouchHandled()
	return stats
}

// TouchHandled
//
//	@Description: 更新最后一批处理的时间戳为当前时间
//	@receiver s
func (s *Stats) TouchHandled() {
	s.lastHandledUnix.Store(time.Now().UnixNano())
}

// LastHandledAt
//
//	@Description: 将存储的时间戳转换为time.Time类型
//	@receiver s
//	@return time.Time
func (s *Stats) LastHandledAt() time.Time {
	unixNano := s.lastHandledUnix.Load()
	if unixNano == 0 {
		return time.Time{}
	}
	return time.Unix(0, unixNano)
}
