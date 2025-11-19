package kafka

import (
	"time"
)

// Message Kafka 消息结构
type Message struct {
	ID   string
	Host string
	Port string
}

// FailedMessage 失败消息详情
type FailedMessage struct {
	Value string
	Error error
	Time  time.Time
}

// ProducerTracker 用于跟踪 Producer 发送状态
type ProducerTracker struct {
	sentCount       int64 // 已发送消息数
	successCount    int64 // 成功确认数
	failedCount     int64 // 失败数
	maxPending      int64 // 最大待确认消息数（背压阈值）
	backpressureHit int64 // 背压触发次数
}

// GetStats 获取统计信息
func (pt *ProducerTracker) GetStats() (sent, success, failed, backpressureHits int64) {
	return pt.sentCount, pt.successCount, pt.failedCount, pt.backpressureHit
}

// ProducerConfig Producer 配置
type ProducerConfig struct {
	CompressionType string // 压缩类型：snappy, lz4, gzip
	BatchSize       int    // 批量大小（字节）
	LingerMs        int    // 等待时间（毫秒）
	Acks            string // 确认级别：0, 1, all
	MaxPending      int64  // 最大待确认消息数（背压阈值）
}

// ConsumerConfig Consumer 配置
type ConsumerConfig struct {
	GroupID         string   // Consumer Group ID
	Topics          []string // 订阅的 Topic 列表
	AutoOffsetReset string   // earliest 或 latest
	CommitBatchSize int      // 批量提交大小
}

// TopicConfig Topic 配置
type TopicConfig struct {
	Name              string // Topic 名称
	NumPartitions     int    // 分区数
	ReplicationFactor int    // 副本因子
}
