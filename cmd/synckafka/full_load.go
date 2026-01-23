package main

/*
单个goroutine从postgresql读取数据,推送个体到jobs channel
多个goroutine从jobs channel消费数据,批量写入kafka
多个producer从kafka消费数据,批量写入es
postgresql → [Reader goroutine] → jobs channel → [N Sender goroutines] → Kafka → [M Consumers] → ES (Bulk)
*/

import (
	"DataImport/internal/db/postgresql"
	_ "DataImport/internal/pkg/config"
	"DataImport/internal/pkg/es"
	mKakfa "DataImport/internal/pkg/kafka"
	"DataImport/internal/pkg/utils"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	kafkaGo "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jackc/pgx/v5"
)

const (
	TopicName    = "full_load"
	DLQTopicName = "full_load_dlq"
	IndexName    = "article"
)

type DataReocrd struct {
	Content   string    `json:"content"`
	OriginId  string    `json:"origin_id"`
	ID        int       `json:"id"`
	Title     string    `json:"title"`
	CreatedAt time.Time `json:"created_at"`
	AccountId int       `json:"account_id"`
}

// readFromPostgres
//
//	@Description: 从postgresql读取数据
//	@param ctx
//	@param pageSize limit条数
//	@param startId 开始id
//	@return []postgresql.Record
func readFromPostgres(ctx context.Context, pageSize, startId int) []DataReocrd {
	// TODO 重复查询出来了一条
	rows, err := postgresql.Pool.Query(ctx, "SELECT content, origin_id, id, title, created_at, account_id FROM article WHERE id > $1 order by id LIMIT $2", startId, pageSize)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	results, err := pgx.CollectRows(rows, pgx.RowToStructByName[DataReocrd])
	if err != nil {
		fmt.Println("Error reading from postgres:", err)
		panic(err)
	}
	// 限制时间精度到毫秒,es最多支持到毫秒级别(3位小数)
	for i := range results {
		results[i].CreatedAt = results[i].CreatedAt.Truncate(time.Millisecond)
	}
	return results
}

// pushToChannel
//
//	@Description: 读取数据推送到channel
func pushToChannel(job chan []DataReocrd, producer *mKakfa.Producer) {
	defer close(job)

	startId := 0

	for {
		select {
		case <-producer.StopCtx.Done():
			fmt.Println("收到关闭信号，停止读取pgsql数据/推送channel")
			return
		default:
			// 读取 postgres 超时时间
			ctx, cancel := context.WithTimeout(context.Background(), 160*time.Second)

			records := readFromPostgres(ctx, 100, startId)
			cancel()

			// postgres 没数据 → 退出
			if len(records) == 0 {
				fmt.Println("pgsql数据读取完毕: 停止读取pgsql数据/推送channel")
				return
			}
			// 推送数据
			job <- records
			startId = records[len(records)-1].ID
		}
	}
}

// producer
//
//	@Description: 从channel推送数据到kafka,channel被关闭后,自动退出
func producer(client *mKakfa.Client, group *sync.WaitGroup, stopCtx context.Context) {
	job := make(chan []DataReocrd, 3)
	err := client.NewClientWithConfig(mKakfa.ProducerConfig{
		CompressionType: "snappy",
		BatchSize:       65536,
		LingerMs:        10,
		Acks:            "all",
		//Acks:       "1",
		MaxPending: 10000, // 背压阈值
	}, stopCtx)
	if err != nil {
		panic(err)
	}
	mProducer := client.GetProducer()
	go pushToChannel(job, mProducer)
	var topicName = TopicName
	for i := 0; i < 6; i++ {
		group.Add(1)
		go func(partitionId int) {
			defer group.Done()
			defer mProducer.Producer.Flush(3000) // Flush() 实际上是等待消息的 ack 返回，而不仅仅是发出网络请求
			for record := range job {
				//  批量kafka
				jsonData, err := json.Marshal(record)
				if err != nil {
					fmt.Println("序列化channel数据异常", err)
					continue
				}
				sendMsg := &kafkaGo.Message{
					TopicPartition: kafkaGo.TopicPartition{
						Topic:     &topicName,
						Partition: int32(i), // 直接指定partition
					},
					Value: jsonData,
				}
				if err := mProducer.Produce(sendMsg); err != nil {
					fmt.Printf("Produce message failed: %v\n", err)
				}
			}
			// 消息发送完毕,再发送一个结束标识
			eofMsg := &kafkaGo.Message{
				TopicPartition: kafkaGo.TopicPartition{
					Topic:     &topicName,
					Partition: int32(i),
				},
				Value: []byte("__EOF__"),
			}
			if err := mProducer.Produce(eofMsg); err != nil {
				fmt.Printf("Produce ending message failed: %v\n", err)
			}
		}(i)
	}
}

func pushToElasticSearch(consumerID int, msg *kafkaGo.Message, esClient *es.ESClient, kafkaClient *mKakfa.Client) error {
	// 解析消息
	var record []map[string]interface{}
	if err := json.Unmarshal(msg.Value, &record); err != nil {
		fmt.Println("反序列化失败:", err)
		return err
	}
	errIds, insertErr := utils.Retry(esClient.MaxRetries, 2*time.Second, func() ([]string, error) {
		return esClient.BulkInsertDocuments(consumerID, context.Background(), IndexName, record)
	})
	if insertErr != nil {
		// 写入死信队列
		mProducer := kafkaClient.GetProducer()
		var topicName = DLQTopicName
		jsonData, err := json.Marshal(errIds)
		if err != nil {
			fmt.Println("序列化失败:", err)
		}
		sendMsg := &kafkaGo.Message{
			TopicPartition: kafkaGo.TopicPartition{
				Topic: &topicName,
			},
			Value: jsonData,
		}
		if err := mProducer.Produce(sendMsg); err != nil {
			fmt.Printf("Produce message failed: %v\n", err)
		}

	}
	return nil
}

// consumer
//
//	@Description: 从kafka消费数据,批量写入es
func consumer(client *mKakfa.Client, consumerID int, total *int64, esClient *es.ESClient, stopCtx context.Context) error {
	consumer, err := client.CreateConsumer(mKakfa.ConsumerConfig{
		GroupID:         "number_one",
		Topics:          []string{TopicName},
		AutoOffsetReset: "earliest",
		CommitBatchSize: 11,
	}, consumerID, stopCtx)
	if err != nil {
		return err
	}

	// 开始消费
	fmt.Printf("consumer-%d 启动成功\n", consumerID)
	if err := consumer.StartConsuming(consumerID, pushToElasticSearch, total, esClient, client); err != nil {
		return err
	}
	return nil

}

func createEsIndex(esClient *es.ESClient) error {
	projectPath := utils.GetConfigPath()
	mappingPath := filepath.Join(projectPath, "article.json")
	// 读取映射文件
	mapping, err := utils.ReadJSONFile(mappingPath)
	if err != nil {
		return err
	}
	if err := esClient.CreateIndexWithMapping(context.Background(), "article", mapping); err != nil {
		return err
	}
	return nil
}

func initTopics(kafkaClient *mKakfa.Client) error {
	topicConfig := mKakfa.TopicConfig{
		Name:              TopicName,
		NumPartitions:     6,
		ReplicationFactor: 1,
		ConfigMap: map[string]string{
			// 1. 每个分区最大保留 1GB 数据
			"retention.bytes": "1073741824", // 1GB = 1024*1024*1024

			// 2. 每个 segment 文件的最大大小 (100MB)
			"segment.bytes": "104857600", // 100MB

			// 3. 删除策略: delete (删除旧数据) 或 compact (压缩)
			"cleanup.policy": "delete",

			// ============ 基于时间的保留策略 ============

			// 4. 消息保留时间 (7天)
			"retention.ms": "604800000", // 7天 = 7*24*60*60*1000

			// 5. segment 文件关闭前的最长时间 (1天)
			"segment.ms": "86400000", // 1天

			// ============ 其他重要配置 ============

			// 6. 最小同步副本数
			"min.insync.replicas": "1",

			// 7. 压缩类型
			"compression.type": "snappy",
		},
	}
	DlqTopicConfig := mKakfa.TopicConfig{
		Name:              DLQTopicName,
		NumPartitions:     3,
		ReplicationFactor: 1,
		ConfigMap: map[string]string{
			// 1. 每个分区最大保留 1GB 数据
			"retention.bytes": "1073741824", // 1GB = 1024*1024*1024

			// 2. 每个 segment 文件的最大大小 (100MB)
			"segment.bytes": "104857600", // 100MB

			// 3. 删除策略: delete (删除旧数据) 或 compact (压缩)
			"cleanup.policy": "delete",

			// ============ 基于时间的保留策略 ============

			// 4. 消息保留时间 (7天)
			"retention.ms": "604800000", // 7天 = 7*24*60*60*1000

			// 5. segment 文件关闭前的最长时间 (1天)
			"segment.ms": "86400000", // 1天

			// ============ 其他重要配置 ============

			// 6. 最小同步副本数
			"min.insync.replicas": "1",

			// 7. 压缩类型
			"compression.type": "snappy",
		},
	}

	//删除topic
	if err := kafkaClient.DeleteTopic(TopicName); err != nil {
		panic(err)
	}
	if err := kafkaClient.DeleteTopic(DLQTopicName); err != nil {
		panic(err)
	}

	if err := kafkaClient.CreateTopic(topicConfig); err != nil {
		panic(err)
	}
	if err := kafkaClient.CreateTopic(DlqTopicConfig); err != nil {
		panic(err)
	}
	if err := kafkaClient.WaitTopicReady(TopicName, 300*time.Second); err != nil {
		panic(err)
	}
	return nil
}

func main() {
	waitGroup := sync.WaitGroup{}

	cancelCtx, cancel := context.WithCancel(context.Background())
	// 初始化pgsql
	if err := postgresql.InitDB(); err != nil {
		panic(err)
	}

	// 创建ES
	esClient := es.NewESClient()

	// 初始化索引
	//if err := esClient.DeleteIndex(context.Background(), IndexName); err != nil {
	//	fmt.Println("删除索引失败!")
	//}
	//if err := createEsIndex(esClient); err != nil {
	//	panic(err)
	//}
	// 创建kafka
	kafkaClient, err := mKakfa.NewClient()
	if err != nil {
		panic(err)
	}

	//if err := initTopics(kafkaClient); err != nil {
	//	panic(err)
	//}

	go producer(kafkaClient, &waitGroup, cancelCtx)

	var total int64 = 0
	for i := 0; i < 6; i++ {
		waitGroup.Add(1)
		go func(consumerID int) {
			defer waitGroup.Done()
			if err := consumer(kafkaClient, consumerID, &total, esClient, cancelCtx); err != nil {
				fmt.Printf("Consumer %d 错误: %v\n", consumerID, err)
			}
		}(i + 1)
	}

	go func() {
		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
		<-signalChan
		cancel()
		fmt.Println("收到退出信号，正在关闭...")
		fmt.Printf("一共消费%d组数据\n", total)
	}()

	waitGroup.Wait()
	kafkaClient.Close()
	fmt.Printf("一共消费%d\n", total)
	fmt.Println("所有 goroutine 已运行结束，程序结束")
}

/*
1.收到退出信号
2.关闭数据库读取生产者
3.生产者将所有数据写入kafka,然后结束
4.消费者消费完成所有kafka里的数据,并且全部写入es,结束
*/
